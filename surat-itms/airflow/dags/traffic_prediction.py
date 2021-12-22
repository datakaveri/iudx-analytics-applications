import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import geopandas
import h3
import h3pandas
import pickle
import pytz
from minio import Minio
import os
import tensorflow as tf
from tensorflow.keras.layers import Input,Dense,Reshape,LSTM, Dropout
from tensorflow.keras.models import Model
from spektral.layers import GCNConv
from spektral.utils import gcn_filter
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import Window

client = Minio(
        endpoint="minio1:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )



def get_all_hexes():
    surat = geopandas.read_file(client.get_object(
        "itms-prediction", "map_traffic.geojson"
    ))
    gdf_h3 = surat.h3.polyfill(8,explode=True)
    gdf_h3=gdf_h3.drop_duplicates(subset=['h3_polyfill'])
    all_surat_hexes=list(gdf_h3['h3_polyfill'])
    all_surat_hexes.sort()
    return all_surat_hexes

def get_adjacency_matrix(hexes):
    adj=[]
    for hex1 in hexes:
        row=[]
        rings=h3.hex_range_distances(h=hex1,K=1)
        for hex2 in hexes:
            if hex2 in rings[1] or hex2 in rings[0]:
                row.append(1)
            else:
                row.append(0)
        adj.append(row)
    adj_mat=np.array(adj)
    adj_lap=gcn_filter(adj_mat, symmetric=True)
    return adj_lap

def get_X(time_now:datetime, hex_list:list):
    #spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
    dtime_tab=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"traffic_delay").load()
    time_history=time_now-timedelta(hours=4)
    end=time_now.strftime("'%Y-%m-%d %H:00:00'")
    start=time_history.strftime("'%Y-%m-%d %H:00:00'")
    dtime_tab.createOrReplaceTempView("dtime_table")
    dtime_query = "SELECT * FROM dtime_table WHERE observationDateTime >= {} AND observationDateTime < {} ORDER BY observationDateTime".format(start,end)
    dtime_now = spark.sql(dtime_query)
    dtime_now = dtime_now.withColumn("temp_dwell_time",f.when(f.col("dwell_time")>500,-1).otherwise(f.col("dwell_time")))
    dtime_now=dtime_now.toPandas()
    data=[]
    hexes=[]
    end_time=time_now
    for hex_id in hex_list:
        for direction in ['DN','UP']:
            group=dtime_now[(dtime_now['h3']==hex_id) & (dtime_now['trip_direction']==direction)]
            start_time=time_history
            row=[]
            while start_time!=end_time:
                slot_end=start_time+timedelta(hours=1)
                dtimes=group[(group['observationDateTime']>=start_time.strftime("'%Y-%m-%d %H:00:00'")) & (group['observationDateTime']<slot_end.strftime("'%Y-%m-%d %H:00:00'"))]
                if len(dtimes)!=0:
                    avg_dtime=dtimes.dwell_time.mean()
                else:
                    avg_dtime=-1
                row.append(avg_dtime)
                start_time+=timedelta(hours=1)
            data.append(row)
            hexes.append(hex_id)
    data=np.array(data)
    adj_lap=get_adjacency_matrix(hexes)
    return data,adj_lap

def scale_data(data):
    max_time = 500
    min_time = -1
    return (data - min_time) / (max_time - min_time)

def unscale_data(data_scaled):
    max_time=500
    min_time=-1
    data_unscaled=(data_scaled*(max_time-min_time))+min_time
    return data_unscaled

def clean_prediction(prediction):
    for i in range(prediction.shape[0]):
        for j in range(prediction.shape[1]):
            if prediction[i,j]<0:
                prediction[i,j]=-1
    return prediction

def load_tgcn(adj_lap,seq_length,load_weights=True):
    N=adj_lap.shape[0]
    inp_feat = Input((N, seq_length))

    x = GCNConv(32, activation='relu')([inp_feat, adj_lap])
    x = GCNConv(16, activation='relu')([x, adj_lap])
    x = Reshape((16,N))(x)
    x = LSTM(128, activation='relu', return_sequences=True)(x)
    x = LSTM(32, activation='relu')(x)

    x = Dense(128, activation='relu')(x)
    x = Dropout(0.3)(x)
    out = Dense(N)(x)

    tgcn = Model(inp_feat, out)
    if load_weights==True:
        client.fget_object(
        "itms-prediction", "t-gcn-model_weights_28.h5",'model.h5'
    )
        tgcn.load_weights('model.h5')
        os.remove('model.h5')
    return tgcn

spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()

IST = pytz.timezone('Asia/Kolkata')
hex_list=get_all_hexes()
time_now=datetime.now(IST)-timedelta(hours=1)
data, adj_lap=get_X(time_now,hex_list)
scaled_data=scale_data(data)
seq_length=4


tgcn=load_tgcn(adj_lap,seq_length)
prediction=tgcn.predict(np.reshape(scaled_data,(1,adj_lap.shape[0],seq_length)))
prediction=clean_prediction(unscale_data(prediction))


result=[]
j=0
for hex_id in hex_list:
    for direction in ['DN','UP']:
        result.append((time_now,hex_id,direction,float(prediction[0][j])))
        j+=1

df=spark.createDataFrame(result,['observationDateTime','h3','trip_direction','predicted_dwell_time'])


w = Window().orderBy(f.lit('A'))
df = df.withColumn("primary_key",f.concat(f.expr("uuid()"),f.monotonically_increasing_id(),f.current_date().cast("String"),f.row_number().over(w).cast("String")))
df = df.withColumn("observationDateTime",f.current_timestamp())\
                     .withColumn("observationDateTime", f.col('observationDateTime') + f.expr('INTERVAL 4 HOURS 30 MINUTES'))\
                     .withColumn("observationDateTime",f.col("observationDateTime").cast("long"))\
                     .withColumn("observationDateTime",f.col("observationDateTime")*1000000)


df.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "traffic_prediction")\
.save()

print("=="*50)
print("Done")
