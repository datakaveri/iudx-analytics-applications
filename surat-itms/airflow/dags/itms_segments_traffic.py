# -*- coding: utf-8 -*-
"""itms_segments_traffic.(2).ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1SGfoVbXgIHzlE4eLA0ntcbhSaxsU4BcQ
"""

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
from minio import Minio
import pyspark.sql.functions as f
import time
from datetime import datetime,timedelta
import pytz
import h3
from pyspark.sql import Window
from pyspark.sql.types import *
from pyspark.sql import SparkSession
import calendar
from shapely.geometry import Point, LineString, Polygon
import pickle
import geopandas
import math
import numpy as np

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
IST = pytz.timezone('Asia/Kolkata')
now_time = datetime.now(IST)
now = now_time - timedelta(minutes = 1)
start_time = now.strftime("'%Y-%m-%d %H:%M:%S'")
end_time = now_time.strftime("'%Y-%m-%d %H:%M:%S'")

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
kudu_eta_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"surat_itms_live").load()
kudu_eta_df.createOrReplaceTempView("eta_table")
eta_query = "SELECT * FROM eta_table WHERE observationDateTime BETWEEN {} AND {} ORDER BY observationDateTime".format(start_time,end_time)
eta = spark.sql(eta_query)

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
eta = eta.select('primary_key', 'trip_id', 'id', 'route_id', 'trip_direction', 'actual_trip_start_time', 'last_stop_arrival_time', 'vehicle_label', 'license_plate', 'last_stop_id', 'speed', 'observationDateTime', 'trip_delay', 'location_type', f.col('latitude').alias("eta_longitude"), f.col('longitude').alias("eta_latitude"))

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
eta=eta.withColumn("actual_trip_start_time", f.unix_timestamp(f.col('actual_trip_start_time')))
eta=eta.withColumn("last_stop_arrival_time", f.unix_timestamp(f.col('last_stop_arrival_time')))
eta=eta.withColumn("observationDateTime", f.unix_timestamp(f.col('observationDateTime')))
eta = eta.withColumn("trip_id", eta["trip_id"].cast('int'))
eta = eta.withColumn("last_stop_id", eta["last_stop_id"].cast('int'))
eta = eta.withColumn("trip_delay", eta["trip_delay"].cast('float'))

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
eta_trip_route = eta.select("primary_key","trip_id","observationDateTime", "route_id","trip_direction", "eta_latitude","eta_longitude","license_plate")
eta_trip_route = eta_trip_route.sort("observationDateTime").select("primary_key","observationDateTime","trip_id", "route_id","trip_direction", "eta_latitude","eta_longitude","license_plate")

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
client = Minio(
        endpoint="minio1:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
h3_to_seg = pickle.loads(client.get_object(
        "itms-prediction", "mmr-h3-segments.pkl"
    ).read())
segment_coords_dict = pickle.loads(client.get_object(
        "itms-prediction", "mmr-segments-coords-len.pkl"
    ).read())
h3_resolution=11

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
def geo_to_segment(latitude,longitude):
    hid=h3.geo_to_h3(latitude,longitude,h3_resolution)
    notfound=True
    k=0
    hex_seg=hid
    while(notfound):
        ring=h3.k_ring(hid,k)
        for nh in ring:
            if nh in list(h3_to_seg.keys()):
                hex_seg=nh
                notfound=False
                break
        k+=1
        if k>5:
            return -1
    segment_ids=h3_to_seg[hex_seg]
    if len(segment_ids)==1:
        return segment_ids[0]
    distances=[]
    for i in range(len(segment_ids)):
        sid=segment_ids[i]
        distances.append(min([(((c[0]-longitude)**2)+((c[1]-latitude)**2))**0.5 for c in segment_coords_dict[sid]['coordinates']]))
    return segment_ids[distances.index(min(distances))]

geoToSegmentUDF = f.udf(lambda latitude,longitude: geo_to_segment(latitude,longitude),IntegerType())

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
eta_trip_route = eta_trip_route.withColumn("segment", geoToSegmentUDF(f.col("eta_latitude"),f.col("eta_longitude"))).select("primary_key","observationDateTime","trip_id","route_id","trip_direction","segment","license_plate","eta_latitude","eta_longitude")
# eta_trip_route.show()

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark

import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType

schema = StructType([
    StructField("observationDateTime", LongType()),
    StructField("trip_id", IntegerType()),
    StructField("route_id", StringType()),
    StructField("trip_direction", StringType()),
    StructField("license_plate", StringType()),
    StructField("segment", IntegerType()),
    StructField("dwell_time", IntegerType()),
    StructField("sb_dwell_time", IntegerType()),
])

def Euclidean(p1, p2):
    return (((p1[0]-p2[0])**2)+((p1[1]-p2[1])**2))**0.5

@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def get_dwell_time(df):
    new_df_rows=[]
    sid=df['segment'].values[0]
    df = df.sort_values("observationDateTime").reset_index(drop=True)
    segment_start = df.iloc[[0]]
    segment_end = df.iloc[[-1]].reset_index(drop=True)
    segment_dist = Euclidean([segment_start["eta_latitude"][0], segment_start["eta_longitude"][0]], [segment_end["eta_latitude"][0], segment_end["eta_longitude"][0]])
    segment_start_time = segment_start["observationDateTime"].values[0]
    segment_end_time = segment_end["observationDateTime"].values[0]
    travel_time = (segment_end_time - segment_start_time)
        
    travel_speed=segment_dist/travel_time
        
    dwell_time=segment_coords_dict[sid]['length']/travel_speed if travel_speed>0 else 0
    
    new_df_rows.append([segment_end['observationDateTime'].values[0],df['trip_id'].values[0],df['route_id'].values[0],df['trip_direction'].values[0],df['license_plate'].values[0],df['segment'].values[0],travel_time,dwell_time if dwell_time<86400 else 86400])
    new_df=pd.DataFrame(new_df_rows, columns=['observationDateTime','trip_id','route_id','trip_direction','license_plate','segment','dwell_time','sb_dwell_time'])
    return new_df

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
eta_trip_route = eta_trip_route.where(f.col("segment")!=-1).groupby(["trip_id","route_id","segment"]).apply(get_dwell_time)
# eta_trip_route = eta_trip_route.where(f.col("dwell_time")>0)

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
def hourOfWeek(observetime):
    observationtime = datetime.fromtimestamp(observetime)
    return calendar.weekday(observationtime.year,observationtime.month,observationtime.day)*24+observationtime.hour
hourOfWeekUDF = f.udf(lambda observetime: hourOfWeek(observetime),IntegerType())

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
eta_trip_route = eta_trip_route.withColumn("time_slot", hourOfWeekUDF(f.col("observationDateTime")))

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
def trafficcategory(dtime):
    if dtime<60:
        return 1
    elif dtime<180:
        return 2
    elif dtime<300:
        return 3
    elif dtime<600:
        return 4
    else:
        return 5
trafficCategoryUDF = f.udf(lambda dtime: trafficcategory(dtime),IntegerType())

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
eta_trip_route = eta_trip_route.withColumn("category", trafficCategoryUDF(f.col("sb_dwell_time")))

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
def seg_to_geojson(seg_id):
    return  '{"type": "Polygon", "geometry": {"type": "Polygon", "coordinates": ['+ str(segment_coords_dict[seg_id]['coordinates']) +'] }}'
seg_to_geojsonUDF = f.udf(lambda seg_id: seg_to_geojson(seg_id),StringType())

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
eta_trip_route = eta_trip_route.withColumn("geojson", seg_to_geojsonUDF(f.col("segment")))
eta_trip_route = eta_trip_route.withColumn('geojson', f.regexp_replace(f.col('geojson'), "\\(", "\\["))\
                  .withColumn('geojson', f.regexp_replace(f.col('geojson'), "\\)", "\\]"))

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
eta_trip_route = eta_trip_route.withColumn("observationDateTime",f.col("observationDateTime")*1000000)\
                    .withColumn("trip_id",f.col("trip_id").cast('String'))\
                    .withColumn("time_slot",f.col("time_slot").cast('String'))\
                    .withColumn("dwell_time",f.col("dwell_time").cast('double'))\
                    .withColumn("sb_dwell_time",f.col("sb_dwell_time").cast('double'))

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
w = Window().orderBy(f.lit('A'))
eta_trip_route = eta_trip_route.withColumn("primary_key",f.concat(f.expr("uuid()"),f.monotonically_increasing_id(),f.current_date().cast("String"),f.row_number().over(w).cast("String")))

# Commented out IPython magic to ensure Python compatibility.
# %spark.pyspark
eta_trip_route.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "traffic_segment")\
.save()
