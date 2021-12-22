
import pyspark.sql.functions as f
from datetime import datetime,timedelta
from pyspark.sql.types import StringType
import geopandas
from functools import reduce
from pyspark.sql import DataFrame
import json
import h3
import pytz
import h3pandas
import requests
from scipy.interpolate import griddata
from pyspark.sql import Window
import math
import numpy as np
from pyspark.sql import SparkSession


spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
kudu_eta_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"pune_aqm_live").load()
kudu_eta_df=kudu_eta_df.dropna()


w = Window.partitionBy("id").orderBy(f.desc("observationDateTime"))
df = kudu_eta_df.withColumn("row", f.row_number().over(w)).where(f.col("row") == 1)


spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
sensors_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"pune_sensor_coordinates").load()


final_df=df.join(sensors_df,"id","inner")
final_pd= final_df.toPandas()


def geoToH3(latitude,longitude):
    resolution = 9
    return h3.geo_to_h3(latitude, longitude, resolution)

def h3ToCoordinates(h3_hash):
    # return '{"type": "FeatureCollection","features": [{"type": "Feature","properties": {},"geometry": {"type": "Polygon","coordinates": ['+str(h3.h3_set_to_multi_polygon([h3_hash], geo_json=True)[0][0])+']}}]}'
    return  '{"type": "Polygon", "geometry": {"type": "Polygon", "coordinates": ['+ str(h3.h3_set_to_multi_polygon([h3_hash], geo_json=True)[0][0]) +'] }}'

def h3ToGeo(h3_hash):
    return str(h3.h3_set_to_multi_polygon([h3_hash], geo_json=True)[0][0])



result=[]
pollutants={
    'co2':200,
    'co':0,
    'no2':0,
    'so2':0,
    'pm10':0,
    'pm2p5':0
}


for pollutant in pollutants.keys():
    z=final_pd[pollutant]
    ids=final_pd['id']
    x=final_pd['longitude']
    y=final_pd['latitude']
    res=100

    y_arr = np.linspace(np.min(y), np.max(y), res)
    x_arr = np.linspace(np.min(x), np.max(x), res)
    # Make mesh grid
    x_mesh, y_mesh = np.meshgrid(x_arr, y_arr)
    # Perform cubic interpolation
    z_mesh = griddata((x, y), z, (x_mesh, y_mesh), method='cubic')

    for i in range(0,res):
        for j in range(0,res):
            if math.isnan(z_mesh[i][j])==False:
                result.append([pollutant, float(z_mesh[i][j]) if z_mesh[i][j]>pollutants[pollutant] else float(pollutants[pollutant]), h3ToCoordinates(geoToH3(y_mesh[i][j],x_mesh[i][j]))])


cols=['pollutant','weight','coodinates']
df = spark.createDataFrame(result, cols)


w = Window().orderBy(f.lit('A'))
df = df.withColumn("primary_key",f.concat(f.expr("uuid()"),f.monotonically_increasing_id(),f.current_date().cast("String"),f.row_number().over(w).cast("String")))
df = df.withColumn("observationDateTime",f.current_timestamp())\
                     .withColumn("observationDateTime", f.col('observationDateTime') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
                     .withColumn("observationDateTime",f.col("observationDateTime").cast("long"))\
                     .withColumn("observationDateTime",f.col("observationDateTime")*1000000)
df = df.withColumn('coodinates', f.regexp_replace(f.col('coodinates'), "\\(", "\\["))\
                  .withColumn('coodinates', f.regexp_replace(f.col('coodinates'), "\\)", "\\]"))

df.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "pune_pollutants_contours")\
.save()
