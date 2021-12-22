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
import pickle
import numpy as np


IST = pytz.timezone('Asia/Kolkata')
now_time = datetime.now(IST)
now = now_time - timedelta(minutes = 15)
start_time = now.strftime("'%Y-%m-%d %H:%M:%S'")
end_time = now_time.strftime("'%Y-%m-%d %H:%M:%S'")


spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
kudu_eta_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"surat_itms_live").load()
kudu_eta_df.createOrReplaceTempView("eta_table")
eta_query = "SELECT * FROM eta_table WHERE observationDateTime BETWEEN {} AND {} ORDER BY observationDateTime".format(start_time,end_time)
eta = spark.sql(eta_query)

eta = eta.select('primary_key', 'trip_id', 'id', 'route_id', 'trip_direction', 'actual_trip_start_time', 'last_stop_arrival_time', 'vehicle_label', 'license_plate', 'last_stop_id', 'speed', 'observationDateTime', 'trip_delay', 'location_type', f.col('latitude').alias("eta_longitude"), f.col('longitude').alias("eta_latitude"))


eta=eta.withColumn("actual_trip_start_time", f.unix_timestamp(f.col('actual_trip_start_time')))
eta=eta.withColumn("last_stop_arrival_time", f.unix_timestamp(f.col('last_stop_arrival_time')))
eta=eta.withColumn("observationDateTime", f.unix_timestamp(f.col('observationDateTime')))
eta = eta.withColumn("trip_id", eta["trip_id"].cast('int'))
eta = eta.withColumn("last_stop_id", eta["last_stop_id"].cast('int'))
eta = eta.withColumn("trip_delay", eta["trip_delay"].cast('float'))


eta_trip_route = eta.select("primary_key","trip_id","observationDateTime", "route_id","trip_direction", "eta_latitude","eta_longitude","license_plate")
eta_trip_route = eta_trip_route.sort("observationDateTime").select("primary_key","observationDateTime","trip_id", "route_id","trip_direction", "eta_latitude","eta_longitude","license_plate")



def geoToH3(latitude, longitude):
    resolution = 8
    return h3.geo_to_h3(latitude, longitude, resolution)
geoToH3UDF = f.udf(lambda latitude,longitude: geoToH3(latitude,longitude),StringType())


eta_trip_route = eta_trip_route.withColumn("h3", geoToH3UDF(f.col("eta_latitude"),f.col("eta_longitude"))).select("primary_key","observationDateTime","trip_id","route_id","trip_direction","h3","license_plate")


w = Window.partitionBy("trip_id","route_id","h3")
eta_trip_route = eta_trip_route.withColumn("dwell_time", f.max("observationDateTime").over(w)-f.min("observationDateTime").over(w))


w = Window.partitionBy("trip_id","route_id","h3").orderBy(f.desc("observationDateTime"))
eta_trip_route = eta_trip_route.withColumn("row", f.row_number().over(w)).where(f.col("row") != 1).drop("row")


def hourOfWeek(observetime):
    observationtime = datetime.fromtimestamp(observetime)
    return calendar.weekday(observationtime.year,observationtime.month,observationtime.day)*24+observationtime.hour
hourOfWeekUDF = f.udf(lambda observetime: hourOfWeek(observetime),IntegerType())


eta_trip_route = eta_trip_route.withColumn("time_slot", hourOfWeekUDF(f.col("observationDateTime")))


client = Minio(
        endpoint="minio1:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

models = pickle.loads(client.get_object(
        "mergedmodel", "delay_model.pkl"
    ).read())


def predict(h3_hash,route_id,time_slot):
    try:
        model = pickle.loads(models[(models["route_id"]==route_id) & (models["h3"]==h3_hash)]["model"].values[0])
        return model.predict(np.array(time_slot).reshape(1, 1)).tolist()[0]
    except:
        return None

predictUDF = f.udf(lambda h3_hash,route_id,time_slot: predict(h3_hash,route_id,time_slot),DoubleType())


def geoToH3(latitude,longitude):
    resolution = 8
    return h3.geo_to_h3(latitude, longitude, resolution)

def h3ToCoordinates(h3_hash):
    return  '{"type": "Polygon", "geometry": {"type": "Polygon", "coordinates": ['+ str(h3.h3_set_to_multi_polygon([h3_hash], geo_json=True)[0][0]) +'] }}'

def h3ToGeo(h3_hash):
    return str(h3.h3_set_to_multi_polygon([h3_hash], geo_json=True)[0][0])


geoToH3UDF = f.udf(lambda latitude,longitude: geoToH3(latitude,longitude),StringType())
h3ToGeoUDF = f.udf(lambda h3_hash: h3ToGeo(h3_hash),StringType())
h3ToCoordinatesUDF = f.udf(lambda h3_hash: h3ToCoordinates(h3_hash),StringType())


eta_trip_route = eta_trip_route.withColumn("geojson", h3ToCoordinatesUDF(f.col("h3")))


eta_trip_route = eta_trip_route.withColumn('geojson', f.regexp_replace(f.col('geojson'), "\\(", "\\["))\
                  .withColumn('geojson', f.regexp_replace(f.col('geojson'), "\\)", "\\]"))


eta_trip_route = eta_trip_route.withColumn("predicted_dwell_time", predictUDF(f.col("h3"),f.col("route_id"),f.col("time_slot")))


eta_trip_route = eta_trip_route.withColumn("delay",f.col("dwell_time")-f.col("predicted_dwell_time"))


eta_trip_route = eta_trip_route.withColumn("observationDateTime",f.col("observationDateTime")*1000000)\
                    .withColumn("trip_id",f.col("trip_id").cast('String'))\
                    .withColumn("time_slot",f.col("time_slot").cast('String'))\
                    .withColumn("dwell_time",f.col("dwell_time").cast('double'))

eta_trip_route.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "traffic_delay")\
.save()

print("=="*50)
print("Done")
