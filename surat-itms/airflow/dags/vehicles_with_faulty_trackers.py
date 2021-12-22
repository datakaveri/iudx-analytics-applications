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
now = datetime.now(IST)
start_time = now.strftime("'%Y-%m-%d 00:00:00'")

spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
completed_trips_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"trip_status").load()
completed_trips_df.createOrReplaceTempView("completed_trips_table")
completed_trips_query = "SELECT * FROM completed_trips_table where last_stop_arrival_time >= {} AND status=='COMPLETED'".format(start_time)

completed_trips = spark.sql(completed_trips_query)
completed_trips = completed_trips.dropna()
completed_trips = completed_trips.select("trip_id")

IST = pytz.timezone('Asia/Kolkata')
now_time = datetime.now(IST)
now = now_time
start_time = now.strftime("'%Y-%m-%d 00:00:00'")
end_time = now_time.strftime("'%Y-%m-%d %H:%M:%S'")

spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
kudu_eta_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"surat_itms_live").load()
kudu_eta_df.createOrReplaceTempView("eta_table")
eta_query = "SELECT * FROM eta_table WHERE observationDateTime BETWEEN {} AND {} ORDER BY observationDateTime".format(start_time,end_time)
eta = spark.sql(eta_query)

eta = eta.dropna()
eta = eta.select('primary_key', 'trip_id', 'id', 'route_id', 'trip_direction', "vehicle_label", 'actual_trip_start_time', 'last_stop_arrival_time', 'vehicle_label', 'license_plate', 'last_stop_id', 'speed', 'observationDateTime', 'trip_delay', 'location_type', f.col('latitude').alias("eta_longitude"), f.col('longitude').alias("eta_latitude"))

eta=eta.withColumn("actual_trip_start_time", f.unix_timestamp(f.col('actual_trip_start_time')))
eta=eta.withColumn("last_stop_arrival_time", f.unix_timestamp(f.col('last_stop_arrival_time')))
eta=eta.withColumn("observationDateTime", f.unix_timestamp(f.col('observationDateTime')))
eta = eta.withColumn("trip_id", eta["trip_id"].cast('int'))
eta = eta.withColumn("last_stop_id", eta["last_stop_id"].cast('int'))
eta = eta.withColumn("trip_delay", eta["trip_delay"].cast('float'))

eta_completed_df = completed_trips.join(eta,"trip_id","inner")

eta_trip_route = eta_completed_df.select("primary_key","trip_id","observationDateTime", "vehicle_label", "route_id","license_plate")
eta_trip_route = eta_trip_route.sort("observationDateTime").select("primary_key","observationDateTime","trip_id", "route_id","license_plate","vehicle_label")


w = Window.partitionBy("trip_id","vehicle_label").orderBy("observationDateTime")
eta_trip_route = eta_trip_route.withColumn("time_to_message", (f.col("observationDateTime")-f.lag(f.col("observationDateTime"),1).over(w)))

def faultDetection(time_to_message):
    if time_to_message is None:
        return 1
    if time_to_message>110:
        return 1
    else:
        return 0
faultDetectionUDF = f.udf(lambda time_to_message: faultDetection(time_to_message),StringType())


eta_trip_route = eta_trip_route.withColumn("fault", faultDetectionUDF(f.col("time_to_message")))

eta_trip_route = eta_trip_route.groupBy('trip_id','license_plate').agg(
    f.count(f.when(f.col("fault") == 0, True)).alias("not_faulty_count"),
    f.count(f.when(f.col("fault") == 1, True)).alias("faulty_count"))


eta_trip_route = eta_trip_route.withColumn("is_faulty",f.when(f.col("not_faulty_count") < f.col("faulty_count"), True).otherwise(False))

eta_trip_route = eta_trip_route.withColumn("observed_time",f.current_timestamp())\
                     .withColumn("observed_time", f.col('observed_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
                     .withColumn("observed_time",f.col("observed_time").cast("long"))\
                     .withColumn("observed_time",f.col("observed_time")*1000000)


eta_trip_route = eta_trip_route.withColumn("not_faulty_count", f.col("not_faulty_count").cast('int'))
eta_trip_route = eta_trip_route.withColumn("faulty_count", f.col("faulty_count").cast('int'))


eta_trip_route = eta_trip_route.withColumn("primary_key",f.concat(f.col("trip_id").cast("String"),f.current_date().cast("String")))

eta_trip_route.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "vehicles_with_faulty_trackers")\
.save()


print("=="*50)
print("Plot Done")
