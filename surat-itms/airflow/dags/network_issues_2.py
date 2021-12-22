import pyspark.sql.functions as f
import time
from datetime import datetime,timedelta
import pytz
import h3
from pyspark.sql import Window
from pyspark.sql.types import StringType
from pyspark.sql import SparkSession
print("=="*50)

spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
bus_route_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"surat_bus_route").load()
bus_route_df.createOrReplaceTempView("bus_route_table")
bus_route_query = "SELECT * FROM bus_route_table"
bus_routes_df = spark.sql(bus_route_query)

bus_routes_df = bus_routes_df.select("route_id",f.col("latitude").alias("bus_route_longitude"),f.col("longitude").alias("bus_route_latitude"))

IST = pytz.timezone('Asia/Kolkata')
now_time = datetime.now(IST)
now = now_time - timedelta(weeks = 1)
start_time = now.strftime("'%Y-%m-%d 05:30:00'")
end_time = now_time.strftime("'%Y-%m-%d %H:%M:%S'")

spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
completed_trips_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"trip_status").load()
completed_trips_df.createOrReplaceTempView("completed_trips_table")
completed_trips_query = "SELECT * FROM completed_trips_table where observe_time >= {} AND observe_time <= {} AND status=='COMPLETED'".format(start_time,end_time)

completed_trips = spark.sql(completed_trips_query)
completed_trips = completed_trips.select("trip_id")

IST = pytz.timezone('Asia/Kolkata')
now_time = datetime.now(IST)
now = now_time - timedelta(weeks = 1)
start_time = now.strftime("'%Y-%m-%d 05:30:00'")
end_time = now_time.strftime("'%Y-%m-%d %H:%M:%S'")

spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
kudu_eta_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"surat_itms_live").load()
kudu_eta_df.createOrReplaceTempView("eta_table")
eta_query = "SELECT * FROM eta_table WHERE observationDateTime BETWEEN {} AND {} ORDER BY observationDateTime".format(start_time,end_time)
eta = spark.sql(eta_query)

eta = eta.select('primary_key', 'trip_id', 'id', 'route_id', 'trip_direction', 'vehicle_label', 'license_plate', 'last_stop_id', 'speed', 'observationDateTime', 'trip_delay', 'location_type', f.col('latitude').alias("eta_longitude"), f.col('longitude').alias("eta_latitude"))

eta = eta.dropna()

eta=eta.withColumn("observationDateTime", f.unix_timestamp(f.col('observationDateTime')))
completed_trips = completed_trips.withColumn("trip_id", completed_trips["trip_id"].cast('int'))
eta = eta.withColumn("trip_id", eta["trip_id"].cast('int'))
eta = eta.withColumn("last_stop_id", eta["last_stop_id"].cast('int'))
eta = eta.withColumn("trip_delay", eta["trip_delay"].cast('float'))


eta_completed_df = completed_trips.join(eta,"trip_id","inner")

eta_completed_df = eta_completed_df.select("trip_id","observationDateTime", "last_stop_id", "route_id", "eta_latitude","eta_longitude","license_plate")

eta_trip_route = eta_completed_df.sort("observationDateTime").select("trip_id","observationDateTime","route_id","last_stop_id", "eta_latitude","eta_longitude")



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


eta_trip_route_bins = eta_trip_route.withColumn("h3", geoToH3UDF(f.col("eta_latitude"),f.col("eta_longitude")))
eta_trip_route_bins = eta_trip_route_bins.dropna()

bus_routes_df_bins = bus_routes_df.withColumn("h3", geoToH3UDF(f.col("bus_route_latitude"),f.col("bus_route_longitude")))
bus_routes_df_bins = bus_routes_df_bins.dropna()

joined_coordinates = bus_routes_df_bins.join(eta_trip_route_bins,"h3","left_anti")
joined_coordinates = joined_coordinates.withColumn("polygon", h3ToGeoUDF(f.col("h3")))
joined_coordinates = joined_coordinates.withColumn("coordinates", h3ToCoordinatesUDF(f.col("h3")))


w = Window.partitionBy('h3')
joined_coordinates = joined_coordinates.withColumn('weight', f.count("route_id").over(w))


maximum = joined_coordinates.agg({"weight":"max"}).collect()[0][0]
minimum = joined_coordinates.agg({"weight":"min"}).collect()[0][0]


joined_coordinates = joined_coordinates.withColumn("normalize",f.floor(((5-1)*(f.col("weight") - minimum) / (maximum - minimum))+1).cast("int"))


w = Window().orderBy(f.lit('A'))
joined_coordinates = joined_coordinates.withColumn("primary_key",f.concat(f.col("h3"),f.expr("uuid()"),f.monotonically_increasing_id(),f.current_date().cast("String"),f.row_number().over(w).cast("String")))
joined_coordinates = joined_coordinates.select("h3","route_id", f.col("bus_route_latitude").alias("latitude"),f.col("bus_route_longitude").alias("longitude"),f.col("normalize").alias("weight"),"primary_key","polygon","coordinates")


joined_coordinates = joined_coordinates.withColumn('polygon', f.regexp_replace(f.col('polygon'), "\\(", "\\["))\
                  .withColumn('polygon', f.regexp_replace(f.col('polygon'), "\\)", "\\]"))

joined_coordinates = joined_coordinates.withColumn('coordinates', f.regexp_replace(f.col('coordinates'), "\\(", "\\["))\
                  .withColumn('coordinates', f.regexp_replace(f.col('coordinates'), "\\)", "\\]"))


joined_coordinates.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "network_issue_regions")\
.save()


print("=="*50)
print("Done")
