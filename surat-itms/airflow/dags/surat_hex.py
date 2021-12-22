import pyspark.sql.functions as f
from datetime import datetime,timedelta
from pyspark.sql.types import StringType
import geopandas
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
import h3
import pytz
import h3pandas
from minio import Minio


IST = pytz.timezone('Asia/Kolkata')


live_now = datetime.now(IST)
live_start_time = live_now - timedelta(days = 30)
live_start_time = live_start_time.strftime("'%Y-%m-%d %H:%M:%S'")
live_end_time = live_now.strftime("'%Y-%m-%d %H:%M:%S'")
print(live_start_time)
print(live_end_time)


spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
kudu_eta_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"surat_itms_live").load()
kudu_eta_df.createOrReplaceTempView("eta_table")
eta_query = "SELECT * FROM eta_table WHERE observationDateTime BETWEEN {} AND {} ORDER BY observationDateTime".format(live_start_time,live_end_time)
eta = spark.sql(eta_query)
eta = eta.dropna()
eta = eta.select('primary_key', 'trip_id','route_id','actual_trip_start_time', 'last_stop_arrival_time', 'vehicle_label', 'license_plate', 'observationDateTime', f.col('latitude').alias("eta_longitude"), f.col('longitude').alias("eta_latitude"))


def geoToH3(latitude,longitude):
    resolution = 8
    return h3.geo_to_h3(latitude, longitude, resolution)

def h3ToCoordinates(h3_hash):
    return  '{"type": "Polygon", "geometry": {"type": "Polygon", "coordinates": ['+ str(h3.h3_set_to_multi_polygon([h3_hash], geo_json=True)[0][0]) +'] }}'


geoToH3UDF = f.udf(lambda latitude,longitude: geoToH3(latitude,longitude),StringType())
h3ToCoordinatesUDF = f.udf(lambda h3_hash: h3ToCoordinates(h3_hash),StringType())


eta_trip_route_bins = eta.withColumn("h3", geoToH3UDF(f.col("eta_latitude"),f.col("eta_longitude")))


eta_trip_route_bins = eta_trip_route_bins.groupBy("h3").count()


hex_live = eta_trip_route_bins.select("h3").distinct().collect()


list_live = []
for i in hex_live:
    list_live.append(i.h3)


hex_live = set(list_live)


client = Minio(
        endpoint="minio1:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )


data = geopandas.read_file(client.get_object(
        "geojson", "map(1).geojson"
    ))


gdf_h3 = data.h3.polyfill(8,explode=True)


hex_surat = set(gdf_h3.h3_polyfill.values)


missing_hex = hex_surat-hex_live


missing_hex_list = []
for i in missing_hex:
    missing_hex_list.append((i,0))


newRow = spark.createDataFrame(missing_hex_list,["h3","count"])


def unionAll(*dfs):
    return reduce(DataFrame.unionAll, dfs)

df = unionAll(*[eta_trip_route_bins, newRow])


df = df.withColumn("coordinates", h3ToCoordinatesUDF(f.col("h3")))


df = df.withColumn("count",f.col("count").cast("int"))


df = df.withColumn("observationDateTime",f.current_timestamp())\
                     .withColumn("observationDateTime", f.col('observationDateTime') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
                     .withColumn("observationDateTime",f.col("observationDateTime").cast("long"))\
                     .withColumn("observationDateTime",f.col("observationDateTime")*1000000)


df = df.withColumn('coordinates', f.regexp_replace(f.col('coordinates'), "\\(", "\\["))\
                  .withColumn('coordinates', f.regexp_replace(f.col('coordinates'), "\\)", "\\]"))

df.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "surat_hex")\
.save()

print("=="*50)
print("Done")
