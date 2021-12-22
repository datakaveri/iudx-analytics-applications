from minio import Minio
import io
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
import pandas as pd
from sklearn.linear_model import Lasso

from pyspark.sql.functions import pandas_udf
from pyspark.sql.functions import PandasUDFType


IST = pytz.timezone('Asia/Kolkata')
now_time = datetime.now(IST)
now = now_time - timedelta(weeks = 1)
start_time = now.strftime("'%Y-%m-%d %H:%M:%S'")
end_time = now_time.strftime("'%Y-%m-%d %H:%M:%S'")


spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
kudu_eta_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"surat_itms_live").load()
kudu_eta_df.createOrReplaceTempView("eta_table")
eta_query = "SELECT * FROM eta_table WHERE observationDateTime BETWEEN {} AND {} ORDER BY observationDateTime".format(start_time,end_time)
eta = spark.sql(eta_query)

eta = eta.select('primary_key', 'trip_id', 'id', 'route_id', 'trip_direction', 'vehicle_label', 'license_plate', 'last_stop_id', 'speed', 'observationDateTime', 'trip_delay', 'location_type', f.col('latitude').alias("eta_longitude"), f.col('longitude').alias("eta_latitude"))

eta = eta.dropna()

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


eta_trip_route1 = eta_trip_route.groupBy("h3","route_id","time_slot").avg("dwell_time").sort("h3","route_id","time_slot")


eta_trip_route1 = eta_trip_route1.select("h3", "route_id", "time_slot", f.col("avg(dwell_time)").alias("dwell_time"))


schema = StructType([
    StructField("route_id", StringType()),
    StructField("h3", StringType()),
    StructField("model", BinaryType())
])
@pandas_udf(schema, functionType=PandasUDFType.GROUPED_MAP)
def lasso(df):
    hex_dict={}
    model=Lasso()
    model.fit(df["time_slot"].values.reshape(-1, 1),df["dwell_time"].values.reshape(-1, 1))
    df["model"] = pickle.dumps(model)
    df.drop(["time_slot","dwell_time"],axis=1, inplace=True)
    return df


eta_trip_route2 = eta_trip_route1.groupby("h3","route_id").apply(lasso).dropDuplicates(["h3","route_id"])


pandasDF = eta_trip_route2.toPandas()


client = Minio(
        endpoint="minio1:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )


bytes_file = pickle.dumps(pandasDF)


client.put_object(
            bucket_name="mergedmodel",
            object_name="delay_model.pkl",
            data=io.BytesIO(bytes_file),
            length=len(bytes_file)
        )

print("=="*50)
print("Done")
