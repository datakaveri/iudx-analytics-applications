import requests
import pytz
from datetime import datetime
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

IST = pytz.timezone('Asia/Kolkata')
time = datetime.now(IST)
date = time.strftime("%Y-%m-%d")

r =requests.get('https://smcitms.in/avls/index.php/Smcapi/getschedule/format/json?tokenid=20200908')
data = r.json()

cols=["arrival_time", "trip_id", "stop_sequence", "stop_id", "departure_time"]
records=[]
for rec in data:
    records.append((date+" "+rec['trip_details']['arrival_time'],rec['trip_id'],rec['trip_details']['stop_sequence'],rec['trip_details']['stop_id'], date+" "+rec['trip_details']['departure_time']))

spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
df = spark.createDataFrame(records, cols)

df = df.withColumn("primary_key",f.concat(f.expr("uuid()").cast("String"),f.col("arrival_time")))\
        .withColumn("arrival_time", (f.unix_timestamp(f.col('arrival_time').cast("timestamp"))*1000000).cast("long"))\
        .withColumn("departure_time", (f.unix_timestamp(f.col('departure_time').cast("timestamp"))*1000000).cast("long"))\
        .withColumn("trip_id", f.col("trip_id").cast("int"))\
        .withColumn("stop_sequence", f.col("stop_sequence").cast("int"))\
        .withColumn("stop_id", f.col("stop_id").cast("int"))

df.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "surat_itms_schedule")\
.save()
