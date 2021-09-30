import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql import Window

"""# Select Time Interval"""

now = datetime.today()
start = now
end = now+timedelta(days=1)
start_date = start.strftime("'%Y-%m-%d 00:00:00'")
end_date = end.strftime("'%Y-%m-%d 00:00:00'")

"""# Read Schedules Data"""

spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
kudu_sch_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"surat_itms_schedule").load()
kudu_sch_df.createOrReplaceTempView("sch_table")
sch_query = "SELECT * FROM sch_table WHERE arrival_time >= {} AND arrival_time < {} ORDER BY arrival_time".format(start_date,end_date)
sch = spark.sql(sch_query)

sch = sch.dropna()

sch = sch.withColumn('arrival_time', f.col('arrival_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
    .withColumn('departure_time', f.col('departure_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))

"""# Processing columns"""

sch=sch.withColumn("arrival_time", f.unix_timestamp(f.col('arrival_time')))
sch=sch.withColumn("departure_time", f.unix_timestamp(f.col('departure_time')))

sch = sch.withColumn("trip_id", sch["trip_id"].cast('int'))
sch = sch.withColumn("stop_id", sch["stop_id"].cast('int'))

"""## Scheduled stops"""

w = Window.partitionBy('trip_id')
sch_max_min = sch.withColumn('end_time', f.max('arrival_time').over(w))\
    .withColumn('start_time', f.min('departure_time').over(w))\
    .where((f.col('arrival_time') == f.col('end_time')) | (f.col('departure_time') == f.col('start_time')))

"""### b. Flatten Stop IDs to get Start Stop and End Stop"""

temp_df = sch_max_min.sort("trip_id","stop_sequence", ascending=True).groupBy("trip_id").agg(f.collect_list("stop_id").alias("stop_id"))

temp_df = temp_df.withColumn(
            "start_stop",
            f.col("stop_id").getItem(0))
temp_df = temp_df.withColumn(
    "end_stop",
    f.col("stop_id").getItem(1))

temp_df = temp_df.drop("stop_id").sort("trip_id")
temp_df = temp_df.join(sch_max_min, "trip_id", "inner")

stops_sch_df = temp_df.select("trip_id", "start_stop", "end_stop", "start_time", "end_time").dropDuplicates(["trip_id", "start_stop", "end_stop", "start_time", "end_time"]).sort("trip_id")

stops_sch_df = stops_sch_df.withColumn("start_time", f.col("start_time")*1000000)
stops_sch_df = stops_sch_df.withColumn("end_time", f.col("end_time")*1000000)
stops_sch_df = stops_sch_df.withColumn("primary_key",f.concat(f.col("trip_id").cast("String"),f.current_date().cast("String")))

stops_sch_df = stops_sch_df.withColumn("observed_time",f.current_timestamp())\
                     .withColumn("observed_time", f.col('observed_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
                     .withColumn("observed_time",f.col("observed_time").cast("long"))

stops_sch_df = stops_sch_df.withColumn("observed_time",f.col("observed_time")*1000000)
stops_sch_df = stops_sch_df.withColumn("trip_id", f.col("trip_id").cast('String'))
stops_sch_df = stops_sch_df.withColumn("start_stop", f.col("start_stop").cast('String'))
stops_sch_df = stops_sch_df.withColumn("end_stop", f.col("end_stop").cast('String'))

"""### c. Push data to Kudu"""

stops_sch_df.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "stop_schedule")\
.save()

print("=="*50)
print("Done")
