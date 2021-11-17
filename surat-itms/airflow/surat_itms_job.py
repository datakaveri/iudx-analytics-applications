# Import Libraries
import pyspark.sql.functions as f
import time
from datetime import datetime,timedelta
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pytz
import time
from pyspark.sql.types import StringType
"""# Set Time Zone"""


IST = pytz.timezone('Asia/Kolkata')

"""# Timestamps for ITMS Live Data"""

live_now = datetime.now(IST)
live_start_time = live_now.strftime("'%Y-%m-%d 05:30:00'")
live_end_time = live_now.strftime("'%Y-%m-%d %H:%M:%S'")
print(live_start_time)
print(live_end_time)

"""# Timestamps for ITMS Schedules Data"""


sch_now = datetime.today()
sch_start = sch_now
sch_end = sch_now+timedelta(days=1)
sch_start_date = sch_start.strftime("'%Y-%m-%d 00:00:00'")
sch_end_date = sch_end.strftime("'%Y-%m-%d 00:00:00'")
print(sch_start_date)
print(sch_end_date)

"""# Timestamp for Filter Data After Processing"""


trim_now = datetime.now(IST)-timedelta(hours=1)
trim_start_time = trim_now.strftime("'%Y-%m-%d %H:%M:%S'")
trim_epoch = int(time.mktime(time.strptime(trim_start_time, "'%Y-%m-%d %H:%M:%S'")))
print(trim_epoch)

"""# Get ITMS Live Data"""


spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
kudu_eta_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"surat_itms_live").load()
kudu_eta_df.createOrReplaceTempView("eta_table")
eta_query = "SELECT * FROM eta_table WHERE observationDateTime BETWEEN {} AND {} ORDER BY observationDateTime".format(live_start_time,live_end_time)
eta = spark.sql(eta_query)
eta = eta.dropna()

"""# Get ITMS Schedules Data"""


spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
kudu_sch_df=spark.read.format('org.apache.kudu.spark.kudu').option('kudu.master',"kudu-master:7051").option('kudu.table',"surat_itms_schedule").load()
kudu_sch_df.createOrReplaceTempView("sch_table")
sch_query = "SELECT * FROM sch_table WHERE arrival_time >= {} AND arrival_time < {} ORDER BY arrival_time".format(sch_start_date,sch_end_date)
sch = spark.sql(sch_query)

sch = sch.dropna()

"""# Add Timezone Offset to Schedules Data"""


sch = sch.withColumn('arrival_time', f.col('arrival_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
    .withColumn('departure_time', f.col('departure_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))

"""# Convert Timestamp columns to UNIX Timestamp (Epoch) format"""


eta=eta.withColumn("actual_trip_start_time", f.unix_timestamp(f.col('actual_trip_start_time')))
eta=eta.withColumn("last_stop_arrival_time", f.unix_timestamp(f.col('last_stop_arrival_time')))
eta=eta.withColumn("observationDateTime", f.unix_timestamp(f.col('observationDateTime')))

sch=sch.withColumn("arrival_time", f.unix_timestamp(f.col('arrival_time')))
sch=sch.withColumn("departure_time", f.unix_timestamp(f.col('departure_time')))

"""# Data Type Casting"""


eta = eta.withColumn("trip_id", eta["trip_id"].cast('int'))
eta = eta.withColumn("last_stop_id", eta["last_stop_id"].cast('int'))
eta = eta.withColumn("trip_delay", eta["trip_delay"].cast('float'))

sch = sch.withColumn("trip_id", sch["trip_id"].cast('int'))
sch = sch.withColumn("stop_id", sch["stop_id"].cast('int'))

"""# Not Yet Started Trips"""


all_trips = sch.join(eta, "trip_id", "left")
not_yet_started_trips = all_trips[all_trips['id'].isNull()].select("trip_id",f.col("arrival_time").alias("stop_arrival_time"),f.col("departure_time").alias("stop_departure_time")).distinct()


w = Window.partitionBy('trip_id')
not_yet_started_trips_max_min = not_yet_started_trips.withColumn('stop_end_time', f.max('stop_arrival_time').over(w))\
    .withColumn('stop_start_time', f.min('stop_departure_time').over(w))\
    .where((f.col('stop_arrival_time') == f.col('stop_end_time')) | (f.col('stop_departure_time') == f.col('stop_start_time'))).select("trip_id","stop_end_time","stop_start_time").dropDuplicates(["trip_id","stop_end_time","stop_start_time"]).sort("trip_id")

"""# Get Maximum Arrival Time and Minimum Departure Time for Schedules Data"""


w = Window.partitionBy('trip_id')
sch_max_min = sch.withColumn('end_time', f.max('arrival_time').over(w))\
    .withColumn('start_time', f.min('departure_time').over(w))\
    .where((f.col('arrival_time') == f.col('end_time')) | (f.col('departure_time') == f.col('start_time'))).select("trip_id","end_time","start_time").dropDuplicates(["trip_id","end_time","start_time"]).sort("trip_id")

"""# Get Maximum Last Stop Arrival Time and Minimum Actual Trip Start Departure Time for Live Data"""


w = Window.partitionBy('trip_id')
eta_max_min = eta.withColumn('max_last_stop_arrival_time', f.max('last_stop_arrival_time').over(w))\
    .withColumn('min_actual_trip_start_time', f.min('actual_trip_start_time').over(w))\
    .withColumn('max_observationDateTime', f.min('observationDateTime').over(w))\
    .withColumn('max_latitude', f.max('latitude').over(w))\
    .withColumn('max_longitude', f.min('longitude').over(w))\
    .where((f.col('min_actual_trip_start_time') == f.col('actual_trip_start_time')) | (f.col('max_last_stop_arrival_time') == f.col('last_stop_arrival_time')) | (f.col('max_longitude') == f.col('longitude')) | (f.col('max_latitude') == f.col('latitude')) | (f.col('observationDateTime') == f.col('max_observationDateTime'))).select("primary_key","trip_id","max_last_stop_arrival_time","min_actual_trip_start_time","max_latitude","max_longitude","max_observationDateTime","license_plate").dropDuplicates(["trip_id","max_last_stop_arrival_time","min_actual_trip_start_time","max_latitude","max_longitude","max_observationDateTime","license_plate"]).sort("trip_id")

"""# Collect Stop IDs as List in Schedules Data"""


sch_stops = sch.dropDuplicates(["trip_id","stop_id"]).groupBy("trip_id").agg(f.collect_list("stop_id").alias("stop_id")).sort("trip_id")

"""# Collect Stop IDs as List in Live Data"""


eta_common = sch.join(eta,"trip_id","inner")
eta_stops = eta_common.dropDuplicates(["trip_id","last_stop_id"]).groupBy("trip_id").agg(f.collect_list("last_stop_id").alias("last_stop_id")).sort("trip_id")

"""# Merge Stop IDs Data for Schedules And Live"""


common_stops = eta_stops.join(sch_stops,"trip_id","inner").sort("trip_id")
common_stops = common_stops.withColumn('common_stop_id', f.array_intersect("last_stop_id","stop_id"))
common_stops = common_stops.withColumn('size_sch_stops', f.size("stop_id"))\
            .withColumn('size_eta_stops', f.size("last_stop_id"))\
            .withColumn('size_common_stops', f.size("common_stop_id"))

"""# Get Percentage of Stops Completed"""


common_stops = common_stops.withColumn('percent', f.col("size_common_stops")/f.col("size_sch_stops"))

"""# Join Common Stops Dataframe with Live Dataframe and Common Stops"""


merged_df = sch_max_min.join(eta_max_min,"trip_id","inner")
merged_df = merged_df.join(common_stops,"trip_id","inner")

"""# Convert Epoch Timestamp to UTC Timestamp for Calculation Purposes"""


merged_df=merged_df.withColumn('utc_start_time',f.to_timestamp(f.col('start_time')))\
  .withColumn('utc_end_time', f.to_timestamp(f.col('end_time')))\
  .withColumn('utc_observationDateTime',f.to_timestamp(f.col('max_observationDateTime')))\
  .withColumn("curr_time",f.current_timestamp()+f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
  .withColumn('total_sch_time',(f.col("utc_end_time").cast("long") - f.col('utc_start_time').cast("long"))/60.0)\
  .withColumn('delta_min',(f.col("curr_time").cast("long") - f.col('utc_observationDateTime').cast("long"))/60.0)\
  .sort("trip_id", ascending=False)

"""# Classify Status of Trips"""


threshold_pct = 0.5
threshold_min = 5000
late_threshold_min = 60
merged_df = merged_df.withColumn("status", f.when((f.col("percent")>=threshold_pct) & (f.col("max_last_stop_arrival_time")+f.col("total_sch_time")*(1-threshold_pct)+(threshold_min*60)>f.col("end_time")) , "COMPLETED").when((f.col("percent")<threshold_pct) & (f.col("delta_min")>=late_threshold_min),"INCOMPLETE").when((f.col("percent")<threshold_pct) & (f.col("delta_min")<late_threshold_min),"LIVE"))

"""# Merge the Dataframe with Not Started Trips with"""


merged_df = merged_df.join(not_yet_started_trips_max_min,"trip_id","outer")

"""# Timestamp to filter out Not Started Trips and Missed Trips"""


missed_now = datetime.now(IST)
missed_start_time = missed_now.strftime("'%Y-%m-%d %H:%M:%S'")
missed_epoch = int(time.mktime(time.strptime(missed_start_time, "'%Y-%m-%d %H:%M:%S'")))
print(missed_epoch)

"""# Classify Not Yet Started Trips as Not Started and Missed"""


merged_df = merged_df.withColumn("status",f.when((f.col("status").isNull()) & (f.col("stop_start_time")<missed_epoch),"MISSED").when((f.col("status").isNull()) & (f.col("stop_start_time")>=missed_epoch),"NOT STARTED").otherwise(f.lit(f.col("status"))))

"""# Concatenate Timestamp Columns to get Last Stop Arrival Time """


merged_df = merged_df.select(f.col("trip_id"),f.col("status"),f.col("license_plate"),f.col("total_sch_time").alias("trip_time"),f.concat_ws('',f.col("max_last_stop_arrival_time"),f.col("stop_end_time")).alias("last_stop_arrival_time"),f.col("max_latitude").alias("latitude"),f.col("max_longitude").alias("longitude"))

"""# Add Observation Datetime for Superset purposes"""


merged_df = merged_df.withColumn("observe_time",f.current_timestamp())\
                     .withColumn("observe_time", f.col('observe_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
                     .withColumn("observe_time",f.col("observe_time").cast("long"))

"""# Convert Timestamps from seconds to microseconds"""


merged_df = merged_df.withColumn("observe_time",f.col("observe_time")*1000000)\
.withColumn("last_stop_arrival_time",f.col("last_stop_arrival_time")*1000000)

"""# Data Type Cast"""


merged_df = merged_df.withColumn("last_stop_arrival_time", f.col("last_stop_arrival_time").cast('long'))
merged_df = merged_df.withColumn("trip_id", f.col("trip_id").cast('String'))

"""# Add Primary Key"""


# merged_df = merged_df.withColumn("primary_key",f.concat(f.col("trip_id").cast("String"),f.current_date().cast("String")))


def getPrimaryKey(trip_id):
    now = datetime.now(IST)
    start_time = now.strftime("'%Y-%m-%d %H:%M:%S'")
    epoch = int(time.mktime(time.strptime(start_time, "'%Y-%m-%d %H:%M:%S'")))
    return str(trip_id)+""+str(epoch)
getPrimaryKeyUDF = f.udf(lambda trip_id: getPrimaryKey(trip_id),StringType())


merged_df = merged_df.withColumn("primary_key", getPrimaryKeyUDF(f.col("trip_id")))

"""# Filter out Last One Hour Data"""


merged_df1 = merged_df[merged_df["last_stop_arrival_time"]>=trim_epoch*1000000]

"""# Push to Kudu"""


merged_df1.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "trip_status")\
.save()


merged_df = merged_df.withColumn("primary_key",f.concat(f.col("trip_id").cast("String"),f.current_date().cast("String")))
merged_df.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "trip_status_live")\
.save()

"""# Get Completed Trips"""


completed_trip_id = merged_df[merged_df['status']=="COMPLETED"].select("trip_id").sort("trip_id")

completed_trips = completed_trip_id.join(eta,"trip_id","inner").select("trip_id","trip_delay","license_plate")

"""# Get Completed Trips with Delay"""


grp_window = Window.partitionBy('trip_id')
magic_percentile = f.expr("percentile_approx(trip_delay,0.5)")

completed_trips_median = completed_trips.withColumn('trip_delay', magic_percentile.over(grp_window)).select("trip_id","trip_delay","license_plate").dropDuplicates(["trip_id","trip_delay","license_plate"]).sort("trip_id")

completed_trips_median = completed_trips_median.withColumn("primary_key",f.concat(f.col("trip_id").cast("String"),f.current_date().cast("String")))\
                                                .withColumn("trip_id",f.col("trip_id").cast("String"))

completed_trips_median = completed_trips_median.withColumn("observe_time",f.current_timestamp())\
                     .withColumn("observe_time", f.col('observe_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
                     .withColumn("observe_time",f.col("observe_time").cast("long"))\
                     .withColumn("observe_time",f.col("observe_time")*1000000)

completed_trips_median = completed_trips_median.withColumn("primary_key", getPrimaryKeyUDF(f.col("trip_id")))


"""# Push to Kudu"""


completed_trips_median.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "completed_trip_with_delay")\
.save()

"""# Get Live Trips"""


live_trip_id = merged_df[merged_df['status']=="LIVE"].select("trip_id").sort("trip_id")

live_trips = live_trip_id.join(eta,"trip_id","inner").select("trip_id","trip_delay","license_plate")

"""# Get Live Trips with Delay"""


grp_window = Window.partitionBy('trip_id')
magic_percentile = f.expr("percentile_approx(trip_delay,0.5)")

live_trips_median = live_trips.withColumn('trip_delay', magic_percentile.over(grp_window)).select("trip_id","trip_delay","license_plate").dropDuplicates(["trip_id","trip_delay","license_plate"]).sort("trip_id")


live_trips_median = live_trips_median.withColumn("primary_key",f.concat(f.col("trip_id").cast("String"),f.current_date().cast("String")))\
                                                .withColumn("trip_id",f.col("trip_id").cast("String"))


live_trips_median = live_trips_median.withColumn("observe_time",f.current_timestamp())\
                     .withColumn("observe_time", f.col('observe_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
                     .withColumn("observe_time",f.col("observe_time").cast("long"))\
                     .withColumn("observe_time",f.col("observe_time")*1000000)
live_trips_median = live_trips_median.withColumn("primary_key", getPrimaryKeyUDF(f.col("trip_id")))

"""# Push to Kudu"""


live_trips_median.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "live_trip_with_delay")\
.save()

"""# Get Completed Trips with Deviation"""


completed_trips_with_deviation = completed_trip_id.join(common_stops,"trip_id","inner")
completed_trips_with_deviation = completed_trips_with_deviation.withColumn("deviation_status",f.when(f.col("size_eta_stops")==f.col("size_common_stops"),0).otherwise(1))
completed_trips_with_deviation = completed_trips_with_deviation[completed_trips_with_deviation["deviation_status"]==1]
completed_trips_with_deviation = completed_trips_with_deviation.withColumn("primary_key",f.concat(f.col("trip_id").cast("String"),f.current_date().cast("String")))\
                                                .withColumn("trip_id",f.col("trip_id").cast("String"))\
                                                .withColumn("eta_stops",f.col("last_stop_id").cast("String"))\
                                                .withColumn("sch_stops",f.col("stop_id").cast("String"))\
                                                .withColumn("common_stops",f.col("common_stop_id").cast("String"))


completed_trips_with_deviation = completed_trips_with_deviation.select("trip_id","eta_stops","sch_stops","common_stops",f.col("size_eta_stops").alias("eta_stops_count"),f.col("size_sch_stops").alias("sch_stops_count"),f.col("size_common_stops").alias("common_stops_count"))


completed_trips_with_deviation = completed_trips_with_deviation.withColumn("observe_time",f.current_timestamp())\
                     .withColumn("observe_time", f.col('observe_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
                     .withColumn("observe_time",f.col("observe_time").cast("long"))\
                     .withColumn("observe_time",f.col("observe_time")*1000000)

completed_trips_with_deviation = completed_trips_with_deviation.join(eta_max_min,"trip_id","left").select("trip_id", "eta_stops", "sch_stops","common_stops","eta_stops_count","sch_stops_count","common_stops_count","observe_time","license_plate")

completed_trips_with_deviation = completed_trips_with_deviation.withColumn("primary_key",f.concat(f.col("trip_id").cast("String"),f.current_date().cast("String")))\
                                                .withColumn("trip_id",f.col("trip_id").cast("String"))

completed_trips_with_deviation = completed_trips_with_deviation.withColumn("primary_key", getPrimaryKeyUDF(f.col("trip_id")))

"""# Push to Kudu"""


completed_trips_with_deviation.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "completed_trip_with_deviation")\
.save()

"""# Get Live Trips with Deviation"""


live_trips_with_deviation = live_trip_id.join(common_stops,"trip_id","inner")
live_trips_with_deviation = live_trips_with_deviation.withColumn("deviation_status",f.when(f.col("size_eta_stops")==f.col("size_common_stops"),0).otherwise(1))
live_trips_with_deviation = live_trips_with_deviation[live_trips_with_deviation["deviation_status"]==1]
live_trips_with_deviation = live_trips_with_deviation.withColumn("primary_key",f.concat(f.col("trip_id").cast("String"),f.current_date().cast("String")))\
                                                .withColumn("trip_id",f.col("trip_id").cast("String"))\
                                                .withColumn("eta_stops",f.col("last_stop_id").cast("String"))\
                                                .withColumn("sch_stops",f.col("stop_id").cast("String"))\
                                                .withColumn("common_stops",f.col("common_stop_id").cast("String"))


live_trips_with_deviation = live_trips_with_deviation.select("trip_id","eta_stops","sch_stops","common_stops",f.col("size_eta_stops").alias("eta_stops_count"),f.col("size_sch_stops").alias("sch_stops_count"),f.col("size_common_stops").alias("common_stops_count"))

live_trips_with_deviation = live_trips_with_deviation.withColumn("observe_time",f.current_timestamp())\
                     .withColumn("observe_time", f.col('observe_time') + f.expr('INTERVAL 5 HOURS 30 MINUTES'))\
                     .withColumn("observe_time",f.col("observe_time").cast("long"))\
                     .withColumn("observe_time",f.col("observe_time")*1000000)

live_trips_with_deviation = live_trips_with_deviation.join(eta_max_min,"trip_id","left").select("trip_id", "eta_stops", "sch_stops","common_stops","eta_stops_count","sch_stops_count","common_stops_count","observe_time","license_plate")

live_trips_with_deviation = live_trips_with_deviation.withColumn("primary_key",f.concat(f.col("trip_id").cast("String"),f.current_date().cast("String")))\
                                                .withColumn("trip_id",f.col("trip_id").cast("String"))

live_trips_with_deviation = live_trips_with_deviation.withColumn("primary_key", getPrimaryKeyUDF(f.col("trip_id")))


"""# Push to Kudu"""


live_trips_with_deviation.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "live_trip_with_deviation")\
.save()


print("=="*50)
print("Done")
