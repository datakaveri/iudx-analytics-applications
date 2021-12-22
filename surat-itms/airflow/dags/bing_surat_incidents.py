import requests
import pyspark.sql.functions as f
from pyspark.sql import Window
from pyspark.sql import SparkSession


url='http://dev.virtualearth.net/REST/v1/Traffic/Incidents/21.064015,72.651803,21.355104,72.980239?key=Ah_NTRarD2fECte9W44cJf0f-qq6eAa6reSDZc-1gaz3A9YUtSPRtUxbHKmezmms'
result=requests.get(url).json()

type_dict={
    1: 'Accident',
    2: 'Congestion',
    3: 'DisabledVehicle',
    4: 'MassTransit',
    5: 'Miscellaneous',
    6: 'OtherNews',
    7: 'PlannedEvent',
    8: 'RoadHazard',
    9: 'Construction',
    10: 'Alert',
    11: 'Weather',
}

rows=[]
for resource in result['resourceSets'][0]['resources']:
    row=[]
    row.append(resource['point']['coordinates'][0])
    row.append(resource['point']['coordinates'][1])
    row.append(resource['description'])
    row.append(int(resource['end'].strip('/Date()'))*1000)
    row.append(int(resource['icon']))
    row.append(resource['incidentId'])
    row.append(resource['isEndTimeBackfilled'])
    row.append(int(resource['lastModified'].strip('/Date()'))*1000)
    row.append(resource['roadClosed'])
    row.append(resource['severity'])
    row.append(resource['source'])
    row.append(int(resource['start'].strip('/Date()'))*1000)
    if 'title' in resource.keys():
        row.append(resource['title'])
    else:
        row.append('')
    row.append(resource['toPoint']['coordinates'][0])
    row.append(resource['toPoint']['coordinates'][1])
    row.append(resource['type'])
    row.append(type_dict[resource['type']])
    row.append(resource['verified'])
    rows.append(row)

cols=['latitude', 'longitude', 'description', 'end', 'icon', 'incidentId', 'isEndTimeBackfilled', 'lastModified', 'roadClosed', 'severity', 'source', 'start', 'title', 'toPoint_latitude', 'toPoint_longitude', 'type_int', 'type_string', 'verified']
spark = SparkSession.builder.master('spark://spark:7077').appName('kudu_read').getOrCreate()
df = spark.createDataFrame(rows, cols)

w = Window().orderBy(f.lit('A'))
df = df.withColumn("primary_key",f.concat(f.expr("uuid()"),f.monotonically_increasing_id(),f.current_date().cast("String"),f.row_number().over(w).cast("String")))
df=df.withColumn("icon", f.col("icon").cast("int"))
df=df.withColumn("severity", f.col("severity").cast("int"))
df=df.withColumn("source", f.col("source").cast("int"))
df=df.withColumn("type_int", f.col("type_int").cast("int"))

df = df.withColumn("end",f.col("end")+(330 * 60 * 1000000))
df = df.withColumn("lastModified",f.col("lastModified")+(330 * 60 * 1000000))
df = df.withColumn("start",f.col("start")+(330 * 60 * 1000000))


df.write.format('org.apache.kudu.spark.kudu')\
.option('kudu.master', "kudu-master:7051") \
.mode('append') \
.option('kudu.table', "bing_surat_traffic_incidents")\
.save()
