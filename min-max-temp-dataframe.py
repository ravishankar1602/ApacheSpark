from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pysparl.sql.types import StructTypes, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinMaxTemp").getOrCreate()

schema = StructTypes([ \
                        StructField("stationID", StringType(), True), \
                        StructField("Date", IntegerType(), True), \
                        StructField("measure_type", StringType(), True), \
                        StructField("temprature", FloatType(), True)])

df = spark.read.schema(schema).csv("file:\\\SparkCourse/1800.csv")
df.printSchema()

mintemp = df.filter(df.measure_type == "TMIN")
stationTemps = minTemps.select("stationID", "temprature")

minTempsByStation = stationTemps.groupBy("stationID").min("temprature")
minTempsByStation.show()

spark.stop()
