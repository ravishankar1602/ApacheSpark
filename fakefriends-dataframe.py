from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLfakefriends").getOrCreate()

line = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///sparkcourse/fakefriends-header.csv")

print("here is schema")
line.printSchema()

print("Total friends by group")

friends = line.select("friends", "age")

friends.groupBy("age").avg("friends").show()

spark.stop()
