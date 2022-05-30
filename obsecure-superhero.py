from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("ObsecureSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///sparkcourse/Marvel+Names")
lines = spark.read.text("file:///sparkcourse/Marvel+Graph")

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

min_connection_count = connections.agg(func.min("connections")).first()[0]



min_connection = connections.filter(func.col("connections") == min_connection_count)

min_connection_name = min_connection.join(names, "id")

min_connection_name.select("name").show()
