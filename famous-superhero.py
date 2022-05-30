from pyspark.sql import functions as func
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

Spark = SparkSession.builder.appName("FamousSuperhero").getOrCreate()

schema = StructType ([ \

                        StructField("id", IntegerType(), True),\
                        StructField("name", StringType(), True)
                    ])


names = Spark.read.schema(schema).option("sep", " ").csv("file:///sparkcourse/Marvel+Names")

lines = Spark.read.text("file:///sparkcourse/Marvel+Graph")

Connections = lines.withColumn("id", func.split(func.col("Value"), " ")[0]) \
                .withColumn("connections", func.size(func.split(func.col("Value"), " ")) -1) \
                .groupBy("id").agg(func.sum("connections").alias("connections"))


mostpopular = Connections.sort(func.col("connections").desc()).first()

mostpopularname = names.filter(func.col("id") == mostpopular[0]).select("name").first()

print(mostpopularname[0] + " is most popular with " + str(mostpopular[1]) + " co-appear")
