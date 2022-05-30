from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType

spark = SparkSession.builder.appName("MovieRating").getOrCreate()

schema = StructType([ \
                        StructField("UserID", IntegerType(), True), \
                        StructField("movie", IntegerType(), True), \
                        StructField("rating", IntegerType(), True), \
                        StructField("timestamp", LongType(), True), \
                    ])
#loading up the data

movieDF = spark.read.option("sep", "\t").schema(schema).csv(r"file:///sparkcourse\ml-100k\ml-100k\u.txt")

topmovie= movieDF.groupBy("movie").count().orderBy(func.desc("count"))

topmovie.show(10)

spark.stop()
