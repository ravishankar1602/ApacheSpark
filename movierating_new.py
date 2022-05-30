from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

#using broadcast variable in code
def loadmoviename():
    movie_name = {}

    with codecs.open(r"C:/sparkcourse/ml-100k/item.txt","r", encoding = "ISO-8859-1", errors = "ignore") as f:

        for line in f:
            field = line.split("|")
            movie_name[int(fields[0])] = fields[1]

        return movie_name


spark = SparkSession.builder.appName("MovieRating_new").getOrCreate()

schema = StructType ([ \
                        StructField("UserID", IntegerType(), True), \
                        StructField("movieID", IntegerType(), True), \
                        StructField("rating", IntegerType(), True), \
                        StructField("timestamp", LongType(), True), \
                    ])

movieDF = spark.read.option("sep", "\t").schema(schema).csv("file:///sparkcourse/ml-100k/data.txt")

#C:\sparkcourse\ml-100k\ml-100k\u


movieCounts = movieDF.groupBy("movieID").count()


namedict = spark.sparkContext.broadcast(loadmoviename())

def lookupname(movieID):
    return namedict.value(movieID)

lookupnameUDF = func.udf(lookupname)


#add a movie title column in using new udf
movieWithname = moviecounts.withColumn("movieTitle", lookupnameUDF(func.col("movieID")))

#sort the result
sortedMovies = movieWithname.orderBy(func.desc("count"))

#grab the top 10
movieWithname.show(10, False)

#stop the SparkSession

spark.stop()
