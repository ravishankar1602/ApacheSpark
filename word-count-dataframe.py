from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("file:///SparkCourse/book.txt")

words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))

words.filter(words.word != "")

lowercasewords = words.select(func.lower(words.word).alias("word"))

wordcounts = lowercasewords.groupBy("word").count()

wordcountsorted = wordcount.sort("count")

wordcountsorted.show(wordcountsorted.count())

spark.stop()
