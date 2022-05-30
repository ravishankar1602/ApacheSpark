from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType


spark = SparkSession.builder.appName("TotalAmount").getOrCreate()

schema = StructType([\
                        StructField("CustId", IntegerType(), True),
                        StructField("Item", IntegerType(), True),
                        StructField("Amount", FloatType(), True)])

df = spark.read.schema(schema).csv("file:///sparkcourse/customer-orders.csv")

total = df.groupBy("custId").agg(func.round(func.sum("Amount"), 2).alias("total_amount"))

sorted_total = total.sort("total_amount")

sorted_total.show()

spark.stop()
