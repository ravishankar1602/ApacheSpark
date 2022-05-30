from pyspark.sql import SparkSession, Row


#creating a spark session
spark = SparkSession.builder.appName("sparkSQL").getOrCreate()

def mapper(line):
    field = line.split(',')
    return Row(ID = int(field[0]), name = str(field[1]), age = int(field[2]), numfrnds = int(field[3]))

#importing the file
lines = spark.sparkContext.textFile("fakefriends.csv")

#returning the the data with column names
people = lines.map(mapper)

#infer the schema and register the dataframe as table
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

#running the sql over the dataframe that registered as the table
teens = spark.sql("select * from people where age >=13 and age <=19")

#collecting the results

for i in teens.collect():
    print(i)
#can also use functions instead of sql queries to display the results
schemaPeople.groupby("age").count().orderBy("age").show()


spark.stop()
