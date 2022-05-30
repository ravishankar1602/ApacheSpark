from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


#importing the file and reading the header and schema of the data
people = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///sparkcourse/fakefriends-header.csv")

print("Here is schema")
people.printSchema()

print("displaying the column name")
people.select("name").show()

print("filter out over 21")
people.filter(people.age < 21).show()

print("group by age")
people.groupBy("age").count().show()

print("making everyone 10 years older")
people.select(people.name, people.age + 10).show()

spark.stop()
