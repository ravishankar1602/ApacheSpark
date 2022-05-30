from pyspark import SparkConf, SparkContext #importing the required packages

conf = SparkConf().setMaster("local").setAppName("FriendsByAge") #local means this will run on local machine not on cluster
sc = SparkContext(conf = conf)

def ParseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///sparkcourse/fakefriends.csv") #load up the data from the local machine

rdd = lines.map(ParseLine)
totalByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
#above line will count the sum of friends and no of entries per age
#rdd.mapValues(lambda x: (x, 1)) will do the following (33,385),(33,2) -->(33,(385,1)),(33,(2,1))
#reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) will add up the value for each unique key(age) ex: (33,(387,2))

AvgByage = (totalByAge.mapValues(lambda x: x[0] / x[1]))#calculates the avg age

results = AvgByage.collect()
for result in results:
    print (result)
