from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemprature")
sc = SparkContext(conf = conf)

def ParseLine(line):
    fields = line.split(',')
    stationId = fields[0]
    entryType = fields[2]
    temp = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return(stationId,entryType,temp)

lines = sc.textFile("file:///sparkcourse/1800.csv")
parsedLines = lines.map(ParseLine)
mintemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationtemps = mintemps.map(lambda x: (x[0], x[2]))
mintemps = stationtemps.reduceByKey(lambda x, y: min(x, y))
results = mintemps.collect()

for i in results:
    print(i)


#for result in results:
    #print (result[0] + "\t{:.2f}F".format(result[1]))
