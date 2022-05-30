from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("TotalAmountByCustomer")
sc = SparkContext(conf = conf)

def ParseLine(line):
    field = line.split(',')
    customer = int(field[0])
    amount = float(field[2])

    return(customer, amount)

input = sc.textFile("file:///sparkcourse/customer-orders.csv")
rdd = input.map(ParseLine)

TotalAmountByCustomer = rdd.reduceByKey(lambda x, y: x + y)

totalAmountSorted = TotalAmountByCustomer.map(lambda x: (x[1], x[0])).sortByKey()
results = totalAmountSorted.collect()

for result in results:
    print(result)
