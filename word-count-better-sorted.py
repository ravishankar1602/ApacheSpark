import re
from pyspark import SparkContext, SparkConf

def normalword(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


conf = SparkConf().setMaster("local").setAppName("WordCountBetterSorted")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalword)

wordcount = words.map(lambda x:(x, 1)).reduceByKey(lambda x, y: x + y)
wordcountsorted = wordcount.map(lambda x:(x[1], x[0])).sortByKey()

result = wordcountsorted.collect()

for results in result:
    count = str(results[0])
    word = results[1].encode('ascii', 'ignore')
    if(word):
        print(word.decode() + ":\t" + count)
