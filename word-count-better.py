import re
from pyspark import SparkContext, SparkConf

def normalword(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCountBetter")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalword)
wordcount = words.countByValue()

for word, count in wordcount.items():
    cleanword = word.encode('ascii', 'ignore')
    if(cleanword):
        print(cleanword.decode() + " " + str(count))
