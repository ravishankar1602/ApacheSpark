from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(lambda x: x.split())
wordcounts = words.countByValue()

for word, count in wordcounts.items():
    cleanword = word.encode('ascii', 'ignore')
    if(cleanword):
        print(cleanword.decode() + " " + str(count))
