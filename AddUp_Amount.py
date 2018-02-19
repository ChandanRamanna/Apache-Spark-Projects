from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Add up amount spent by customer Sorted by Amount")
sc = SparkContext(conf = conf)

def data(line):
    fields = line.split(',')
    CustomerID = fields[0]
    DollarAmt = fields[2]
    return (int(CustomerID), float(DollarAmt))


lines = sc.textFile("/Users/chandanramanna/Desktop/Spark/customer-orders.csv")
data = lines.map(data)

totalsum = data.reduceByKey(lambda x, y: x + y)

totalsumSorted = totalsum.map(lambda x: (x[1], x[0])).sortByKey()

results = totalsumSorted.collect()

for totalsum in results:
    print(totalsum[0], totalsum[1])