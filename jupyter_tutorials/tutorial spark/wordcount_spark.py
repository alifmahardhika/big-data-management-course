# NOTE gabuat dijalanin dilocal
# error gangerti bye
from operator import add
from pyspark import SparkContext
sc = SparkContext("local[*]", "example")
f = sc.textFile("data/README.md")
wc = f.flatMap(lambda x: x.split(' ')).map(lambda x: (x, 1)).reduceByKey(add)
wc.saveAsTextFile("wc_out")
