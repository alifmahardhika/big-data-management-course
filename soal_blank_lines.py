from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *

conf = SparkConf().setMaster("local[*]").setAppName("Accumulator")
spark = SparkSession.builder.getOrCreate()

blank_count = spark.sparkContext.accumulator(0)

print("blank line starting count:" , blk_cnt.value)

def blank_lines(line):
    if(len(line) == 0):
        blk_cnt.add(1)

file = spark.sparkContext.textFile("README.md")

r1 = file.foreach(lambda x: blank_lines(x))

print("blank line final count: " ,blank_count.value)