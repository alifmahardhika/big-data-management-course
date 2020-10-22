from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *

conf = SparkConf().setMaster("local[*]").setAppName("Accumulator")
spark = SparkSession.builder.getOrCreate()

blk_cnt = spark.sparkContext.accumulator(0)

print("Initial Value of Accumulator:" , blk_cnt.value)

def Blank_lines(line):
    if(len(line) == 0):
        blk_cnt.add(1)

file = spark.sparkContext.textFile("data/README.md")

r1 = file.foreach(lambda x: Blank_lines(x))

print("Updated Value of Accumulator: " ,blk_cnt.value)