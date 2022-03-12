import os,sys
import findspark 
findspark.init()

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
# sc = SparkContext(master="local", appName="test")
sc = SparkContext.getOrCreate()
spark = SparkSession(sc)


DATA_DIR='/home/sarbajit/Documents/Programs/spark/Data'

def shutdown():
    spark.stop()

# shutdown()