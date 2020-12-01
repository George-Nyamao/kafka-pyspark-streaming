from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
from os.path import abspath
import unittest

warehouse_loc =abspath('spark-warehouse')

spark=SparkSession\
    .builder.appName("rdbms_unittest")\
    .master("local[*]")\
    .config("spark.jars","/usr/local/spark-3.0.1/jars/mysql-connector-java-8.0.22.jar")\
    .config("spark.sql.warehouse.dir", warehouse_loc)\
    .enableHiveSupport()\
    .getOrCreate()

rdbms_df = spark.read.format("jdbc")\
    .option('url', 'jdbc:mysql://localhost:3306/stocks')\
    .option('user', 'morara')\
    .option('password', 'd3barl')\
    .option("query","select close from stocks.wmtstock order by date desc limit 5")\
    .load()

mysql_df = rdbms_df.select(avg('close').alias('mysql_mean'))
mysql_avg =mysql_df.collect()[0].__getitem__("mysql_mean")

hive_df= spark.sql('select round(avg(*),2) as hive_mean from (select close from stocks.wmtstock order by date desc limit 5)')
hive_avg = hive_df.collect()[0].asDict()["hive_mean"]

def mysql_hive_test():
    return mysql_avg == hive_avg
