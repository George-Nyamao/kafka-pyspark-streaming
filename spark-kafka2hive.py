from pyspark.sql import SparkSession
from pyspark.sql.types import (StructField,StringType,IntegerType,FloatType,StructType)
from pyspark.sql import functions as func
from pyspark.sql.functions import *
# from pyspark.sql import types as ty
import time


if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Application Started ...")

    spark = SparkSession \
        .builder \
        .appName("spark-kafka2hive") \
        .master("local[*]") \
        .config("spark.jars", "/usr/local/spark-3.0.1/jars/mysql-connector-java-8.0.17.jar") \
        .enableHiveSupport()\
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 2)
    spark.sparkContext.setLogLevel("ERROR")

    msft_df = spark\
            .readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers","localhost:9092")\
            .option("subscribe","capstone14")\
            .load()

    msft_df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    msft_df2 = msft_df.select(func.get_json_object(func.col("value").cast("string"), '$[*].open').alias('open'),
                    func.get_json_object(func.col("value").cast("string"), '$[*].high').alias('high'),
                    func.get_json_object(func.col("value").cast("string"), '$[*].low').alias('low'),
                    func.get_json_object(func.col("value").cast("string"), '$[*].close').alias('close'),
                    func.get_json_object(func.col("value").cast("string"), '$[*].volume').alias('volume'),
                    func.get_json_object(func.col("value").cast("string"), '$[*].date').alias('date'))

    msft_df2.printSchema()

    msft_df3 = msft_df2 \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false") \
        .format("console") \
        .start()

    def handle_rdd(df, batch_id):
        df.write.saveAsTable(name='capstone.msstock', format='hive', mode='append')
    #query = msft_df2.writeStream.foreachBatch(handle_rdd).start()
    
    msft_df3.awaitTermination()