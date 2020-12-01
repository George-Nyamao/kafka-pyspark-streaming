from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as f
from pyspark.sql.types import *
import time
import os

os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"
os.environ["SPARK_HOME"] = "/usr/local/spark-3.0.1"

spark_sql_kafka = "/usr/local/spark-3.0.1/jars/spark-sql-kafka-0-10_2.12-3.0.1.jar"
kafka_clients = "/usr/local/spark-3.0.1/jars/kafka-clients-2.6.0.jar"

kafka_topic_name = "capstone3"
kafka_bootstrap_servers = 'localhost:9092'

if __name__ == "__main__":
    print("Welcome to Kafka !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    # Create spark session
    spark = SparkSession \
        .builder \
        .appName("spark-kafka2hive") \
        .master("local[*]") \
        .config("spark.jars", spark_sql_kafka) \
        .config("spark.jars", kafka_clients) \
        .config("spark.sql.warehouse.dir", '/hive/warehouse') \
        .enableHiveSupport() \
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions", 2)
    spark.sparkContext.setLogLevel("ERROR")

    # Dataframe reads topic
    data_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()
    #
    print("Printing Schema of orders_df: ")
    data_df.printSchema()

    stock_df = data_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define Schema
    stock_schema = StructType() \
        .add("date", StringType()) \
        .add("open", StringType()) \
        .add("high", StringType()) \
        .add("low", StringType()) \
        .add("close", StringType()) \
        .add("volume", StringType())

    # Applying the schema
    stock_df1 = stock_df\
        .select(from_json(f.col("value"), stock_schema)\
        .alias("wmt_stock"), "timestamp")

    stock_df2 = stock_df1.select("wmt_stock.*", "timestamp")
    print('Printing the schema of stock_df2:')
    stock_df2.printSchema()

    # Write to console
    query = stock_df2 \
       .writeStream \
       .trigger(processingTime='5 seconds') \
       .outputMode("update") \
       .option("truncate", "false")\
       .format("console") \
       .start()

    #Function to write table to hive
    def handle_hive(df, batch_id):
        df.write.saveAsTable(name='stocks.wmtstock', format='hive', mode='append')

    #Write to console
    query1 = stock_df2\
        .writeStream\
        .foreachBatch(handle_hive)\
        .start()

    #fFunction to write table to MySQL
    def handle_mysql(df, batch_id):
        df.write.format("jdbc").mode("append") \
            .option("url", "jdbc:mysql://localhost/stocks?useSSL=false") \
            .option("dbtable", "wmtstock") \
            .option("user", "morara") \
            .option("password", "d3barl") \
            .save()

    # Write to MySQL
    query2 = stock_df2\
        .writeStream\
        .foreachBatch(handle_mysql)\
        .start()

    query2.awaitTermination()

    print("Stream process complete ...")
