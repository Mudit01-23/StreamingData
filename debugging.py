import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("StreamingData") \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.12.0") \
            .getOrCreate()
        logger.info("Spark session created successfully")
        return spark
    except Exception as e:
        logger.error(f"Error creating Spark session: {e}")
        raise

def read_stream(spark):
    try:
        kafka_df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "your_topic") \
            .load()
        logger.info("Kafka stream read successfully")
        return kafka_df
    except Exception as e:
        logger.error(f"Error reading Kafka stream: {e}")
        raise

def process_stream(kafka_df):
    try:
        schema = StructType([
            StructField("room", StringType(), True),
            StructField("co2", FloatType(), True),
            StructField("humidity", FloatType(), True),
            StructField("light", FloatType(), True),
            StructField("pir", FloatType(), True),
            StructField("temperature", FloatType(), True),
            StructField("if_movement", StringType(), True),
            StructField("event_ts_min", TimestampType(), True)
        ])

        value_df = kafka_df.selectExpr("CAST(value AS STRING)")
        json_df = value_df.select(from_json(col("value"), schema).alias("data"))
        final_df = json_df.select("data.*")
        logger.info("Dataframe created from Kafka stream")

        final_df.printSchema()  # Print schema for debugging
        final_df.show(n=5)  # Show a few rows for debugging

        return final_df
    except Exception as e:
        logger.error(f"Error processing stream: {e}")
        raise

def write_to_elasticsearch(final_df):
    try:
        es_write_conf = {
            "es.nodes": "localhost",
            "es.port": "9200",
            "es.resource": "office_input/_doc",
            "es.input.json": "true"
        }
        query = final_df.writeStream \
            .outputMode("append") \
            .format("org.elasticsearch.spark.sql") \
            .options(**es_write_conf) \
            .start()
        logger.info("Streaming to Elasticsearch started")
        query.awaitTermination()
    except Exception as e:
        logger.error(f"Error writing to Elasticsearch: {e}")
        raise

def main():
    spark = create_spark_session()
    kafka_df = read_stream(spark)
    final_df = process_stream(kafka_df)
    write_to_elasticsearch(final_df)

if __name__ == "__main__":
    main()