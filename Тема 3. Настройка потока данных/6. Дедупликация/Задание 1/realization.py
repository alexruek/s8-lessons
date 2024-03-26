from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType

TOPIC_NAME = 'student.topic.cohort20.alexanderpavlyuk'

# необходимая библиотека с идентификатором в maven
kafka_lib_id = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"
    ]
)

# настройки security для кафки
# вы можете использовать из с помощью метода .options(**kafka_security_options)
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}

def spark_init() -> SparkSession:
    """
        Инициализация SparkSession.
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName("Connect to kafka") \
        .config("spark.jars.packages", kafka_lib_id) \
        .getOrCreate()
    return spark


def load_df(spark: SparkSession) -> DataFrame:
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
        .option("subscribe", "TOPIC_NAME")
        .options(**kafka_security_options)
        .load()
    )
    return df


def transform(df: DataFrame) -> DataFrame:
    """
            Преобразование DataFrame.
    """
    # Определяем схему входного сообщения для JSON
    incomming_message_schema = StructType([
        StructField("client_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ])

    # десериализуем из колонки value сообщения JSON и удалим дубликаты
    transform_df = df \
                .withColumn('value', f.col('value').cast(StringType())) \
                .withColumn('event', f.from_json(f.col('value'), incomming_message_schema)) \
                .selectExpr('event.*') \
                .withWatermark("timestamp", "10 minutes") \
                .dropDuplicates(["client_id", "timestamp"])

    return transform_df


spark = spark_init()

source_df = load_df(spark)
output_df = transform(source_df)
output_df.printSchema()

query = (output_df
         .writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", False)
         .trigger(once=True)
         .start())
try:
    query.awaitTermination()
finally:
    query.stop()