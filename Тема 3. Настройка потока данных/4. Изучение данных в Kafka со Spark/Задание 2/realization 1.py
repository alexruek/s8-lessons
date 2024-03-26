from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import from_json, col

# необходимая библиотека с идентификатором в maven
# вы можете использовать ее с помощью метода .config и опции "spark.jars.packages"
# Указываем версию библиотеки для работы с Kafka
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

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
        .config("spark.jars.packages", spark_jars_packages) \
        .getOrCreate()
    return spark


def load_df(spark: SparkSession) -> DataFrame:
    """
        Загрузка данных из Kafka в DataFrame.
        """
    df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091")
        .option("subscribe", "persist_topic")
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
        StructField("subscription_id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("description", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("topic", StringType(), True),
        StructField("partition", IntegerType(), True),
        StructField("offset", LongType(), True),  # Изменено на LongType
        StructField("timestamp", TimestampType(), True),  # Изменено на TimestampType
        StructField("timestampType", IntegerType(), True)
    ])

    # десериализуем из колонки value сообщения JSON
    transform_df = df.select(from_json(col("value").cast("string"), incomming_message_schema).alias("parsed_key_value"))

    return transform_df

spark = spark_init()

source_df = load_df(spark)
df = transform(source_df)


df.printSchema()
df.show(truncate=False)

