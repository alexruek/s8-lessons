from pyspark.sql import SparkSession

# Указываем версию библиотеки для работы с Kafka
spark_jars_packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

# Создаем сессию Spark
spark = SparkSession.builder \
    .master("local") \
    .appName("Connect to kafka") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()
    
df = (spark.read
    .format('kafka')
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091')
    .option('kafka.security.protocol', 'SASL_SSL')
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512')
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";')
    .option('subscribe', 'persist_topic')
    .load())

# Выводим схему DataFrame для проверки
df.printSchema()