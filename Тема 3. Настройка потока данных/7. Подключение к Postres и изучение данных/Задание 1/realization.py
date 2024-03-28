from pyspark.sql import SparkSession

spark = (
        SparkSession.builder.appName("Read PostgreSQL Data")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0")
        .getOrCreate()
    )

jdbc_url = "jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de"
connection_properties = {
    "user": "student",
    "password": "de-student",
    "driver": "org.postgresql.Driver"
}


df = spark.read \
        .jdbc(url=jdbc_url,
              table="public.marketing_companies",
              properties=connection_properties)
df.count()