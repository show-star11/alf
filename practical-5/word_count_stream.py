from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("WordCount")
    .config(
        "spark.sql.streaming.checkpointLocation",
        "file:///D:/spark-checkpoints/wordcount"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

data = (
    spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 9999)
    .load()
)

words = data.select(
    explode(split(data.value, " ")).alias("word")
)

counts = words.groupBy("word").count()

query = (
    counts.writeStream
    .outputMode("complete")
    .format("console")
    .option(
        "checkpointLocation",
        "file:///D:/spark-checkpoints/wordcount"
    )
    .start()
)

query.awaitTermination()
