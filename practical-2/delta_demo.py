import os
import sys

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Tell Spark to use the same Python that runs this script
# (on Windows, Spark looks for "python3" by default, which does not exist)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Start Spark
builder = (
    SparkSession.builder
    .appName("DeltaLakeDemo")
    .master("local[*]")
    .config(
        "spark.sql.extensions",
        "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

print("Spark started successfully!")

# Create sample data
data = [
    (1, "Siddhant", 90),
    (2, "Shreya", 88),
    (3, "Shruti", 75),
    (4, "Chinmay", 90),
    (5, "Vishnu", 88),
    (6, "Nikita", 75)
]

# Create DataFrame
df = spark.createDataFrame(
    data,
    ["id", "name", "marks"]
)

# Display student data
print("Student Data:")
df.show()

# Stop Spark
spark.stop()
