# import findspark
# findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HelloWorld") \
    .master("local[*]") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

df = spark.createDataFrame([("Hello, world!",)], ["text"])
df.show()
spark.stop()