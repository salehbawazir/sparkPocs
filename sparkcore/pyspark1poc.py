from pyspark.sql import *
from pyspark.sql.functions import *

from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

win = Window.partitionBy(col("state")).orderBy(col("zip"))

data="D:/b/spark_with_python/data/us-500.csv"

df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)

df.show()



