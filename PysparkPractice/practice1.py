from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext

data="D:/b/spark_with_python/data/donations.csv"

ardd=sc.textFile(data)


