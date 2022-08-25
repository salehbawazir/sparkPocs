from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()


data = "D:/a/asl.txt"
aslrdd=spark.sparkContext.textFile(data)

res = aslrdd.filter(lambda x: "hyd" in x)

for i in res.collect():
    print(i)