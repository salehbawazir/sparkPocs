from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext

data="D:/b/spark_with_python/data/donations.csv"

ardd=sc.textFile(data)
res=ardd.filter(lambda x: "dt" not in x).map(lambda x:x.split(",")) #.map(lambda x:(x[0],1))
# res=ardd.filter(lambda x: "dt" not in x).map(lambda x:x.split(","))


for i in res.collect():
    print(i)