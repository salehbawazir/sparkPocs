from pyspark.sql import *
from pyspark.sql.functions import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
sc=spark.sparkContext

data="D:/b/spark_with_python/data/asl.csv"

ardd=sc.textFile(data)
res=ardd.filter(lambda x: "age" not in x).filter(lambda x :"hyd"in x)
# res=ardd.filter(lambda x: "dt" not in x).map(lambda x:x.split(","))


for i in res.collect():
    print(i)