from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
data="D:/b/spark_with_python/data/us-500.csv"
df=spark.read.format("csv").option("header","True").option("inferSchema","True").load(data)

Q) When you want give offer to peoples from particular states ?
e) Sometimes as per your business requirement you need to create your own function(user defined function), so after creating that function in spark sql and spark dataframe

df.createOrReplaceTempView("tab")
#create ur own functions
def func(st):
    if(st=="NY"):
        return "30% off"
    elif(st=="CA"):
        return "40% off"
    elif(st=="OH"):
        return "50% off"
    else:
        return "500/- off"

#by default spark unable to understand python functions. so convert python/scala/java function to UDF (spark able to understand udfs)
uf = udf(func)
#=> converting user defined function into Spark understanding function

spark.udf.register("offer",uf) #user define function convert to sql function
#=> converting Spark understanding function into SparkSQL understanding function

ndf=spark.sql("select *, offer(state) todayoffers from tab")
#=> user defined function using with SparkSQL
ndf.show()

ndf1=df.withColumn("offer",uf(col("state")))
#=> user defined function using with Spark Dataframe
ndf1.show()
