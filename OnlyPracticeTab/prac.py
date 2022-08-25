from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("test").config("spark.sql.session.timezone","EST").getOrCreate()

data="D:/b/spark_with_python/data/donations.csv"
df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)


def daystoyrmndays(nums):
    yrs = int(nums / 365)
    mon = int((nums % 365) / 30)
# if we have 395 days so num%365=30 and 30/30 = 1 month i.e 1 year 1 month (395 days)
    days = int((nums % 365) % 30)
# if we have 400 days 400%365=35 , 35%30= 5 days
    result = "{}-years {}-months {}-days".format(yrs,mon,days)
    return result

udffunc = udf(daystoyrmndays)

ndf=df.withColumn("dt",to_date(col("dt"),"d-M-yyyy"))\
.withColumn("today",current_date())\
    .withColumn("dtdiff",datediff(col("today"),col("dt")))\
    .withColumn("dtdiff1",udffunc(col("dtdiff")))
ndf.show(20,truncate=False)

# #tasks2: every month 15th what day you will get? (sun?or mon)
# \
#     .withColumn("dttrunc",date_trunc("month",col("dt"))\
#                 .withColumn("dtadd",date_add(col("dttrunc"),15))\
#                 .withColumn("15thDay",date_format(col("dtadd"),"EEE"))