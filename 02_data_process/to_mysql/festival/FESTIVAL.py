from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("yarn").appName("Festival").getOrCreate()

df = spark.read.option("multiline", "true").json("hdfs://localhost:9000/user/big/ProjectEng/festival_addCoord.json")

df_temp = df.select(explode(col('culture')).alias('temp'))

festival = df_temp.withColumn('tmp', explode(arrays_zip(col('temp.place'), col('temp.coord'))))\
    .select(col('tmp.place'), col('tmp.coord'), col('temp.period'), col('temp.time'))\
    .withColumn('from_dt', to_date(substring(split('period', '~').getItem(0), 1, 10), 'yyyy-MM-dd'))\
    .withColumn('to_dt', to_date(substring(split('period', '~').getItem(1), 2, 10), 'yyyy-MM-dd')).drop('period')\
    .withColumn('x_coord', col('coord').getItem(0))\
    .withColumn('y_coord', col('coord').getItem(1)).drop('coord')

# mysql에 데이터 넣기
festival.write.mode("overwrite").format("jdbc")\
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("url", "jdbc:mysql://localhost:3306/mysql")\
    .option("dbtable", "new_db.festival")\
    .option("user", "root")\
    .option("password", "great1234")\
    .save()