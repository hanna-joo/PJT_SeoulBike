from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("yarn").appName("TOUR_PLACE").getOrCreate()

# 데이터 가공
# hdfs에서 데이터 불러오기
df = spark.read.option('header', 'true').csv('tourplace_star.csv')

# null 값 0으로 바꿔주기
df = df.fillna('0')

# 장소명 중복제거
df = df.drop_duplicates(['Name'])

# _c0 제거 후 row_id 추가
df = df.withColumn('row_id', monotonically_increasing_id()).drop(col('_c0'))

# 정렬, 컬럼명 변경하기
df = df.select(col('row_id').alias('place_id'), col('Name').alias('place_nm'), col('Point').alias('place_star'))

# 최종(erd에 맞춰서 데이터 형식 변환)
TOUR_PLACE = df.select(col('place_id').cast('int'), col('place_nm').cast('varchar(50)'), col('place_star').cast('float'))

# mysql에 데이터 넣기
TOUR_PLACE .write.mode("overwrite").format("jdbc")\
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("url", "jdbc:mysql://localhost:3306/mysql")\
    .option("dbtable", "pjt2.TOUR_PLACE")\
    .option("user", "root")\
    .option("password", "1234")\
    .save()
