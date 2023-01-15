from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.master("yarn").appName("BIKE_USAGE").getOrCreate()

# 데이터 가공
# hdfs에서 데이터 불러오기
df = spark.read.option('header', 'true').csv('BikeUsage/BIKE_USAGE.csv')

# 대여, 일자시간 가공 및 필요없는 컬럼 제거
df  = df.withColumn(('대여일자'), split(col("`대여일시`"), ' ')[0]).withColumn(('대여시간'), split(col("`대여일시`"), ' ')[1]).withColumn(('반납일자'), split(col("`반납일시`"), ' ')[0]).withColumn(('반납시간'), split(col("`반납일시`"), ' ')[1]).drop('이용시간', '이용거리', '자전거번호', '대여일시', '반납일시')

# 대여관련 데이터 프레임 생성( 대여 대여소번호, 대여일자, 대여시간 컬럼 사용)
df_rent2 = df.select(col('대여 대여소번호').alias('station_id'), col('대여일자').alias('use_dt'), substring(col('대여시간'), 1, 2).alias('use_tm'))

# station_id, use_dt, use_tm으로 묶고 count처리 및 정렬
df_rent2 = df_rent2.groupBy(col('station_id'), col('use_dt'), col('use_tm')).count().withColumnRenamed('count', 'rent_amt').sort(col('station_id'), col('use_dt'), col('use_tm'))

# 반납관련 데이터 프레임 생성( 반납대여소번호, 반납일자, 반납시간 컬럼 사용)
df_return2 = df.select(col('반납대여소번호').alias('station_id'), col('반납일자').alias('use_dt'), substring(col('반납시간'), 1, 2).alias('use_tm'))

# station_id, use_dt, use_tm으로 묶고 count 처리 및 정렬
df_return2 = df_return2.groupBy(col('station_id'), col('use_dt'), col('use_tm')).count().withColumnRenamed('count', 'return_amt').sort(col('station_id'), col('use_dt'), col('use_tm'))

#  대여, 반납 join후 select으로 필요한 컬럼 가져오기 (대여, 반납 둘 다 있는 데이터만 가져오기)
df_usage = df_rent2.join(df_return2, (df_return2.station_id == df_rent2.station_id) & (df_rent2.use_dt == df_return2.use_dt) & (df_rent2.use_tm == df_return2.use_tm)).select(df_rent2['*'], df_return2['return_amt'])

# 정렬
df_usage = df_usage.sort('use_dt', 'use_tm', 'station_id')

# 대여 anti, 새로운 행(returnt_am) 추가 (0으로)
df_rent_anti = df_rent2.join(df_return2, (df_return2.station_id == df_rent2.station_id) & (df_rent2.use_dt == df_return2.use_dt) & (df_rent2.use_tm == df_return2.use_tm), how='leftanti').withColumn('returnt_amt', lit(0))

# 대여 anti 정렬
df_rent_anti = df_rent_anti.sort('use_dt', 'use_tm', 'station_id')
df_rent_anti = df_rent_anti.na.drop()

# 반납 anti, 새로운 행(rent_am) 추가 (0으로)
df_return_anti = df_return2.join(df_rent2, (df_return2.station_id == df_rent2.station_id) & (df_rent2.use_dt == df_return2.use_dt) & (df_rent2.use_tm == df_return2.use_tm), how='leftanti').withColumn('rent_amt', lit(0))

# 반납 anti 정렬
df_return_anti = df_return_anti.sort('use_dt', 'use_tm', 'station_id')
df_return_anti = df_return_anti.select(col('station_id'), col('use_dt'), col('use_tm'), col('rent_amt'), col('return_amt'))
df_return_anti = df_return_anti.na.drop()

# 대여, 반납 join 데이터, 대여 anti 데이터, 반납 anti 데이터 union
df_union1 = df_usage.union(df_rent_anti)
df_union2 = df_union1.union(df_return_anti)

# 정렬
df_union2 = df_union2.sort('use_dt', 'use_tm', 'station_id')

# row_id 추가
df_union2 = df_union2.withColumn('row_id', monotonically_increasing_id())

# BIKE_USAGE 으로 저장
BIKE_USAGE = df_union2.select(col('row_id'), col('station_id'), col('use_dt'), col('use_tm'), col('rent_amt'), col('return_amt'))

# station_id 통일 작업 (ex 00103 -> 103)
BIKE_USAGE = BIKE_USAGE.select(col('row_id'), col('station_id').cast('int'), col('use_dt'), col('use_tm'), col('rent_amt'), col('return_amt'))
BIKE_USAGE =  BIKE_USAGE.select(col('row_id'), col('station_id').cast('string'), col('use_dt'), col('use_tm'), col('rent_amt'), col('return_amt'))

# mysql에 데이터 넣기
BIKE_USAGE.write.mode("overwrite").format("jdbc")\
    .option("driver", "com.mysql.cj.jdbc.Driver")\
    .option("url", "jdbc:mysql://localhost:3306/mysql")\
    .option("dbtable", "pjt2.BIKE_USAGE")\
    .option("user", "root")\
    .option("password", "1234")\
    .save()
