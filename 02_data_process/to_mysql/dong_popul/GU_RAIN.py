# gu_rain

from pyspark.sql.functions import split, substring, col
GU_RAIN=spark.read.option('header', 'true').csv('bike/mysql/GU_RAIN/GU_RAIN.csv')
df = GU_RAIN.select('*').where(substring(GU_RAIN.강우량계명, -2, 2) == '구청')
df=df.drop('강우량계명')
split_col=df.withColumn('split', split(df['자료수집 시각'], ' '))
split_col = split_col.withColumn('base_dt', col('split').getItem(0))
split_col = split_col.withColumn('base_tm', col('split').getItem(1))
df = split_col.drop('split')
df = df.drop('강우량계 코드', '자료수집 시각')

GU_RAIN = df.selectExpr("`10분우량` as rain_amt", "`구청 코드` as gu_cd", "base_dt", "base_tm", "`구청명` as gu_name")
GU_RAIN = GU_RAIN.sort('base_dt', 'base_tm')

GU_RAIN.write.mode("overwrite").format("jdbc")\
.option("driver", "com.mysql.cj.jdbc.Driver")\
.option("url", "jdbc:mysql://localhost:3306/mysql")\
.option("dbtable", "pjt2.GU_RAIN")\
.option("user", "root")\
.option("password", "1234")\
.save()