메모장
기준일 / 시간대 / 동 아이디 / 생활인구 / 버스인구/ 지하철인구
from pyspark.sql.functions import lit, explode, col, arrays_zip, substring
import pandas as pd
bus_popul = spark.read.option("multiline", "true").json("hdfs://localhost:9000/user/big/bike/mysql/DONG_POPUL/bus_popul.json")
df = bus_popul.select(explode(col('buscountinfo')).alias('tmp'))
df = df.withColumn('DAY', df.tmp.row.CRTR_DT)
df = df.withColumn('00HH_CNT', df.tmp.row.BUS_PSGR_CNT_00HH)
df = df.withColumn('01HH_CNT', df.tmp.row.BUS_PSGR_CNT_01HH)
df = df.withColumn('02HH_CNT', df.tmp.row.BUS_PSGR_CNT_02HH)
df = df.withColumn('03HH_CNT', df.tmp.row.BUS_PSGR_CNT_03HH)
df = df.withColumn('04HH_CNT', df.tmp.row.BUS_PSGR_CNT_04HH)
df = df.withColumn('05HH_CNT', df.tmp.row.BUS_PSGR_CNT_05HH)
df = df.withColumn('06HH_CNT', df.tmp.row.BUS_PSGR_CNT_06HH)
df = df.withColumn('07HH_CNT', df.tmp.row.BUS_PSGR_CNT_07HH)
df = df.withColumn('08HH_CNT', df.tmp.row.BUS_PSGR_CNT_08HH)
df = df.withColumn('09HH_CNT', df.tmp.row.BUS_PSGR_CNT_09HH)
df = df.withColumn('10HH_CNT', df.tmp.row.BUS_PSGR_CNT_10HH)
df = df.withColumn('11HH_CNT', df.tmp.row.BUS_PSGR_CNT_11HH)
df = df.withColumn('12HH_CNT', df.tmp.row.BUS_PSGR_CNT_12HH)
df = df.withColumn('13HH_CNT', df.tmp.row.BUS_PSGR_CNT_13HH)
df = df.withColumn('14HH_CNT', df.tmp.row.BUS_PSGR_CNT_14HH)
df = df.withColumn('15HH_CNT', df.tmp.row.BUS_PSGR_CNT_15HH)
df = df.withColumn('16HH_CNT', df.tmp.row.BUS_PSGR_CNT_16HH)
df = df.withColumn('17HH_CNT', df.tmp.row.BUS_PSGR_CNT_17HH)
df = df.withColumn('18HH_CNT', df.tmp.row.BUS_PSGR_CNT_18HH)
df = df.withColumn('19HH_CNT', df.tmp.row.BUS_PSGR_CNT_19HH)
df = df.withColumn('20HH_CNT', df.tmp.row.BUS_PSGR_CNT_20HH)
df = df.withColumn('21HH_CNT', df.tmp.row.BUS_PSGR_CNT_21HH)
df = df.withColumn('22HH_CNT', df.tmp.row.BUS_PSGR_CNT_22HH)
df = df.withColumn('23HH_CNT', df.tmp.row.BUS_PSGR_CNT_23HH)
df = df.withColumn('DONG_ID', df.tmp.row.ADMDONG_ID)
df = df.drop('tmp')

bus_popul = df.withColumn("tmp", arrays_zip("DONG_ID", '00HH_CNT', '01HH_CNT', '02HH_CNT', '03HH_CNT', '04HH_CNT', '05HH_CNT',\
'06HH_CNT', '07HH_CNT', '08HH_CNT', '09HH_CNT', '10HH_CNT', '11HH_CNT', '12HH_CNT', '13HH_CNT', '14HH_CNT', '15HH_CNT', '16HH_CNT', '17HH_CNT',\
'18HH_CNT','19HH_CNT', '20HH_CNT', '21HH_CNT', '22HH_CNT', '23HH_CNT', 'DAY')).withColumn("tmp", explode("tmp")).select(col("tmp.DONG_ID"), \
col('tmp.00HH_CNT'), col('tmp.01HH_CNT'), col('tmp.02HH_CNT'), col('tmp.03HH_CNT'), col('tmp.04HH_CNT'), col('tmp.05HH_CNT'),\
col('tmp.06HH_CNT'), col('tmp.07HH_CNT'), col('tmp.08HH_CNT'), col('tmp.09HH_CNT'), col('tmp.10HH_CNT'), col('tmp.11HH_CNT'), col('tmp.12HH_CNT'), \
col('tmp.13HH_CNT'), col('tmp.14HH_CNT'), col('tmp.15HH_CNT'), col('tmp.16HH_CNT'), col('tmp.17HH_CNT'), col('tmp.18HH_CNT'),\
col('tmp.19HH_CNT'), col('tmp.20HH_CNT'), col('tmp.21HH_CNT'), col('tmp.22HH_CNT'), col('tmp.23HH_CNT'), col('tmp.DAY'))

temp = bus_popul.toPandas()

columns = ['00HH_CNT', '01HH_CNT', '02HH_CNT', '03HH_CNT', '04HH_CNT', '05HH_CNT',\
'06HH_CNT', '07HH_CNT', '08HH_CNT', '09HH_CNT', '10HH_CNT', '11HH_CNT', '12HH_CNT', '13HH_CNT', '14HH_CNT', '15HH_CNT', '16HH_CNT', '17HH_CNT',\
'18HH_CNT','19HH_CNT', '20HH_CNT', '21HH_CNT', '22HH_CNT', '23HH_CNT']


temp = pd.melt(temp, id_vars=["DONG_ID", "DAY"], value_vars=columns, var_name='base_tm',value_name='bus_popul')

bus_popul = spark.createDataFrame(temp)
bus_popul = bus_popul.selectExpr("DAY as base_dt", "DONG_ID as dong_cd", 'base_tm', 'bus_popul')
bus_popul = bus_popul.select('base_dt', substring('base_tm', 1, 2).alias('base_tm'), 'dong_cd', 'bus_popul')
bus_popul = bus_popul.select('*').where(col('base_dt') <= 20220800).sort('base_dt', 'base_tm', 'dong_cd')

subway_popul base_dt|base_tm|dong_cd|subway_popul
bus_popul base_dt|base_tm|dong_cd|bus_popul

spark.sql("""
	select *
	from subway_popul inner join bus_popul on(subway_popul.dong_cd = bus_popul.dong_cd)
""").show()

join_condition = subway_popul['dong_cd'] == bus_popul['dong_cd']
subway_popul.join(bus_popul, join_condition).show()

dong_popul = spark.sql("""
	select subway_popul.base_dt, subway_popul.base_tm, subway_popul.dong_cd, subway_popul.subway_popul, bus_popul.bus_popul
	from subway_popul inner join bus_popul
	where subway_popul.base_dt = bus_popul.base_dt and  subway_popul.base_tm = bus_popul.base_tm and subway_popul.dong_cd = bus_popul.dong_cd
""")

