df=spark.read.option('header', 'true').csv('GU_CODE.csv')
DONG_CODE = df.selectExpr("H_SDNG_CD as dong_cd", 'H_DNG_CD as dong_cd_8', 'H_DNG_NM as dong_nm', 'RESD_CD as gu_cd')
DONG_CODE.write.mode("overwrite").format("jdbc")\
.option("driver", "com.mysql.cj.jdbc.Driver")\
.option("url", "jdbc:mysql://localhost:3306/mysql")\
.option("dbtable", "pjt2.DONG_CODE")\
.option("user", "root")\
.option("password", "1234")\
.save()