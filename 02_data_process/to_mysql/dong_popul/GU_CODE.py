

df=spark.read.option('header', 'true').csv('GU_CODE.csv')

GU_CODE =df.selectExpr("RESD_CD as gu_cd", "CT_NM as gu_nm").distinct()

GU_CODE.write.mode("overwrite").format("jdbc")\
.option("driver", "com.mysql.cj.jdbc.Driver")\
.option("url", "jdbc:mysql://localhost:3306/mysql")\
.option("dbtable", "pjt2.GU_CODE")\
.option("user", "root")\
.option("password", "1234")\
.save()








