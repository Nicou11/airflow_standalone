from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, col, size
import sys

APP_NAME = sys.argv[1]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

jdf = spark.read.option("multiline","true").json('/home/young12/data/json')

edf = jdf.withColumn("company", explode_outer("companys"))

fdf = edf.withColumn("director", explode_outer("directors"))

sdf = fdf.withColumn("directorNm", col("director.peopleNm"))

rdf = sdf.select("movieCd", "movieNm", "genreAlt", "typeNm", "directorNm", "company.companyCd", "company.companyNm")

# 저장
rdf.write.mode("overwrite").parquet('/home/young12/tmp/movdir')


spark.stop()
