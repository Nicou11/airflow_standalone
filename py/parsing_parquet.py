from pyspark.sql import SparkSession
import sys

APP_NAME = sys.argv[1]

spark = SparkSession.builder.appName(APP_NAME).getOrCreate()

#######################
# pyspark 에서 multiline(배열) 구조 데이터 읽기
jdf = spark.read.option("multiline","true").json('/home/young12/tmp/mvstar/data/movies/year=2015/data.json')

## companys, directors 값이 다중으로 들어가 있는 경우 찾기 위해 count 컬럼 추가
from pyspark.sql.functions import explode, col, size
ccdf = jdf.withColumn("company_count", size("companys")).withColumn("directors_count", size("directors"))

# 펼치기
from pyspark.sql.functions import explode_outer, col, size
edf = fdf.withColumn("company", explode_outer("companys"))

# 또 펼치기
eedf = edf.withColumn("director", explode_outer("directors"))
#######################

# 저장
eedf.write.mode("overwrite").parquet('/home/young12/tmp/movdir')


spark.stop()
