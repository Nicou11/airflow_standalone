from pyspark.sql import SparkSession

import sys

APP_NAME = sys.argv[1]

df = SparkSession.builder.appName(APP_NAME).getOrCreate()

df1 = df.read.parquet('/home/young12/tmp/movdir')

df1.createOrReplaceTempView("movie")

director = df.sql("""
SELECT
    directorNm,
    count(directorNm) AS cnt
FROM movie
GROUP BY directorNm
""")


# 저장
director.write.mode('overwrite').parquet("/home/young12/data/movies/director")

