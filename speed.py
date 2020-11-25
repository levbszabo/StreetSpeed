from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark
import pandas as pd
spark = SparkSession.builder.master('local').getOrCreate()                                               
df_load = spark.read.csv('hdfs://babar.es.its.nyu.edu/user/ls5122/march2020.csv',header='true')
df_load.createOrReplaceTempView("speeds")
df = spark.sql("select concat(year,'-',month,'-',day,' ',hour,':00:00') as ds, concat(osm_start_node_id,'-',osm_end_node_id) as id,speed_mph_mean from speeds")
df = (df.repartition(spark.sparkContext.defaultParallelism,['id'])).cache() 
df = df.withColumn('speed_mph_mean',df['speed_mph_mean'].cast(DecimalType())) 

df = df.withColumn('ds',df['ds'].cast(DateType()))

df.write.parquet("speeds.parquet")