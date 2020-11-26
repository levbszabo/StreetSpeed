from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import current_date
from pyspark.sql.functions import pandas_udf, PandasUDFType
from fbprophet import Prophet
import pyspark
import pandas as pd
spark = SparkSession.builder.master('local').getOrCreate()   

spark.conf.set("spark.sql.execution.arrow.enabled", "true")
df = spark.read.parquet("speeds.parquet")
result_schema =StructType([
  StructField('ds',TimestampType()),
  StructField('id',StringType()),
  StructField('y',DoubleType()),
  StructField('yhat',DoubleType()),
  StructField('yhat_upper',DoubleType()),
  StructField('yhat_lower',DoubleType())
  ])

sdf = df.withColumn('y',df['speed_mph_mean'].cast(DoubleType()))
sdf = sdf[['ds','id','y']]
sdf.createOrReplaceTempView("speeds")
small_df = spark.sql("select speeds.ds, speeds.y, speeds.id from speeds where speeds.id in (select distinct speeds.id from speeds limit 100)")

@pandas_udf( result_schema, PandasUDFType.GROUPED_MAP )
def forecast_speed( sdf ):
    model = Prophet(interval_width=0.95, daily_seasonality=True, weekly_seasonality=True, yearly_seasonality=True)
    model.fit( sdf )
    forecast_pd = model.predict(sdf)

results = (small_df
    .groupBy('id')
    .apply(forecast_speed)
    .withColumn('training_date', current_date()
    )
    )

