from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark
import pandas as pd
import numpy as np
#from pyspark.sql.functions import pandas_udf, PandasUDFType
from fbprophet import Prophet
spark = SparkSession.builder.master('local').config("spark.driver.memory", "10g").appName('speed-analysis').getOrCreate() 
spark.conf.set("spark.sql.execution.arrow.enabled", "true")

#from pyspark.sql.functions import pandas_udf, PandasUDFType
df =spark.read.csv('hdfs://babar.es.its.nyu.edu/user/yz7413/project/Uber_data/Uber_timeseries_2019.txt')
df  = df.withColumn("end_id",split("_c1", "\t").getItem(0))
df = df.withColumn("_c1",split("_c1","\t").getItem(1))
df = df.withColumn("start_id",df["_c0"])
df = df.withColumn("id",concat("start_id",lit("-"),"end_id")) 
#df.createOrReplaceTempView("speeds")
df.repartition(spark.sparkContext.defaultParallelism,['id'])).cache()

#df =spark.read.parquet('hdfs://babar.es.its.nyu.edu/user/ls5122/speed_slice1.parquet')
#df_load.createOrReplaceTempView("speeds")

ds = pd.date_range('2019-01-01', '2020-01-01', freq='1H', closed='left',tz='US/Eastern')  
ds = np.array([ds[i].replace(tzinfo=None) for i in range(len(ds))])
#tz_aware = tz_naive.tz_localize(tz='US/Eastern')
                          
result_schema =StructType([
  StructField('ds',TimestampType()),
  StructField('id',StringType()),
  StructField('y',DoubleType()),
  StructField('yhat',DoubleType()),
  StructField('yhat_upper',DoubleType()),
  StructField('yhat_lower',DoubleType())
  ])      
df.createOrReplaceTempView("speeds")
df = (spark.sql("select * from speeds limit 10").repartition(spark.sparkContext.defaultParallelism,['id'])).cache()

@pandas_udf( result_schema, PandasUDFType.GROUPED_MAP )
def forecast_speed( pdf ):
  #some additional processing to create the training df in correct format
    y = np.array(pdf[pdf.columns[1:-3]].values,np.double)[0]
    street_id = np.array([pdf["id"].values[0] for _ in range(len(y))])
    train_df = pd.DataFrame(data = [ds,y,street_id]).T
    train_df.columns = ["ds","y","id"]
    model = Prophet(interval_width=0.95, weekly_seasonality=True, yearly_seasonality=True) 
    model.fit(train_df)
    future_pd = model.make_future_dataframe(periods=365, freq='h')
    forecast_pd = model.predict( future_pd )  
    f_pd = forecast_pd[ ['ds','yhat', 'yhat_upper', 'yhat_lower'] ].set_index('ds')
    in_pd = train_df[['ds','id','y']].set_index(['ds'])
    results_pd = f_pd.join(in_pd, how='left' )
    results_pd.reset_index(level=0, inplace=True)
    results_pd = results_pd.fillna(method='ffill')
    return results_pd[ ['ds', 'id','y', 'yhat', 'yhat_upper', 'yhat_lower']]   

results = (df.groupBy('id').apply(forecast_speed).withColumn('training_date', current_date()))

results.write.parquet("hdfs://babar.es.its.nyu.edu/user/yz7413/project/prediction_baseline_10.parquet")
