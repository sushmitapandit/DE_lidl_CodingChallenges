from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.window import *
import events


events = events.Events()

# defining the schema
schema = StructType([ \
    StructField("time", StringType(), True),\
    StructField("action", StringType(), True)
  ])

# creating the dataframe from reading the csv file events
df1 = events.read_data(filename='events.csv', schema=schema).orderBy('time')

#data cleaning
df = df1.select(concat(substring(trim(col('time')), 1, 10), lit(' '), substring(trim(col('time')), 12, 8))\
                .cast(TimestampType()).alias('timestamp'),\
                col('action').alias('action'))\
                .orderBy(col('timestamp'))

df.show(truncate=False)

# total no of rows present in the csv
print('Count of Rows : ', df.count())

# generate new columns
df_new_cols = events.new_columns(df=df)
df_new_cols.filter(col('date')=='2016-07-26').orderBy('hours', 'minutes').show(truncate=False)

"""
Task 1: 
a.	reduce data temporal granularity to 10 minutes, so that there is only one single row for each 10 minutes
b.	Over this temporal aggregation count how many actions of each type there is per minute

"""
df_agg_10_mins = events.agg_10_mins(df=df_new_cols)
df_agg_10_mins.filter(col('date')=='2016-07-26').orderBy('date', 'hours', 'Every10Minutes').show(truncate=False)

"""
Task 2 : Over this temporal aggregation count how many actions of each type there is per minute.
"""
df_agg_Per_mins = events.agg_per_min_per10min(df_new_cols)
df_agg_Per_mins.filter(col('date')=='2016-07-26').orderBy('hours', 'minutes') .show(truncate=False)


"""
Task 3:
After previous calculation, please compute the average number of actions each 10 minutes.
"""
#avg per10_minutes
df_agg_avg_per_10mins = events.avg_perminute(df_agg_Per_mins)
df_agg_avg_per_10mins.filter(col('date')=='2016-07-26').orderBy('hours', 'Every10Minutes') .show(truncate=False)


"""
Task 4:
Finally, we would like you to compute the top 10 minutes with a bigger amount of "open" action. 
"""
# this gives top 10 rows by maximum open action per 10 minutes
df_top10_mins = events.top10_mins(df=df_agg_10_mins)
df_top10_mins.show(truncate=False)

spark.stop()
