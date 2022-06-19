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
df = df1.select(concat(substring(trim(col('time')),1,10),lit(' '),substring(trim(col('time')),12,8)).cast(TimestampType()).alias('timestamp'), \
                col('action').alias('action'))\
                .orderBy(col('timestamp'))

df.show(truncate=False)

# total no of rows present in the csv
print('Count of Rows : ', df.count())

df_new_cols = events.new_columns(df=df)
df_agg_10_mins = events.agg_10_mins(df=df_new_cols)
df_top10_mins = events.top10_mins(df=df_agg_10_mins)
df_top10_mins.show(truncate=False)


df_new_cols = df.withColumn('date', to_date(col('timestamp'))) \
                .withColumn('hours', hour(col('timestamp'))) \
                .withColumn('minutes', minute(col('timestamp')))\
                .withColumn('minutes_rounded', ceil(round(minute(col('timestamp'))/10, 4)) * 10).orderBy(col('hours'))

df_new_cols.filter(col('date')=='2016-07-26').orderBy('hours','minutes').show(truncate=False)

# count per mins per 10 minutes
df_agg_Per_mins = df_new_cols.groupBy(col('date'),'hours','minutes','minutes_rounded','action')\
                             .agg(count(col('hours')).alias('Count'))\
                             .withColumn('Open', when(col('action') == lit('Open'), col('Count')).otherwise(lit(0))) \
                             .withColumn('Close', when(col('action') == lit('Close'), col('Count')).otherwise(lit(0))) \
                             .groupBy('date', 'hours', 'minutes',  col('minutes_rounded').alias('Every10Minutes'))\
                             .agg(sum(col('Open')).alias('Open'), sum(col('Close')).alias('Close'))


df_agg_Per_mins.filter(col('date')=='2016-07-26').orderBy('hours','minutes') .show(truncate=False)

#avg per10_minutes
df_agg_avg_per_10mins = df_agg_Per_mins.groupBy('date','hours','Every10Minutes')\
                                       .agg(avg(col('Open')).cast(DecimalType(18,2)).alias('avg_open'),\
                                        avg(col('Close')).cast(DecimalType(18,2)).alias('avg_close'))


df_agg_avg_per_10mins.filter(col('date')=='2016-07-26').orderBy('hours','Every10Minutes') .show(truncate=False)

df_agg_10_mins = df_new_cols.groupBy('date', 'hours', 'minutes','minutes_rounded','action')\
                            .agg(count(col('hours')).alias('counts')).orderBy('hours','minutes_rounded')\
                            .groupBy('date', 'hours','minutes_rounded','action') \
                            .agg(sum(col('counts')).alias('counts')).orderBy('hours', 'minutes_rounded') \
                            .withColumn('Open', when(col('action') == lit('Open'), col('counts')).otherwise(lit(0))) \
                            .withColumn('Close', when(col('action') == lit('Close'), col('counts')).otherwise(lit(0))) \
                            .groupBy('date', 'hours', col('minutes_rounded').alias('Every10Minutes'))\
                            .agg(sum(col('Open')).alias('Open'), sum(col('Close')).alias('Close'))

# this gives us results every 10 minutes
df_agg_10_mins.filter(col('date')=='2016-07-26').orderBy('date', 'hours', 'Every10Minutes').show(truncate=False)


#Top 10 minutes
df_top10_mins = df_agg_10_mins.groupBy('date', 'hours', 'Every10Minutes')\
    .agg(max(col('Open')).alias('Open'))\
    .orderBy(col('Open').desc()).limit(10)\

df_top10_mins.show(truncate=False)

spark.stop()
