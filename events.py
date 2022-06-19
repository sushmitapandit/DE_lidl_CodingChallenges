from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.sql.window import *
import events


class Events:

    # def __init__(self, filepath):
    #     self.filepath = filepath

    def read_data(self, filename, schema):
        """
        :param filename: filename like events.csv
        :param schema: pass the schema definition for the dataframe
        :return:
        """
        df = spark.read.format('csv').load(f'{filename}', header=True, schema=schema, date_format='yyyy-MM-dd')
        return df

    def new_columns(self, df):
        """
        :param df: output dataframe from the read_data method
        :return: dataframe with additional columns like hours, minutes, minutes_rounded
        """
        df_new_cols = df.withColumn('date', to_date(col('timestamp'))) \
            .withColumn('hours', hour(col('timestamp'))) \
            .withColumn('minutes', minute(col('timestamp'))) \
            .withColumn('minutes_rounded', ceil(round(minute(col('timestamp')) / 10, 4)) * 10).orderBy(col('hours'))

        return df_new_cols

    def agg_10_mins(self, df):
        """
        :param df: output dataframe from function new_columns
        :return: data aggregated by 10 minutes
        """
        df_agg_10_mins = df.groupBy('date', 'hours', 'minutes', 'minutes_rounded', 'action') \
            .agg(count(col('hours')).alias('counts')).orderBy('hours', 'minutes_rounded') \
            .groupBy('date', 'hours', 'minutes_rounded', 'action') \
            .agg(sum(col('counts')).alias('counts')).orderBy('hours', 'minutes_rounded') \
            .withColumn('Open', when(col('action') == lit('Open'), col('counts')).otherwise(lit(0))) \
            .withColumn('Close', when(col('action') == lit('Close'), col('counts')).otherwise(lit(0))) \
            .groupBy('date', 'hours', col('minutes_rounded').alias('Every10Minutes')) \
            .agg(sum(col('Open')).alias('Open'), sum(col('Close')).alias('Close'))
        return df_agg_10_mins

    def top10_mins(self, df):
        """
        :param df: output dataframe from agg_10_mins
        :return: top 10 rows by max open times
        """
        df_top10_mins = df.groupBy('date', 'hours', 'Every10Minutes') \
            .agg(max(col('Open')).alias('Open')) \
            .orderBy(col('Open').desc()).limit(10)

        return df_top10_mins

    def agg_per_min_per10min(self, df):
        """
        :param df: output dataframe from new_columns method
        :return: data aggreated per minute,per 10 minutes
        """
        df_agg_Per_mins = df.groupBy(col('date'), 'hours', 'minutes', 'minutes_rounded', 'action') \
            .agg(count(col('hours')).alias('Count')) \
            .withColumn('Open', when(col('action') == lit('Open'), col('Count')).otherwise(lit(0))) \
            .withColumn('Close', when(col('action') == lit('Close'), col('Count')).otherwise(lit(0))) \
            .groupBy('date', 'hours', 'minutes', col('minutes_rounded').alias('Every10Minutes')) \
            .agg(sum(col('Open')).alias('Open'), sum(col('Close')).alias('Close'))
        return df_agg_Per_mins

    def avg_perminute(self, df):
        """
        :param df: pass output dataframe from agg_permin_per10min function
        :return: aggregated dataframe per minute
        """
        df_agg_avg_per_10mins = df.groupBy('date', 'hours', 'Every10Minutes') \
            .agg(avg(col('Open')).cast(DecimalType(18, 2)).alias('avg_open'), \
                 avg(col('Close')).cast(DecimalType(18, 2)).alias('avg_close'))
        return df_agg_avg_per_10mins
