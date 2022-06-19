import unittest
import events as e
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType
from pyspark.shell import spark


class MyTestCase(unittest.TestCase):

    def test_something(self):
        events = e.Events()

        schema = StructType([ \
            StructField("time", StringType(), True), \
            StructField("action", StringType(), True)
        ])

        df1 = events.read_data(filename='events.csv', schema=schema).orderBy('time')
        df = df1.select(concat(substring(trim(col('time')), 1, 10), lit(' '), substring(trim(col('time')), 12, 8)).cast(
            TimestampType()).alias('timestamp'), \
                        col('action').alias('action')).orderBy(col('timestamp'))
        df_new_cols = events.new_columns(df=df)
        df_agg_10_mins = events.agg_10_mins(df=df_new_cols)
        df_top10_mins = events.top10_mins(df=df_agg_10_mins)

        result = df_top10_mins.orderBy(col('Open').desc()).limit(1).select('Open').collect()[0][0]

        self.assertEqual(result, 185)  # add assertion here


if __name__ == '__main__':
    unittest.main()
