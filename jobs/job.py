from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    get_json_object,
    when,
    lit,
    length,
    regexp_replace,
    last,
    sum,
    desc,
    countDistinct,
    max,
    first,
    udf,
    monotonically_increasing_id
)
from pyspark.sql.types import (
    StructType,
    StringType,
    TimestampType,
    BooleanType,
    DoubleType,
    IntegerType
)


def main():
    spark = SparkSession \
        .builder \
        .appName('spark_app') \
        .getOrCreate()

    clicks_data = extract_clickstream_data(spark)
    purchases_data = extract_purchase_data(spark)
    transformed_data = transform_data(clicks_data, purchases_data)
    transformed_data.show()
    transformed_data.createOrReplaceTempView('target')
    load_data(transformed_data)
    spark.stop()


def extract_clickstream_data(spark):
    clickstream_schema = StructType() \
        .add('userId', StringType(), False) \
        .add('eventId', StringType(), False) \
        .add('eventType', StringType(), False) \
        .add('eventTime', TimestampType(), False) \
        .add('attributes', StringType(), True)

    clickstream_data = spark.read.format('csv') \
        .options(header='True') \
        .schema(clickstream_schema) \
        .load('../data/mobile_app_clickstream_0.csv.gz')

    return clickstream_data


def extract_purchase_data(spark):
    purchase_schema = StructType() \
        .add('purchaseId', StringType(), False) \
        .add('purchaseTime', TimestampType(), False) \
        .add('billingCost', DoubleType(), True) \
        .add('isConfirmed', BooleanType(), True)

    purchase_data = spark.read.format('csv') \
        .options(header='True') \
        .schema(purchase_schema) \
        .load('../data/user_purchases/*.csv.gz')

    return purchase_data


def transform_data(clickstream_data, purchase_data):  # Task 1.1
    win1 = Window.partitionBy('userId').orderBy('eventTime')
    win2 = Window.orderBy('sessionId')

    clickstream_data = clickstream_data \
        .withColumn('appOpenFlag', when((col('eventType') == 'app_open'), monotonically_increasing_id()).otherwise(0)) \
        .withColumn('sessionId', sum(col('appOpenFlag')).over(win1)) \
        .withColumn('attrs', when(col('eventType') == 'purchase',
                                  clickstream_data['attributes'].substr(
                                      lit(2), length('attributes') - lit(2))).otherwise(col('attributes'))) \
        .withColumn('attr', when(col('eventType') == 'purchase',
                                 regexp_replace(col('attrs'), '""', "'")).otherwise(col('attrs'))) \
        .withColumn('campaign_id', when(get_json_object('attr', '$.campaign_id').isNotNull(),
                                        get_json_object('attr', '$.campaign_id')).otherwise(None)) \
        .withColumn('channel_id', when(get_json_object('attr', '$.channel_id').isNotNull(),
                                       get_json_object('attr', '$.channel_id')).otherwise(None)) \
        .withColumn('purchase_id', when(get_json_object('attr', '$.purchase_id').isNotNull(),
                                        get_json_object('attr', '$.purchase_id')).otherwise(None)) \
        .withColumn('campaignId',
                    last(col('campaign_id'), ignorenulls=True).over(win2.rowsBetween(Window.unboundedPreceding, 0))) \
        .withColumn('channelId',
                    last(col('channel_id'), ignorenulls=True).over(win2.rowsBetween(Window.unboundedPreceding, 0)))
    target_df = clickstream_data.join(purchase_data, clickstream_data['purchase_id'] == purchase_data['purchaseId'],
                                      'left')

    target_df = target_df.select(col('purchaseId'), col('purchaseTime'), col('billingCost'), col('isConfirmed'),
                                 col('sessionId'), col('campaignId'), col('channelId'))
    return target_df


def load_data(df):
    df.write.mode('overwrite').parquet('../output/task1.1/target_schema.parquet')


# Task 2.1


def task_21_sql(spark):
    task21_sql = spark.sql('select distinct campaignId, sum(billingCost) as revenue from target '
                           'where isConfirmed = true '
                           'group by campaignId '
                           'order by revenue desc '
                           'limit 10')
    task21_sql.write.mode('overwrite').parquet('../output/task2.1/plain_sql.parquet')
    return task21_sql


def task_21_df_api(df):
    task21_df = df.where('isConfirmed = true') \
        .groupBy('campaignId') \
        .agg(sum('billingCost').alias('revenue')) \
        .orderBy(desc('revenue')) \
        .limit(10) \
        .select('campaignId', 'revenue')
    task21_df.write.mode('overwrite').parquet('../output/task2.1/df_api.parquet')
    return task21_df

# Task 2.2


def task_22_sql(spark):
    task22_sql = spark.sql('select campaignId, first(channelId) as channelId, max(sessionCount) as maxSessions '
                           'from (select campaignId, channelId, count(distinct sessionId) as sessionCount '
                           'from target '
                           'group by campaignId, channelId '
                           'order by campaignId, sessionCount desc) '
                           'group by campaignId '
                           'order by maxSessions desc')
    task22_sql.write.mode('overwrite').parquet('../output/task2.2/plain_sql.parquet')
    return task22_sql


def task_22_df_api(df):
    task22_df = df.groupBy('campaignId', 'channelId') \
        .agg(countDistinct('sessionId').alias('sessionCount')) \
        .orderBy('campaignId', desc('sessionCount')) \
        .select('campaignId', 'channelId', 'sessionCount')

    task22_df = task22_df.groupBy('campaignId') \
        .agg(max('sessionCount').alias('maxSessions'), first('channelId').alias('channelId')) \
        .orderBy(desc('maxSessions')) \
        .select('campaignId', 'channelId', 'maxSessions')
    task22_df.write.mode('overwrite').parquet('../output/task2.2/df_api.parquet')
    return task22_df


if __name__ == '__main__':
    main()
