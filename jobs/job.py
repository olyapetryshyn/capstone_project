from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    get_json_object,
    when,
    concat,
    lit,
    length,
    regexp_replace,
    dense_rank,
    row_number,
    last,
    sum,
    desc,
    countDistinct,
    max,
    first
)
from pyspark.sql.types import (
    StructType,
    StringType,
    TimestampType,
    BooleanType,
    DoubleType
)

spark = SparkSession \
    .builder \
    .appName('spark_app') \
    .getOrCreate()

clickstream_schema = StructType() \
    .add('userId', StringType(), False) \
    .add('eventId', StringType(), False) \
    .add('eventType', StringType(), False) \
    .add('eventTime', TimestampType(), False) \
    .add('attributes', StringType(), True)

purchase_schema = StructType() \
    .add('purchaseId', StringType(), False) \
    .add('purchaseTime', TimestampType(), False) \
    .add('billingCost', DoubleType(), True) \
    .add('isConfirmed', BooleanType(), True)

clickstream_data = spark.read.format('csv') \
    .options(header='True') \
    .schema(clickstream_schema) \
    .load('../data/mobile_app_clickstream_0.csv.gz')

purchase_data = spark.read.format('csv') \
    .options(header='True') \
    .schema(purchase_schema) \
    .load('../data/user_purchases_0.csv.gz')

win1 = Window.partitionBy(col('userId')).orderBy(col('eventTime'))
win2 = Window.orderBy(col('sessionTs'))
win3 = Window.orderBy('sessionId')

clickstream_data = clickstream_data \
    .withColumn('firstTs', when(row_number().over(win1) == 1, col('eventTime'))) \
    .withColumn('sessionTs',
                last(col('firstTs'), ignorenulls=True).over(win1.rowsBetween(Window.unboundedPreceding, 0))) \
    .withColumn('sessionId', concat(lit('session'), dense_rank().over(win2))) \
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
                last(col('campaign_id'), ignorenulls=True).over(win3.rowsBetween(Window.unboundedPreceding, 0))) \
    .withColumn('channelId',
                last(col('channel_id'), ignorenulls=True).over(win3.rowsBetween(Window.unboundedPreceding, 0)))

target_df = clickstream_data.join(purchase_data, clickstream_data['purchase_id'] == purchase_data['purchaseId'], 'left')

target_df = target_df.select(col('purchaseId'), col('purchaseTime'), col('billingCost'), col('isConfirmed'),
                             col('sessionId'), col('campaignId'), col('channelId'))
target_df.write.mode('overwrite').parquet('../output/result.parquet')

# Task 2.1

target_df.createOrReplaceTempView('target')

spark.sql('select distinct campaignId, sum(billingCost) as revenue from target '
          'where isConfirmed = true '
          'group by campaignId '
          'order by revenue desc '
          'limit 10').show()

target_df.where('isConfirmed = true') \
    .groupBy('campaignId') \
    .agg(sum('billingCost').alias('revenue')) \
    .orderBy(desc('revenue')) \
    .limit(10) \
    .select('campaignId', 'revenue') \
    .show()

# Task 2.2

spark.sql('select campaignId, first(channelId) as channelId, max(sessionCount) as maxSessions '
          'from (select campaignId, channelId, count(distinct sessionId) as sessionCount '
          'from target '
          'group by campaignId, channelId '
          'order by campaignId, sessionCount desc) '
          'group by campaignId '
          'order by maxSessions desc').show()

task22_df = target_df.groupBy('campaignId', 'channelId') \
    .agg(countDistinct('sessionId').alias('sessionCount')) \
    .orderBy('campaignId', desc('sessionCount')) \
    .select('campaignId', 'channelId', 'sessionCount')

task22_df.groupBy('campaignId') \
    .agg(max('sessionCount').alias('maxSessions'), first('channelId').alias('channelId')) \
    .orderBy(desc('maxSessions')) \
    .select('campaignId', 'channelId', 'maxSessions').show()
