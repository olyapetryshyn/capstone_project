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
    last
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
    .appName("spark_app") \
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
    .withColumn('campaignId', when(get_json_object('attr', '$.campaign_id').isNotNull(),
                                   get_json_object('attr', '$.campaign_id')).otherwise(None)) \
    .withColumn('channelId', when(get_json_object('attr', '$.channel_id').isNotNull(),
                                  get_json_object('attr', '$.channel_id')).otherwise(None)) \
    .withColumn('purchase_id', when(get_json_object('attr', '$.purchase_id').isNotNull(),
                                    get_json_object('attr', '$.purchase_id')).otherwise(None))

target_df = clickstream_data.join(purchase_data, clickstream_data['purchase_id'] == purchase_data['purchaseId'], 'left')

target_df = target_df.select(col('purchaseId'), col('purchaseTime'), col('billingCost'), col('isConfirmed'),
                             col('sessionId'), col('campaignId'), col('channelId'))
target_df.show()
target_df.write.mode('overwrite').parquet('../output/result.parquet')
