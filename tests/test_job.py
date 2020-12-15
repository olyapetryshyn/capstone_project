import unittest
from pyspark.sql import SparkSession
from jobs.job import (
    clickstream_schema,
    purchase_schema,
    transform_data,
    transform_data_with_udf,
    task_21_df_api,
    task_21_sql,
    task_22_df_api,
    task_22_sql
)


class SparkJobTests(unittest.TestCase):
    def setUp(self):
        """
        Start Spark and define path to test data.
        """
        self.spark = SparkSession \
            .builder \
            .appName('spark_test_app') \
            .getOrCreate()
        self.test_data_path = '../data/test/'
        self.expected_data = (
            self.spark
                .read
                .parquet(self.test_data_path +
                         'transform_data_result/*.snappy.parquet'))
        self.expected_data_udf = (
            self.spark
                .read
                .parquet(self.test_data_path +
                         'transform_data_with_udf/*.snappy.parquet')
        )
        self.expected_data.createOrReplaceTempView('target')

    def test_transform_data(self):
        input_clicks_data = (
            self.spark
                .read
                .options(header='True')
                .schema(clickstream_schema)
                .csv(self.test_data_path + 'mobile_app_clickstream_0.csv.gz'))
        input_purchases_data = (
            self.spark
                .read
                .options(header='True')
                .schema(purchase_schema)
                .csv(self.test_data_path + 'user_purchases_0.csv.gz'))
        expected_cols = len(self.expected_data_udf.columns)
        expected_rows = self.expected_data_udf.count()

        data_transformed = transform_data(input_clicks_data, input_purchases_data)

        cols = len(data_transformed.columns)
        rows = data_transformed.count()

        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in self.expected_data_udf.columns
                         for col in data_transformed.columns])

    def test_transform_data_with_udf(self):
        input_clicks_data = (
            self.spark
                .read
                .options(header='True')
                .schema(clickstream_schema)
                .csv(self.test_data_path + 'mobile_app_clickstream_0.csv.gz'))
        input_purchases_data = (
            self.spark
                .read
                .options(header='True')
                .schema(purchase_schema)
                .csv(self.test_data_path + 'user_purchases_0.csv.gz'))
        expected_cols = len(self.expected_data.columns)
        expected_rows = self.expected_data.count()

        data_transformed = transform_data_with_udf(input_clicks_data, input_purchases_data)

        cols = len(data_transformed.columns)
        rows = data_transformed.count()

        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in self.expected_data.columns
                         for col in data_transformed.columns])

    def test_task_21_df(self):
        expected_result = (
            self.spark
                .read
                .parquet(self.test_data_path +
                         'task2.1/df_api/*.snappy.parquet'))
        expected_cols = len(expected_result.columns)
        expected_rows = expected_result.count()
        result = task_21_df_api(self.expected_data)
        cols = len(result.columns)
        rows = result.count()
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_result.columns
                         for col in result.columns])

    def test_task_21_sql(self):
        expected_result = (
            self.spark
                .read
                .parquet(self.test_data_path +
                         'task2.1/plain_sql/*.snappy.parquet'))
        expected_cols = len(expected_result.columns)
        expected_rows = expected_result.count()
        result = task_21_sql(self.spark)
        cols = len(result.columns)
        rows = result.count()
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_result.columns
                         for col in result.columns])

    def test_task_22_df(self):
        expected_result = (
            self.spark
                .read
                .parquet(self.test_data_path +
                         'task2.2/df_api/*.snappy.parquet'))
        expected_cols = len(expected_result.columns)
        expected_rows = expected_result.count()
        result = task_22_df_api(self.expected_data)
        cols = len(result.columns)
        rows = result.count()
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_result.columns
                         for col in result.columns])

    def test_task_22_sql(self):
        expected_result = (
            self.spark
                .read
                .parquet(self.test_data_path +
                         'task2.2/plain_sql/*.snappy.parquet'))
        expected_cols = len(expected_result.columns)
        expected_rows = expected_result.count()
        result = task_22_sql(self.spark)
        cols = len(result.columns)
        rows = result.count()
        self.assertEqual(expected_cols, cols)
        self.assertEqual(expected_rows, rows)
        self.assertTrue([col in expected_result.columns
                         for col in result.columns])

    def tearDown(self):
        """
        Stop Spark.
        """
        self.spark.stop()


if __name__ == '__main__':
    unittest.main()
