from pyspark.sql import SparkSession
from etl.src.transform import transform_data, load_json_data
from decimal import Decimal


def test_transform_data():
    """ test input/output from transformation"""

    spark = SparkSession.builder.master('local[*]').appName('unit-testing').getOrCreate()

    # input a single row with 3 different product types to validate correct conversion
    df = load_json_data('../etl/resources/test_data.json', spark)

    df = transform_data(df, spark)

    # assert number of orders are 3
    assert df.collect()[0].asDict()['orders'] == 3

    # assert the total net is correct
    assert df.collect()[0].asDict()['totalNetMerchandiseValueEur'] == Decimal('26.90')


