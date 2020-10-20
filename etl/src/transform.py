"""
Given newline JSON with order data for each customer
The app read the data into Spark and calculate the net merchandise value of the ordered products).
After transforming the data it writes it into a mongodb file server.
"""

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pymongo import MongoClient
from loguru import logger

from pathlib import Path
from typing import List

from config import config

# Temporary hardcoded filepath. Would be good to fix in future
FILE_PATH = str(Path.cwd() / Path('etl/resources/data.json'))


def main():
    # start Spark application and get Spark session
    spark = SparkSession \
        .builder \
        .appName("net merchandise per customer") \
        .getOrCreate()

    df = load_json_data(FILE_PATH, spark)
    t_df = transform_data(df, spark)
    rows = dataframe_to_rows(t_df)

    # store the rows in mongodb database
    store_rows_in_database(rows)


def transform_data(df: DataFrame, spark) -> DataFrame:
    logger.debug("Starting data transformation...")

    df.createOrReplaceTempView('table')
    transformed = spark.sql(
        """
        with nett AS (
        select customerId
        , o.orderId as orderId,
            CASE
                WHEN b.productType = 'cold food' THEN b.grossMerchandiseValueEur - (b.grossMerchandiseValueEur * 0.15)
                WHEN b.productType = 'hot food' THEN b.grossMerchandiseValueEur - (b.grossMerchandiseValueEur * 0.07)
                WHEN b.productType = 'beverage' THEN b.grossMerchandiseValueEur - (b.grossMerchandiseValueEur * 0.09)
            END as nettMerchandiseValueEur
        from table
        lateral view explode(orders) as o
        lateral view explode(o.basket) as b
        )
    
        SELECT customerId
        , COUNT(orderId) as orders
        , ROUND(SUM(nettMerchandiseValueEur), 2) as totalNetMerchandiseValueEur
        FROM nett
        GROUP BY 1
        """)
    return transformed


def load_json_data(path: str, spark) -> DataFrame:
    logger.debug(f"Loading data at file path: {path}")
    return spark.read.json(path)


def store_rows_in_database(rows: List[dict]) -> None:
    collection = MongoClient()[config.DATABASE][config.COLLECTION]

    logger.info("Inserting rows into db...")

    collection.insert_many(rows)


def dataframe_to_rows(df: DataFrame) -> List[dict]:
    """
    creates a list of dicts where each dict is a row
    :param df: spark Dataframe
    :return: each row from the dataframe in a dict
    """
    m = map(lambda row: row.asDict(), df.collect())
    rows = [i for i in m]
    return rows


if __name__ == "__main__":
    main()
