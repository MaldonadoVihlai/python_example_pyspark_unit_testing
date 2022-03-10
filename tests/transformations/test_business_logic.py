from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from transformations.business_logic import BusinessLogic


@pytest.fixture(scope="module")
def spark_test():
    spark = SparkSession.builder.appName('spark_test').getOrCreate()
    yield spark


@pytest.fixture(scope="module")
def process():
    process = BusinessLogic("2021-01-31")
    yield process


@pytest.fixture(scope="module")
def messages_dummy(spark_test):
    messages_columns = StructType([
        StructField("direction", StringType()),
        StructField("status", StringType()),
        StructField("num_segments", IntegerType()),
        StructField("from", StringType()),
        StructField("to", StringType()),
        StructField("account_sid", StringType()),
        StructField("date_created", TimestampType()),
        StructField("date_sent", TimestampType()),
        StructField("price_unit", StringType()),
        StructField("price", DoubleType()),
        StructField("body", StringType()),
        StructField("client_id", StringType()),
    ])
    messages_rows = [("inbound", "received", 1, "Whatsapp:+48 727 652 6435", "+385 415 249 4655", "E9133",
                      datetime.fromtimestamp(1615313471), datetime.fromtimestamp(1624225552), "USD", 0.0,
                      "Ergonomic actuating algorithm", "62294f94fc13ae30b200006a"),
                     ("inbound", "read", 1, "Whatsapp:+33 950 704 3164", "+86 962 261 0830", "42653",
                      datetime.fromtimestamp(1626153930), datetime.fromtimestamp(1634162427), "USD", 0.0,
                      "Profit-focused even-keeled leverage", "62294f94fc13ae30b200006b"),
                     ("inbound", "sent", 1, "Whatsapp:+1 704 847 4690", "+86 138 711 6628", "82110",
                      datetime.fromtimestamp(1639301707), datetime.fromtimestamp(1638974520), "USD", 0.0,
                      "Up-sized local ability", "62294f94fc13ae30b200006z")]

    messages_df = spark_test.createDataFrame(messages_rows, schema=messages_columns)
    yield messages_df


@pytest.fixture(scope="module")
def customers_dummy(spark_test):
    customer_columns = StructType([
        StructField("client_id", StringType()),
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("phone_number", StringType()),
        StructField("document_id", StringType())
    ])
    customer_rows = [("62294f94fc13ae30b200006a", "Natka", "Dible", "202-887-9971", "74-105-3121"),
                     ("62294f94fc13ae30b200006b", "Gasparo", "Chattoe", "961-436-2248", "27-441-2577"),
                     ("62294f94fc13ae30b200006c", "Dean", "Ladd", "946-783-0724", "53-256-8467"),
                     ("62294f94fc13ae30b200006d", "Robbi", "Swains", "645-126-9095", "63-233-7191")]

    customer_df = spark_test.createDataFrame(customer_rows, schema=customer_columns)
    yield customer_df


def test_message_dummy(messages_dummy, customers_dummy):
    assert messages_dummy.count() == 3
    assert customers_dummy.count() == 4


def test_filter_message_by_received_status(process, messages_dummy):
    message_filtered_by_receive_status_df = process.filter_message_by_received_status(messages_dummy)
    assert message_filtered_by_receive_status_df.filter(col("status") != "received").count() == 0


def test_filter_message_by_date(process, messages_dummy):
    message_filtered_by_date_df = process.filter_message_by_date(messages_dummy)
    assert message_filtered_by_date_df.filter(col("date_sent") != "2021-01-31").count() == 0


def test_join_messages_with_customer_tables(process, messages_dummy, customers_dummy):
    join_messages_customer_df = process.join_messages_with_customer_tables(messages_dummy, customers_dummy)
    assert join_messages_customer_df.count() == 2
