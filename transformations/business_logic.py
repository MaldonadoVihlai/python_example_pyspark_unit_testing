from pyspark.sql.functions import *


class BusinessLogic:

    def __init__(self, date):
        self.date = date

    def filter_message_by_received_status(self, messages_df):
        """
        Filter the status to received from the messages DataFrame
        :param messages_df: Messages DataFrame
        :return: Filtered DataFrame with received status
        """
        return messages_df.filter(col("status") == "received")

    def format_message_dates(self, messages_df):
        """
        Format date_sent and date_created columns to yyyy-MM-dd
        :param messages_df: Messages DataFrame
        :return: DataFrame with date_sent and date_created formatted to yyyy-MM-dd
        """
        return messages_df.withColumn("date_sent", from_unixtime(substring(col("date_sent"), 1, 10), "yyyy-MM-dd")) \
            .withColumn("date_created", from_unixtime(substring(col("date_created"), 1, 10), "yyyy-MM-dd"))

    def filter_message_by_date_sent(self, messages_df):
        """
        Filter the column date_sent from the message DataFrame
        :param messages_df: Messages DataFrame
        :return: Filtered DataFrame with instance attribute date
        """
        return messages_df.filter(col("date_sent") == self.date)

    def join_messages_with_customer_tables(self, messages_df, customer_tables_df):
        """
        Join the messages DataFrame with the customers DataFrame by client_id
        :param messages_df: Messages DataFrame
        :param customer_tables_df: Customer DataFrame
        :return: Joined DataFrame
        """
        return messages_df.join(customer_tables_df, "client_id")
