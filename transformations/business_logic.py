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

    def filter_message_by_date(self, messages_df):
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

    def group_messages_by_customer(self, messages_df):
        return messages_df.groupBy("")
