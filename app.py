from transformations.business_logic import BusinessLogic


class Main:
    """
    Main method for executing the Pyspark process
    """

    def main(self, spark):
        ret_code = 0
        try:
            business_logic = BusinessLogic("2021-07-19")
            messages_df = spark.read.option("header", True).csv("resources/inputs/messages.csv")
            customers_df = spark.read.option("header", True).csv("resources/inputs/customers.csv")
            messages_filtered_by_status_df = business_logic.filter_message_by_received_status(messages_df)
            messages_formatted_dates_df = business_logic.format_message_dates(messages_filtered_by_status_df)
            messages_filtered_by_date_df = business_logic.filter_message_by_date_sent(messages_formatted_dates_df)
            join_messages_customer_df = business_logic.join_messages_with_customer_tables(messages_filtered_by_date_df,
                                                                                          customers_df)
            join_messages_customer_df.show()
            join_messages_customer_df.write.mode("overwrite").parquet("resources/outputs/")

        except Exception as e:
            print(e)
            ret_code = -1
        return ret_code
