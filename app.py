from transformations.business_logic import BusinessLogic


class Main:
    """
    Main method for executing the Pyspark process
    """

    def main(self, spark):
        ret_code = 0
        try:
            print("Hello world")
            business_logic = BusinessLogic("2021-01-31")
        except Exception as e:
            ret_code = -1
        return ret_code
