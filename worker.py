from pyspark.sql import SparkSession

import app


class InitTask():

    def __init__(self, spark):
        self.spark = spark

    def run_process(selfself, spark) -> int:
        print("run_process")
        process_main = app.Main()
        return process_main.main(spark)


def main(spark) -> int:
    task = InitTask(spark)
    print("#############")
    ret_code = task.run_process(spark)
    print("Exit code: %s" % ret_code)
    print("#############")


if __name__ == '__main__':
    spark = SparkSession.builder.appName('spark_test').getOrCreate()
    main(spark)
