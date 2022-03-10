from pyspark import SparkContext
from pyspark.sql.functions import *

import app


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


class InitTask():

    def __init__(self, spark):
        self.spark = spark

    def runProcess(selfself, spark) -> int:
        print("runProcess")
        process_main = app.Main()
        return process_main.main(spark)


def main(spark) -> int:
    task = InitTask(spark)
    print("#############")
    ret_code = task.runProcess(spark)
    print("Exit code: %s" % ret_code)
    print("#############")


if __name__ == '__main__':
    spark = SparkContext(master="local", appName="Spark Demo")
    main(spark)
