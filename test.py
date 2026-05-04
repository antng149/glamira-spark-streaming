import time

from pyspark.sql import *
from pyspark.sql.functions import col
from browser.browser import parse_browser_udf

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TestExternalPythonLib") \
        .getOrCreate()

    print("Start TestExternalPythonLib")

    data_list = [
        [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"
        ],
        [
            "Mozilla/5.0 (Windows NT x.y; Win64; x64; rv:10.0) Gecko/20100101 Firefox/10.0"
        ]
    ]

    df = spark.createDataFrame(data_list).toDF("user_agent")

    df.printSchema()

    df = df.withColumn("browser", parse_browser_udf("user_agent"))

    df.printSchema()

    df.show()

    # existed_browser_df = spark.read \
    #     .format("jdbc") \
    #     .option("driver", "org.postgresql.Driver") \
    #     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    #     .option("dbtable", "browser") \
    #     .option("user", "postgres") \
    #     .option("password", "UnigapPostgres@123") \
    #     .load()
    #
    # new_browser_df = df.select(col("browser").alias("name")).subtract(existed_browser_df.select("name"))
    #
    # new_browser_df.show()
    #
    # new_browser_df.write \
    #     .format("jdbc") \
    #     .option("driver", "org.postgresql.Driver") \
    #     .option("url", "jdbc:postgresql://postgres:5432/postgres") \
    #     .option("dbtable", "browser") \
    #     .option("user", "postgres") \
    #     .option("password", "UnigapPostgres@123") \
    #     .mode("append") \
    #     .save()

    time.sleep(3600)

    print("Stop TestExternalPythonLib")
    spark.stop()
