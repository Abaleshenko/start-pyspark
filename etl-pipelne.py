from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.types as t
import pyspark.sql.functions as f


def extract_dataset(spark: SparkSession) -> DataFrame:
    url = "./dataset/weatherHistory.csv"
    return spark.read.option("header", 'true').csv(url)


def transform_dataset(df: DataFrame) -> DataFrame:
    outcome = (
        df
        .groupBy("Summary")
        .agg(
            f.count("Summary").alias("count"),
            f.min("Apparent Temperature (C)").alias("min_temp"),
            f.max("Apparent Temperature (C)").alias("max_temp")
        )
        .orderBy(f.col("count").desc())
    )

    return outcome


def save_dataset(df: DataFrame) -> None:
    df.coalesce(2).write.mode("overwrite").format("json").save("outcome.json")


def main():
    spark = SparkSession.builder.appName("Practice-2").getOrCreate()
    df = extract_dataset(spark)
    #print(transform_dataset(df))
    outcome = transform_dataset(df)
    save_dataset(outcome)
    spark.stop()



main()