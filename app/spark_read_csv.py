import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

MAIN_FOLDER = "/app/data/KETI"
OUTPUT_FOLDER = "/app/output/sensor_data"
CSV_FILE_NAMES = ["co2", "humidity", "light", "pir", "temperature"]

spark = (
    SparkSession.builder.master("local[*]").appName("Read to Sensor Data").getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

schema = StructType(
    [
        StructField("ts_min_bignt", LongType(), False),
        StructField("value", FloatType(), False),
        StructField("event_ts_min", DateType(), False),
        StructField("room", StringType(), False),
    ]
)


def read_csv(csv_file_name):
    result_df = None
    for sub_folder in os.listdir(MAIN_FOLDER):
        sub_folder_path = os.path.join(MAIN_FOLDER, sub_folder)

        if not os.path.isdir(sub_folder_path):  # klasör değilse geç
            continue

        # KETI/413/co2.csv
        csv_file_path = os.path.join(sub_folder_path, csv_file_name + ".csv")

        # read csv
        df = spark.read.csv(csv_file_path, schema=schema)
        df = df.withColumn("room", lit(sub_folder))
        df = df.withColumn("event_ts_min", to_timestamp(col("ts_min_bignt")))

        if result_df is None:
            result_df = df
        else:
            result_df = result_df.union(df)

    return result_df


def create_table(csv_file_name):
    df = read_csv(csv_file_name)
    print(f"Read csv files. '{csv_file_name}'")

    df.createOrReplaceTempView(csv_file_name)
    print(f"Create sql tempview. '{csv_file_name}'")

    print()


def final_df():
    return spark.sql(
        """
        select 
            co2.event_ts_min,
            co2.ts_min_bignt,
            co2.room,
            co2.value co2,
            humidity.value humidity,
            light.value light,
            pir.value pir,
            temperature.value temperature
        from co2 
            join humidity    on co2.ts_min_bignt = humidity.ts_min_bignt    and co2.room = humidity.room
            join light       on co2.ts_min_bignt = light.ts_min_bignt       and co2.room = light.room
            join pir         on co2.ts_min_bignt = pir.ts_min_bignt         and co2.room = pir.room
            join temperature on co2.ts_min_bignt = temperature.ts_min_bignt and co2.room = temperature.room
        order by room
    """
    )


def save_df(df, file_path):
    df.withColumn(
        "event_ts_min", date_format(col("event_ts_min"), format="yyyy-MM-dd HH:mm:ss")
    ).coalesce(1).write.csv(path=file_path, header=True, mode="overwrite")

    print(f"The final dataframe was saved as a csv file. {file_path}")


if __name__ == "__main__":
    for name in CSV_FILE_NAMES:
        create_table(name)

    df = final_df()

    print("--Final DataFrame--")

    df.show(truncate=False)

    save_df(df, OUTPUT_FOLDER)
