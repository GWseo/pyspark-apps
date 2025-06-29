from pyspark.sql.functions import to_json, struct
from pyspark.sql.dataframe import DataFrame


def send_to_kafka_json_payload(df: DataFrame, key_col_lst: list, val_col_lst: list, topic_nm: str):

    df2 = df.select(to_json(struct(*val_col_lst), options={'ignoreNullFields':'false'}).alias('key')
                    , to_json(struct(*key_col_lst), options={'ignoreNullFields':'false'}).alias('value')
                    )

    df2.write.format('kafka') \
            .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092") \
        .option('topic',topic_nm) \
        .save()
