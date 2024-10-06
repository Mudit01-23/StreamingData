
import pandas as pd
from kafka import KafkaProducer
import time
import argparse

class DataFrameToKafka:

    def __init__(self, input, sep, kafka_sep, row_sleep_time, source_file_extension, bootstrap_servers,
                 topic, repeat, shuffle, key_index, excluded_cols):
        self.input = input
        print("input: {}".format(self.input))
        self.sep = sep
        print("sep: {}".format(self.sep))
        self.kafka_sep = kafka_sep
        print("kafka_sep: {}".format(self.kafka_sep))
        self.row_sleep_time = row_sleep_time
        print("row_sleep_time: {}".format(self.row_sleep_time))
        self.repeat = repeat
        print("repeat: {}".format(self.repeat))
        self.shuffle = shuffle
        print("shuffle: {}".format(self.shuffle))
        self.excluded_cols = excluded_cols
        print("self.excluded_cols: {}".format(self.excluded_cols))
        self.df = self.read_source_file(source_file_extension)
        self.topic = topic
        print("topic: {}".format(self.topic))
        self.key_index = key_index
        print("key_index: {}".format(self.key_index))
        print("bootstrap_servers: {}".format(bootstrap_servers))
        try:
            self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        except:
            print("No Broker available")

    def turn_df_to_str(self, df):
        x = df.values.astype(str)
        vals = [self.kafka_sep.join(ele) for ele in x]
        return vals

    def read_source_file(self, extension='csv'):
        if extension == 'csv':
            if self.shuffle:
                df = pd.read_csv(self.input, sep=self.sep).sample(frac=1)
            else:
                df = pd.read_csv(self.input, sep=self.sep)
            df = df.dropna()
            columns_to_write = [x for x in df.columns if x not in self.excluded_cols]
            print("columns_to_write", columns_to_write)
            df = df[columns_to_write]
            df['value'] = self.turn_df_to_str(df)
            return df
        else:
            if self.shuffle:
                df = pd.read_parquet(self.input, 'auto').sample(frac=1)
            else:
                df = pd.read_parquet(self.input, 'auto')
            df = df.dropna()
            columns_to_write = [x for x in df.columns if x not in self.excluded_cols]
            print("columns_to_write", columns_to_write)
            df = df[columns_to_write]
            df['value'] = self.turn_df_to_str(df)
            return df

    def df_to_kafka(self, batch_size=100):
        sayac = 0
        df_size = len(self.df) * self.repeat
        total_time = self.row_sleep_time * df_size
        for i in range(self.repeat):
            for start_index in range(0, len(self.df), batch_size):
                end_index = min(start_index + batch_size, len(self.df))
                batch_df = self.df.iloc[start_index:end_index]
                for index, row in batch_df.iterrows():
                    if self.key_index == 1000:
                        self.producer.send(self.topic, key=str(index).encode(), value=row['value'].encode())
                    else:
                        self.producer.send(self.topic, key=str(row[self.key_index]).encode(), value=row['value'].encode())
                    sayac += 1
                    remaining_per = 100 - (100 * (sayac / df_size))
                    remaining_time_secs = total_time - (self.row_sleep_time * sayac)
                    remaining_time_mins = remaining_time_secs / 60
                    print(f"{index} - {row['value']}")
                    print(f"{sayac}/{df_size} processed, % {remaining_per:.2f} will be completed in {remaining_time_mins:.2f} mins.")
                self.producer.flush()
                time.sleep(self.row_sleep_time)
            if sayac >= df_size:
                break
        self.producer.close()

if __name__ == "__main__":
    def str2bool(v):
        if isinstance(v, bool):
            return v
        if v.lower() in ('yes', 'true', 't', 'y', '1'):
            return True
        elif v.lower() in ('no', 'false', 'f', 'n', '0'):
            return False
        else:
            raise argparse.ArgumentTypeError('Boolean value expected.')

    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", required=False, type=str, default="/Users/muditjoshi/Desktop/StreamingData/sensors.csv",
                    help="Source data path. Default: /Users/muditjoshi/Desktop/StreamingData/sensors.csv")
    ap.add_argument("-s", "--sep", required=False, type=str, default=",",
                    help="Source data file delimiter. Default: ,")
    ap.add_argument("-e", "--source_file_extension", required=False, type=str, default="csv",
                    help="Extension of data file. Default: csv")
    ap.add_argument("-ks", "--kafka_sep", required=False, type=str, default=",",
                    help="Kafka value separator. What should be the sep in Kafka. Default: ,")
    ap.add_argument("-rst", "--row_sleep_time", required=False, type=float, default=0.5,
                    help="Sleep time in seconds per row. Default: 0.5")
    ap.add_argument("-t", "--topic", required=False, type=str, default="office_input",
                    help="Kafka topic. Which topic to produce. Default: office_input")
    ap.add_argument("-b", "--bootstrap_servers", required=False, nargs='+', default=["localhost:9092"],
                    help="Kafka bootstrap servers and port in a python list. Default: [localhost:9092]")
    ap.add_argument("-r", "--repeat", required=False, type=int, default=1,
                    help="How many times to repeat dataset. Default: 1")
    ap.add_argument("-shf", "--shuffle", required=False, type=str2bool, default=False,
                    help="Shuffle the rows?. Default: False")
    ap.add_argument("-k", "--key_index", required=False, type=int, default=1000,
                    help="Which column will be send as key to kafka? If not used this option, pandas dataframe index will "
                         "be send. Default: 1000 indicates pandas index will be used.")
    ap.add_argument("-exc", "--excluded_cols", required=False, nargs='+', default=['it_is_impossible_column'],
                    help="The columns not to write log file?. Default ['it_is_impossible_column']. Ex: -exc 'Species' 'PetalWidthCm'")
    ap.add_argument("-bs", "--batch_size", required=False, type=int, default=100,
                    help="Batch size for sending messages to Kafka. Default: 100")

    args = vars(ap.parse_args())

    df_to_kafka = DataFrameToKafka(
        input=args['input'],
        sep=args['sep'],
        kafka_sep=args['kafka_sep'],
        row_sleep_time=args['row_sleep_time'],
        source_file_extension=args['source_file_extension'],
        topic=args['topic'],
        bootstrap_servers=args['bootstrap_servers'],
        repeat=args['repeat'],
        shuffle=args['shuffle'],
        key_index=args['key_index'],
        excluded_cols=args['excluded_cols']
    )
    df_to_kafka.df_to_kafka(batch_size=args['batch_size'])



### this is python dataframe to kafka running code - python dataframe_to_kafka.py -i /Users/muditjoshi/Desktop/StreamingData/sensors.csv -t office_input -rst 0.1 -bs 100
