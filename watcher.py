
import time, pickle
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

class Watcher:
    
    def __init__(self, appName="Watcher", memory="1g", cores="1", partitions="1"):
        print('Memória: {}, Cores: {}, Partitions: {}'.format(memory, cores, partitions))
        conf = SparkConf().setAppName(appName) \
            .set("spark.eventLog.dir", "spark-events") \
            .set("spark.executor.memory", memory) \
            .set("spark.executor.cores", cores) \
            .set("spark.executor.shuffle.partitions", partitions)
            # .set("spark.eventLog.enabled", "true") \

        self.sc = SparkContext(conf=conf)

        self.spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

    def execstatus(self):
        info = self.sc._jsc.sc().getExecutorMemoryStatus()
        return print(info)

    def get_partition_size(self, df):
        def get_bytes(partition):
            return [len(pickle.dumps(row)) for row in partition]

        rdd = df.rdd.mapPartitions(get_bytes)
        partition_sizes = rdd.collect()
        return partition_sizes

    @staticmethod
    def timing():
        start = time.time()
        
        # Function
        
        end = time.time()
        elapsed_time = end - start
        return print('Tempo decorrido: {}'.format(elapsed_time))

        
