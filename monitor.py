
import time
from watcher import Watcher
from pyspark.sql import functions as f

def bronze(path):
    df = Watcher.spark.read.csv(path, header=True)
    return df


def silver(df):
    df = df.withColumn("teste", f.lit("Gabriel")).filter(f.col("id_aluno") != "null")
    return df


def query(sqltext):
    df = Watcher.spark.sql(sqltext)
    return df.show()


Watcher = Watcher(memory="16gb", cores="4", partitions=100)
# 4g, 2, 10: 6.153243780136108
# 8g, 4, 500: 5.7941038608551025
# 512m, 1, 1: 8.55656099319458
# Escrita em .csv: 11.940208911895752
# Escrita em .parquet:


start = time.time()

path = "aluno.csv"
df = bronze(path)
df = df.repartition(2)

# print(df.rdd.getNumPartitions())
# df.write.parquet("output/export_{}".format(start))
# df.show()

# print('Contagem sem null: {}'.format(df.count()))
# df.show()

end = time.time()
elapsed = end - start
print('Tempo decorrido: {} segundos.'.format(elapsed))
Watcher.execstatus()

partition_sizes = Watcher.get_partition_size(df)
for i, size in enumerate(partition_sizes):
    print(f"Partição {i}: {size / (1024 * 1024):.2f} MB")


# print('Finalizado!')

Watcher.sc.stop()

