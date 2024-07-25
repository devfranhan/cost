from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w
from pyspark.sql.types import FloatType, StringType

appName = "test"
spark = SparkSession.builder.appName(appName).getOrCreate()

df = spark.createDataFrame(
    [
        ("0000009.48000", "Gabriel")
    ]
    , ["valor", "Nome"]
)

df.show()

df = (df.withColumn(
    "depois", f.regexp_replace(f.col("valor").cast("decimal(10, 2)").cast(StringType()), r"\.", "")
    )
)

df.show()

print(df.describe())

spark.stop()



