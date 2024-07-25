
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.window import Window as w

class Motor:
    def __init__(self, appName="CalculoMotor"):
        self.spark = SparkSession.builder.appName(appName).getOrCreate()

    def testData(self):
        df = self.spark.createDataFrame(
            [
                (1, "Gabriel", 38402.93),
                (1, "Gabriel", 200.54),
                (1, "Gabriel", 890.00),
                (1, "Gabriel", 150.99),
                (2, "Franhan", 4293.32),
                (2, "Franhan", 238.00)
            ],
            ["id", "nome", "valor"]
        )
        return df

    def sparksql(self):
        df = self.testData()

        df.createOrReplaceTempView("TempView_SQL")

        df_sql = self.spark.sql(
            """
                SELECT *, CAST(CAST(valor AS float) * CAST(tipo AS float) AS DECIMAL(14, 2)) vlr_final_comiss
                FROM
                (
                    SELECT *
                        , CASE
                            WHEN qtde <= 2 THEN 0.036
                            WHEN qtde > 2 THEN 0.067
                            ELSE 0 
                            END tipo
                    FROM
                    (
                        SELECT *
                            , COUNT(id) OVER(PARTITION BY id) qtde
                        FROM TempView_SQL
                    )
                        AS A
                )
                    AS A
            """
        )
        print("SparkSQL:")
        df_sql.show()

    def col1(self, df):
        df = df.withColumn(
            "tipo", f.when(
                f.col("qtde") <= 2, 0.036
            ).when(
                f.col("qtde") > 2, 0.067
            ).otherwise(0)
        )
        return df

    def col2(self, df):
        df = df.withColumn(
            "vlr_final_comiss", (f.cast(float, f.col("valor")) * f.cast(float, f.col("tipo"))).cast("decimal(14,2)")
        )
        return df

    def pyspark(self):
        df = self.testData()

        # Instanciando a chave da tabela
        key1 = w.partitionBy("id")
        mot = Motor()

        df_pyspark = df.withColumn(
            "qtde", f.count("id").over(key1)
            )
        df_pyspark = mot.col1(df_pyspark)
        df_pyspark = mot.col2(df_pyspark)
        print("PySpark:")
        df_pyspark.show()


m = Motor()
m.sparksql()
m.pyspark()


