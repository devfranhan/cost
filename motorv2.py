
from pyspark.sql import SparkSession


class Motor:
    def __init__(self, appName="CalculoMotor"):
        self.spark = SparkSession.builder.appName(appName).getOrCreate()

    def calcular_sql(self):
        df = self.spark.createDataFrame(
            (
                (1, "Gabriel", 38402.93)
                , (1, "Gabriel", 200.54)
                , (1, "Gabriel", 890.00)
                , (1, "Gabriel", 150.99)
                , (2, "Vanjos", 4293.32)
                , (2, "Vanjos", 238.00)
            ), ["id", "nome", "valor"]
        )

        df.show()
        df.createOrReplaceTempView("TempView_SQL")

        df_sql = self.spark.sql(
            """
                SELECT *
                FROM TempView_SQL
                WHERE id = 1
            """
        )
        df_sql.show()

m = Motor()
m.calcular_sql()

