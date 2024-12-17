from databricks.connect import DatabricksSession
import logging
import pandas as pd


class DataHandler:
    def __init__(self, spark: DatabricksSession):
        self.spark = spark

    def read_data(self, table_name: str) -> pd.DataFrame:
        """
        Reads data from a Databricks table and returns a Pandas DataFrame.
        """
        logging.info(f"Reading data from Databricks: '{table_name}'")
        df = self.spark.table(table_name)
        pandas_df = df.toPandas()
        logging.info("Data read into Pandas DataFrame.")
        return pandas_df

    def write_results(self, results_df: pd.DataFrame, result_table_name: str):
        """
        Writes the results DataFrame back to a Delta table in Databricks.
        """
        logging.info(f"Writing results to Delta table '{result_table_name}'")
        results_spark_df = self.spark.createDataFrame(results_df)
        results_spark_df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("overwriteSchema", "true") \
            .saveAsTable(result_table_name)
        logging.info("Results written to Delta table successfully.")
