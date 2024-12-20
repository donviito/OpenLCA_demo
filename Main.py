import os
import logging
import yaml
from databricks.connect import DatabricksSession
import OpenLCAClient, LCACalculator, DataHandler

def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

    config_path = 'config.yaml'
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Could not find {config_path} in {os.getcwd()}")

    # Load configuration
    with open(config_path, 'r') as file:
        config_data = yaml.safe_load(file)


    spark = DatabricksSession.builder.remote(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN"),
        cluster_id=os.getenv("DATABRICKS_CLUSTER_ID")
    ).getOrCreate()


    data_handler = DataHandler.DataHandler(spark)

    olca_client = OpenLCAClient.OpenLCAClient(config_data['openlca']['ipc_port'])

    lca_calculator = LCACalculator.LCACalculator(
        olca_client,
        data_handler,
        config_data['openlca']
    )

    try:
        # Perform LCA calculations
        lca_calculator.calculate_for_products(
            main_table=config_data['databricks']['table_name'],
            result_table_name=config_data['databricks']['result_table_name'],
            electricity_table_name=config_data['databricks']['electricity_table_name']
        )

    finally:
        # olca_client.close()
        logging.info("Done")

if __name__ == "__main__":
    main()