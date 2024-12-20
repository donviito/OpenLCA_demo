import logging
import pandas as pd
import olca_schema as o
import OpenLCAClient
import DataHandler
import LCAConfig

class LCACalculator:
    def __init__(self, olca_client: OpenLCAClient, data_handler: DataHandler, olca_config: dict):
        self.olca_client = olca_client
        self.data_handler = data_handler
        self.olca_config = olca_config

    def calculate_for_products(self, main_table: str, result_table_name: str, electricity_table_name: str):
        """
        Main workflow: fetch main flows, fetch electricity table, and iterate over electricity values for LCA calculations.
        """
        # Fetch the flows table
        main_df = self.data_handler.read_data(main_table)
        logging.info("Fetched main flows table.")

        # Fetch the electricity table
        electricity_df = self.data_handler.read_data(electricity_table_name)
        logging.info("Fetched electricity consumption table.")

        if "Per1000mElectricityConsumption" not in electricity_df.columns:
            raise KeyError("Electricity table must have a column named 'Per1000mElectricityConsumption'.")

        results = []

        # Check if the ProductName column exists in the flows table
        if "ProductName" not in main_df.columns:
            raise KeyError("Main flows table must have a column named 'ProductName'.")

        # Retrieve the unique product names from the flows table
        product_names = main_df["ProductName"].unique()

        if len(product_names) != 1:
            raise ValueError("Flows table must contain exactly one unique 'ProductName'.")
        
        # Set the product name from the main flows table
        product_name = product_names[0]
        logging.info(f"Using product name: {product_name}")

        # Retrieve flow name and unit from config
        electricity_flow_name = self.olca_config['electricity_flow_name']
        electricity_unit = self.olca_config['electricity_unit']

        # Iterate over the electricity table
        for idx, electricity_row in electricity_df.iterrows():
            electricity_value = electricity_row["Per1000mElectricityConsumption"]
            logging.info(f"Running LCA for electricity value: {electricity_value} {electricity_unit}")

            main_df_with_electricity = main_df.copy()

            # Update existing electricity flow or add a new one
            electricity_index = main_df_with_electricity[
                main_df_with_electricity["Flow"] == electricity_flow_name
            ].index

            if not electricity_index.empty:
                # Update the existing electricity row's amount
                main_df_with_electricity.loc[electricity_index, "Amount"] = electricity_value
            else:
                # Add a new electricity flow if it doesn't exist
                electricity_flow = {
                    "Flow": electricity_flow_name,
                    "Amount": electricity_value,
                    "Unit": electricity_unit
                }
                electricity_row_df = pd.DataFrame([electricity_flow])
                main_df_with_electricity = pd.concat([main_df_with_electricity, electricity_row_df], ignore_index=True)

            # Prepare the LCA configuration
            process_name = product_name
            product_system_name = product_name
            lca_config = LCAConfig.LCAConfig.from_dataframe(
                main_df_with_electricity,
                process_name=process_name,
                product_system_name=product_system_name
            )

            # Create or update process and product system
            process, is_new_process, flow_to_provider = self.olca_client.get_or_create_process(lca_config)
            product_system = self.olca_client.get_or_create_product_system(process, product_system_name)

            # Perform the LCA calculation
            try:
                total_co2_emissions = self.olca_client.perform_lca_calculation(o.Ref(
                    id=product_system.id,
                    name=product_system.name,
                    ref_type=o.RefType.ProductSystem
                ))
            except Exception as e:
                logging.error(f"Failed to calculate LCA for electricity value {electricity_value}: {e}")
                total_co2_emissions = None

            # Append the result to the results list
            results.append({
                "ProductName": product_name,
                "ElectricityValue": electricity_value,
                "CO2Emissions": total_co2_emissions
            })

        # Convert results to a DataFrame
        results_df = pd.DataFrame(results)

        # Write results back to the results table in Databricks
        self.data_handler.write_results(results_df, result_table_name)
        logging.info("LCA results written to results table.")
