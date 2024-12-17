import pandas as pd
import logging
class LCAConfig:
    def __init__(self, process_name: str, product_system_name: str, flows: list):
        self.process_name = process_name
        self.product_system_name = product_system_name
        self.flows = flows  # List of flow dictionaries
    @staticmethod
    def from_dataframe(df: pd.DataFrame, process_name: str, product_system_name: str) -> 'LCAConfig':
        """
        Creates an LCAConfig instance from a DataFrame.
        """
        logging.info("Preparing configuration from DataFrame...")
        flows = []
        for index, row in df.iterrows():
            flow = {
                "name": row['Flow'],
                "amount": row['Amount'],
                "unit": row['Unit'],
                "is_output": False
            }
            flows.append(flow)
            logging.debug(f"Added input flow: {flow}")

        # Add product output flow
        total_wet_weight = df.loc[df['Unit'] == 'kg', 'Amount'].sum()
        product_flow = {
            "name": f"{process_name} Output",
            "amount": total_wet_weight,
            "unit": 'kg',
            "is_output": True,
            "is_reference": True
        }
        flows.append(product_flow)
        logging.info(f"Added product output flow: {product_flow}")

        return LCAConfig(process_name, product_system_name, flows)
