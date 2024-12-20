# Databricks and OpenLCA Integration Documentation

This documentation outlines the steps to set up and execute an automated Life Cycle Assessment (LCA) workflow using Databricks and OpenLCA. The solution integrates data handling through Databricks and calculation via OpenLCA's IPC server, ensuring dynamic and automated assessments.

**Note:** This is a high-level demo showcasing how LCA calculations can be somewhat automated using OpenLCA. It is not a production-ready implementation.

## Overview

This example is tailored for **Mirka** as part of the **Data-Driven Sustainability Management project** to demonstrate how LCA can be automated using Databricks and OpenLCA.

**Important Notes:**
- This example does not conduct a real LCA but demonstrates how dynamic calculations could be implemented.
- To showcase dynamic calculations, the raw material data for one product is used, while different electricity values are queried from another table. Results are written to a Delta table in Databricks.
- For this example to work, the flow names in Databricks must match those in the LCI database opened in OpenLCA. For further enhancement, this matching should be based on unique identifiers, such as CAS numbers.

This example is tailored for **Mirka** as part of the **Data-Driven Sustainability Management project** to demonstrate how LCA can be automated using Databricks and OpenLCA.

The solution leverages:

1. **Databricks Connect** for interacting with Databricks clusters.
2. **OpenLCA IPC** for performing LCA calculations programmatically.
3. **Configurations** to ensure flexibility and portability.

---

## Prerequisites

### Tools and Libraries
- Python 3.8+
- Databricks Connect
- OpenLCA (with IPC server enabled)
- Libraries:
  - `databricks-connect`
  - `pyyaml`
  - OpenLCA schema definitions

### Databricks Setup
1. A Databricks workspace URL and access to a cluster.
2. Personal Access Token (PAT) for authentication.
3. Cluster ID where jobs will run.

### OpenLCA Setup
- **IPC Server Requirement:** Ensure the OpenLCA IPC server is running on the specified port before attempting to connect. This is crucial for the script to interact with OpenLCA for LCA calculations.
- **IPC Server Requirement:** Ensure the OpenLCA IPC server is running on the specified port before attempting to connect. This is crucial for the script to interact with OpenLCA for LCA calculations.
- OpenLCA installed and configured.
- IPC server enabled with a specific port.
- Impact assessment methods (e.g., ReCiPe) configured in OpenLCA.

---

## Configuration

### `config.yaml`
Define workflow-specific settings:

```yaml
openlca:
  ipc_port: 8080
  impact_method_name: "ReCiPe 2016 Midpoint (H)"
  electricity_flow_name: "electricity, low voltage, production FI, at grid/kWh/FI U"
  electricity_unit: "MJ"

databricks:
  table_name: 'lca_test.bronze.chemical_classification1'
  result_table_name: 'lca_test.bronze.resultstable'
  electricity_table_name: 'lca_test.bronze.electricityconsumption'
```

---

## Script Execution Workflow

### Script Breakdown

1. **Configuration Loading**:
   The script loads settings from `config.yaml` and validates required inputs.

2. **Databricks Session**:
   A `DatabricksSession` is initialized using credentials from environment variables.

3. **Data Handlers**:
   - `DataHandler` handles reading and writing data to/from Databricks.
   - `OpenLCAClient` connects to OpenLCA IPC for LCA calculations.

4. **LCA Workflow**:
   - Reads product and electricity data from Databricks.
   - Updates electricity consumption dynamically.
   - Performs LCA calculations for each electricity scenario.

5. **Result Storage**:
   Outputs results to a Delta table in Databricks.

### Execution

Save the script as `main.py` and run:

```bash
python main.py
```

---

## Key Components

### 1. `main.py`
Responsible for:
- Loading configurations.
- Initializing Databricks and OpenLCA clients.
- Managing the LCA workflow.

### 2. `DataHandler`
Handles:
- `read_data(table_name: str)`: Reads data from a specified Databricks table and returns it as a Pandas DataFrame.
- `write_results(results_df: pd.DataFrame, result_table_name: str)`: Writes a Pandas DataFrame back to a Delta table in Databricks.
- Reading data from Databricks tables into Pandas DataFrames.
- Writing processed results back to Delta tables.

### 3. `OpenLCAClient`
Responsible for:
- `get_or_create_flow(flow_name: str, unit_name: str)`: Retrieves or creates a flow in OpenLCA based on the provided name and unit.
- `get_or_create_process(lca_config: LCAConfig)`: Creates or updates a process in OpenLCA based on the LCA configuration.
- `perform_lca_calculation(product_system_ref: o.Ref)`: Executes the LCA calculation and returns results for a given product system.
- Creating or updating flows, processes, and product systems.
- Running LCA calculations.

### 4. `LCACalculator`
Manages:
- `calculate_for_products(main_table: str, result_table_name: str, electricity_table_name: str)`: Coordinates the full LCA workflow, including reading data, updating configurations, and storing results.
- Iterating through electricity scenarios.
- Preparing configurations for OpenLCA.
- Storing results.

---

## Example Output

A sample result row stored in the Delta table:

| ProductName       | ElectricityValue | CO2Emissions |
|-------------------|------------------|--------------|
| Product_Example   | 50 MJ            | 12.5 kg      |

---

## Error Handling

### Common Errors
1. **Missing Environment Variables**:
   Ensure all required variables are set in your environment.

2. **Configuration Errors**:
   Verify the `config.yaml` structure and values.

3. **OpenLCA Connection Issues**:
   Check that the IPC server is running on the specified port.

### Logs
All events and errors are logged for debugging:

- Log Level: INFO
- Format: `%(asctime)s %(levelname)s:%(message)s`

---

## Extending the Solution

1. **New Data Flows**:
   Modify `DataHandler` to handle additional data sources.

2. **Custom Impact Methods**:
   Update the `impact_method_name` in `config.yaml`.

3. **Additional Metrics**:
   Extend `LCACalculator` to capture other environmental impacts.

---

## Security Recommendations
- Avoid exposing sensitive configurations or tokens.
- Use role-based access control (RBAC) for Databricks and OpenLCA resources.
- Rotate PATs periodically.

---

## Conclusion
This solution demonstrates a high-level, demo example of integrating Databricks and OpenLCA for automated LCA workflows. It is intended as a starting point to explore automation possibilities, not as a production-ready system. Customize the implementation further based on your organization’s specific requirements.

