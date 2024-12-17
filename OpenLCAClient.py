from datetime import datetime
import olca_ipc as ipc
import olca_schema as o
import uuid
import logging
from typing import Tuple, Optional, Dict, Set
import LCAConfig

class OpenLCAClient:
    def __init__(self, ipc_port: int):
        self.client = ipc.Client(ipc_port)
        logging.info(f"Connected to OpenLCA IPC server on port {ipc_port}")


    def get_or_create_flow(self, flow_name: str, unit_name: str) -> o.Flow:
        """
        Retrieves an existing flow by name or creates a new one with the specified unit.
        """
        flow_descriptors = self.client.get_descriptors(o.Flow)
        flow_desc = next((f for f in flow_descriptors if f.name == flow_name), None)

        flow_property_ref, unit_ref = self.get_flow_and_unit_ref(unit_name)


        if flow_desc:
            flow = self.client.get(o.Flow, flow_desc.id)
            logging.info(f"Retrieved existing flow '{flow.name}' with expected unit '{unit_name}'")
            # Update flow properties
            existing_flow_property_ids = {fp.flow_property.id for fp in flow.flow_properties}
            if flow_property_ref.id not in existing_flow_property_ids:
                flow_property_factor = o.FlowPropertyFactor()
                flow_property_factor.flow_property = flow_property_ref
                flow_property_factor.unit = unit_ref
                flow_property_factor.conversion_factor = 1.0
                flow_property_factor.is_ref_flow_property = True
                flow.flow_properties.append(flow_property_factor)
                # OpenLCA can't parse dates, quickfix
                timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                flow.last_change = timestamp
                self.client.put(flow)
                logging.debug(f"Updated flow '{flow.name}' with new amount '{flow_property_ref.name}'")
        else:
            # Create a new flow with the specified unit and flow property
            flow = o.Flow()
            flow.name = flow_name
            flow.id = str(uuid.uuid4())
            flow.flow_type = o.FlowType.PRODUCT_FLOW

            flow_property_factor = o.FlowPropertyFactor()
            flow_property_factor.flow_property = flow_property_ref
            flow_property_factor.unit = unit_ref
            flow_property_factor.conversion_factor = 1.0
            flow_property_factor.is_ref_flow_property = True
            flow.flow_properties = [flow_property_factor]
            # OpenLCA can't parse dates, quickfix
            timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            flow.last_change = timestamp
            self.client.put(flow)
            logging.debug(f"Created new flow '{flow.name}' with unit '{unit_name}' and flow property '{flow_property_ref.name}'")
        return flow
    
    def get_flow_and_unit_ref(self, unit_name: str) -> Tuple[o.Ref, o.Ref]:
        """
        Retrieves both the flow property and unit reference based on the unit name.
        """
        flow_and_unit_map = {
            'kg': ('93e3e18a-a3c8-11da-a746-0800200b9a66', 'Mass', '93a60a56-a3c8-11da-a746-0800200b9a66'),  # Flow: Mass, Unit: kg
            't*km': ('e7e691d6-0a15-4d4a-96bf-f171b9f47c5b', 'Mass*Distance', 'd572e65f-3c92-4d34-9375-9d70dc8f520b'),  # Flow: Mass*Distance, Unit: t*km
            'MJ': ('d07e26f3-bc83-4de2-a0a2-9f6e6a9ecf9b', 'Energy', 'd07e26f3-bc83-4de2-a0a2-9f6e6a9ecf9b'),  # Flow: Energy, Unit: MJ
        }

        flow_and_unit_info = flow_and_unit_map.get(unit_name)
        if not flow_and_unit_info:
            logging.error(f"No mapping found for unit '{unit_name}'.")
            raise ValueError(f"Unsupported unit: {unit_name}")

        flow_property_id, flow_property_name, unit_id = flow_and_unit_info

        # Create the references for both flow property and unit
        flow_property_ref = o.Ref(
            id=flow_property_id,
            name=flow_property_name,
            ref_type=o.RefType.FlowProperty
        )

        unit_ref = o.Ref(
            id=unit_id,
            name=unit_name,
            ref_type=o.RefType.Unit
        )

        return flow_property_ref, unit_ref

    def get_or_create_process(self, lca_config: 'LCAConfig') -> Tuple[o.Process, bool, Set[str]]:
        """
        Retrieves an existing process by name or creates a new one based on the LCAConfig.
        Updates or adds exchanges and ensures the output flow is marked as the quantitative reference.
        """
        process_descriptors = self.client.get_descriptors(o.Process)
        process_desc = next((p for p in process_descriptors if p.name == lca_config.process_name), None)
        input_flow_ids = set()

        if process_desc:
            process = self.client.get(o.Process, process_desc.id)
            logging.info(f"Updating existing process '{process.name}'")
            is_new_process = False
        else:
            process = o.Process()
            process.name = lca_config.process_name
            process.id = str(uuid.uuid4())
            process.exchanges = []
            is_new_process = True

        # Existing exchange mapping (flow_id -> exchange)
        existing_exchanges = {ex.flow.id: ex for ex in process.exchanges}

        # Track the output flow to ensure it's marked as the quantitative reference
        output_flow_set_as_reference = False

        # Add or update exchanges from lca_config.flows
        for flow_config in lca_config.flows:
            flow = self.get_or_create_flow(flow_config['name'], flow_config['unit'])
            flow_property_ref, unit_ref = self.get_flow_and_unit_ref(flow_config['unit'])
            amount = flow_config['amount']

            if flow.id in existing_exchanges:
                # Update the existing exchange
                exchange = existing_exchanges[flow.id]
                exchange.amount = amount
                exchange.unit = unit_ref
                exchange.flow_property = flow_property_ref
                exchange.is_input = not flow_config.get('is_output', False)
                exchange.is_quantitative_reference = False
                logging.info(f"Updated exchange for flow '{flow.name}' in process '{process.name}' with amount '{amount}'")
            else:
                # Create a new exchange
                exchange = o.Exchange()
                exchange.flow = o.Ref(flow.id, flow.name, ref_type=o.RefType.Flow)
                exchange.amount = amount
                exchange.unit = unit_ref
                exchange.flow_property = flow_property_ref
                exchange.is_input = not flow_config.get('is_output', False)
                exchange.is_quantitative_reference = False
                exchange.internal_id = len(process.exchanges) + 1
                process.exchanges.append(exchange)
                logging.info(f"Added new exchange for flow '{flow.name}' to process '{process.name}'")
                if exchange.is_input:
                    input_flow_ids.add(flow.id)

            # Explicitly set the output flow as the quantitative reference
            if flow_config.get('is_output', False) and not output_flow_set_as_reference:
                exchange.is_quantitative_reference = True
                output_flow_set_as_reference = True
                exchange.unit = unit_ref
                logging.info(f"Set output flow '{flow.name}' as the quantitative reference for process '{process.name}' with amount '{amount}'")

        # OpenLCA can't parse dates, quickfix
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        process.last_change = timestamp


        # Save the updated or new process
        if is_new_process:
            self.client.put(process)
            logging.info(f"Created new process '{process.name}'")
        else:
            self.client.put(process)
            logging.info(f"Updated existing process '{process.name}'")

        # Validate that a reference flow is set
        if not any(ex.is_quantitative_reference for ex in process.exchanges if not ex.is_input):
            logging.error(f"No reference flow defined for process '{process.name}'.")
            raise Exception("The output flow must be set as the reference flow.")

        return process, is_new_process, input_flow_ids

        
    def get_or_create_product_system(self, process: o.Process, product_system_name: str) -> o.ProductSystem:
        """
        Simulates the 'Create Product System' button in the OpenLCA UI.
        """
        # Check if the product system already exists
        product_system_descriptors = self.client.get_descriptors(o.ProductSystem)
        product_system_desc = next((ps for ps in product_system_descriptors if ps.name == product_system_name), None)

        if product_system_desc:
            product_system = self.client.get(o.ProductSystem, product_system_desc.id)
            logging.info(f"Using existing product system '{product_system.name}'")
        else:
            product_system = o.ProductSystem()
            product_system.name = product_system_name
            product_system.id = str(uuid.uuid4())
            product_system.ref_process = o.Ref(process.id, process.name, ref_type=o.RefType.Process)

            # Automatically set the quantitative reference flow
            reference_exchange = next(
                (exchange for exchange in process.exchanges if exchange.is_quantitative_reference),
                None
            )
            if not reference_exchange:
                raise Exception("No quantitative reference flow found in the process.")

            product_system.ref_exchange = o.ExchangeRef(internal_id=reference_exchange.internal_id)
            product_system.target_amount = 1.0
            product_system.target_flow_property = o.Ref(
                id=reference_exchange.flow_property.id,
                name=reference_exchange.flow_property.name,
                ref_type=o.RefType.FlowProperty
            )
            product_system.target_unit = o.Ref(
                id=reference_exchange.unit.id,
                name=reference_exchange.unit.name,
                ref_type=o.RefType.Unit
            )

            # Automatically create process links for input flows
            product_system.process_links = []
            for exchange in process.exchanges:
                if exchange.is_input:
                    provider_process = self._find_provider_process(exchange.flow.id)
                    if provider_process:
                        product_system.process_links.append(o.ProcessLink(
                            provider=o.Ref(provider_process.id, provider_process.name, ref_type=o.RefType.Process),
                            flow=o.Ref(exchange.flow.id, exchange.flow.name, ref_type=o.RefType.Flow),
                            process=o.Ref(process.id, process.name, ref_type=o.RefType.Process),
                            exchange=o.ExchangeRef(internal_id=exchange.internal_id)
                        ))

            # Save the product system
            timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
            product_system.last_change = timestamp
            self.client.put(product_system)
            logging.info(f"Created new product system '{product_system.name}'")

        return product_system

    def _find_provider_process(self, flow_id: str) -> Optional[o.Process]:
        """
        Finds the provider process for a given flow by searching for a process that outputs it.
        """
        process_descriptors = self.client.get_descriptors(o.Process)
        for process_desc in process_descriptors:
            process = self.client.get(o.Process, process_desc.id)
            for exchange in process.exchanges:
                if not exchange.is_input and exchange.flow.id == flow_id:
                    return process
        return None



    def get_impact_method_with_gwp(self) -> o.ImpactMethod:
        # Retrieve all impact method descriptors
        impact_methods = self.client.get_descriptors(o.ImpactMethod)
        logging.debug(f"Available impact methods: {[im.name for im in impact_methods]}")

        # Specify the exact method name
        target_method_name = "ReCiPe 2016 Midpoint (H)"

        for im_desc in impact_methods:
            if im_desc.name == target_method_name:
                im = self.client.get(o.ImpactMethod, im_desc.id)
                logging.info(f"Found impact method '{im.name}' with ID: {im.id}")
                return im
        
        # Log all available impact methods if the target is not found
        logging.error(f"Impact method '{target_method_name}' not found. Available methods: {[im.name for im in impact_methods]}")
        raise Exception(f"Impact method '{target_method_name}' not found in the database.")

    def perform_lca_calculation(self, product_system_ref: o.Ref) -> float:
        """
        Runs the LCA calculation for the provided product system and retrieves the GWP result.
        """
        try:
            # Retrieve the impact method for GWP
            impact_method = self.get_impact_method_with_gwp()

            # Set up the calculation
            setup = o.CalculationSetup(
                target=product_system_ref,
                impact_method=o.Ref(
                    id=impact_method.id,
                    name=impact_method.name,
                    ref_type=o.RefType.ImpactMethod
                ),
                amount=1.0 
            )

            # Perform the calculation
            result = self.client.calculate(setup)
            result.wait_until_ready()
            logging.info("LCA calculation completed.")

            # Fetch the impact results
            impact_results = result.get_total_impacts()

            # Filter for the GWP result
            gwp_result = next(
                (impact for impact in impact_results if "global warming" in impact.impact_category.name.lower()),
                None
            )

            if gwp_result:
                logging.info(f"GWP result: {gwp_result.amount} kg CO₂ eq.")
                return gwp_result.amount
            else:
                logging.warning("GWP result not found in the impact results.")
                return 0.0

        except Exception as e:
            logging.error(f"Failed to perform LCA calculation: {e}")
            raise
