"""
End-to-End Mock Charging Session Flow
=====================================

This module demonstrates a complete EV charging session flow using Beckn-OCPI bridge.
The flow includes: Search → Select → Confirm → Status → Stop → CDR

Each step shows:
- Beckn request format
- OCPI transformation
- OCPI response
- Beckn response format

Author: Generated for Beckn-OCPI Integration
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, Optional

# Import all required modules
from beckn_modules import (
    BecknSearchRequest,
    BecknSelectRequest,
    BecknConfirmRequest,
    BecknStatusRequest,
    BecknUpdateRequest,
    BecknCDRRequest
)
from ocpi_modules import OCPIClient
from beckn_ocpi_bridge import BecknOCPIBridge
from search_module import process_beckn_search_request
from init_module import handle_beckn_select_request
from confirm import handle_beckn_confirm_request
from status_module import handle_beckn_status_request
from update_module import handle_stop_charging_session, handle_beckn_cdr_request

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MockChargingSessionManager:
    """
    Manages the complete end-to-end charging session flow including CDR generation
    """

    def __init__(self, ocpi_base_url: str = "https://mock-ocpi.example.com",
                 ocpi_token: str = "mock_token_123", mock_mode: bool = True):
        self.ocpi_base_url = ocpi_base_url
        self.ocpi_token = ocpi_token
        self.mock_mode = mock_mode
        self.transaction_id = str(uuid.uuid4())
        self.session_id = None
        self.cdr_id = None

        # Common context for all requests
        self.base_context = {
            "domain": "mobility:ev_charging",
            "country": "IND",
            "city": "std:080",
            "core_version": "1.1.0",
            "bap_id": "beckn-bap.example.com",
            "bap_uri": "https://beckn-bap.example.com",
            "bpp_id": "ocpi-bpp.example.com",
            "bpp_uri": "https://ocpi-bpp.example.com",
            "transaction_id": self.transaction_id,
            "ttl": "PT30S"
        }

        logger.info(f"Initialized Mock Charging Session Manager")
        logger.info(f"Transaction ID: {self.transaction_id}")
        logger.info(f"OCPI Base URL: {self.ocpi_base_url}")
        logger.info(f"Mock Mode: {self.mock_mode}")

    def step_1_search_charging_stations(self, user_location: str = "12.9716,77.5946") -> Dict[str, Any]:
        """
        Step 1: Search for charging stations near user location

        Flow: Beckn Search Request → OCPI Locations Query → Beckn Search Response
        """
        logger.info("\n" + "="*60)
        logger.info("STEP 1: SEARCHING FOR CHARGING STATIONS")
        logger.info("="*60)

        # Create Beckn search request
        search_request = {
            "context": {
                **self.base_context,
                "action": "search",
                "message_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            "message": {
                "intent": {
                    "fulfillment": {
                        "start": {
                            "location": {
                                "gps": user_location,
                                "address": "Bangalore, Karnataka, India"
                            },
                            "time": {
                                "range": {
                                    "start": datetime.now(timezone.utc).isoformat(),
                                    "end": (datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)).isoformat()
                                }
                            }
                        }
                    },
                    "item": {
                        "category": {
                            "id": "ev_charging"
                        }
                    }
                }
            }
        }

        logger.info("Beckn Search Request:")
        logger.info(json.dumps(search_request, indent=2))

        # Process search request using existing search module
        search_response = process_beckn_search_request(
            search_request,
            self.ocpi_base_url,
            self.ocpi_token,
            mock_mode=self.mock_mode
        )

        logger.info("Beckn Search Response:")
        logger.info(
            f"Found {len(search_response.get('message', {}).get('catalog', {}).get('items', []))} charging options")

        # Log key information about found stations
        catalog = search_response.get('message', {}).get('catalog', {})
        locations = catalog.get('locations', [])
        items = catalog.get('items', [])

        for i, location in enumerate(locations[:3]):  # Show first 3 locations
            logger.info(
                f"Location {i+1}: {location.get('descriptor', {}).get('name', 'Unknown')}")
            logger.info(f"   GPS: {location.get('gps', 'Unknown')}")
            logger.info(
                f"   Address: {location.get('address', {}).get('full', 'Unknown')}")

        return search_response

    def step_2_select_charging_station(self, search_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Step 2: Select a specific charging station and connector

        Flow: Beckn Select Request → OCPI Session Initiation → Beckn Select Response
        """
        logger.info("\n" + "="*60)
        logger.info("STEP 2: SELECTING CHARGING STATION")
        logger.info("="*60)

        # Extract first available item from search response
        catalog = search_response.get('message', {}).get('catalog', {})
        items = catalog.get('items', [])
        locations = catalog.get('locations', [])
        fulfillments = catalog.get('fulfillments', [])

        if not items:
            raise ValueError("No charging stations found in search response")

        selected_item = items[0]  # Select first available charging option
        selected_location = locations[0] if locations else {}
        selected_fulfillment = fulfillments[0] if fulfillments else {}

        logger.info(
            f"Selected Item: {selected_item.get('descriptor', {}).get('name', 'Unknown')}")
        logger.info(
            f"Selected Location: {selected_location.get('descriptor', {}).get('name', 'Unknown')}")

        # Create Beckn select request
        select_request = {
            "context": {
                **self.base_context,
                "action": "select",
                "message_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            "message": {
                "order": {
                    "items": [
                        {
                            "id": selected_item.get("id"),
                            "descriptor": selected_item.get("descriptor", {}),
                            "location_ids": selected_item.get("location_ids", []),
                            "category_ids": selected_item.get("category_ids", [])
                        }
                    ],
                    "fulfillments": [
                        {
                            "id": selected_fulfillment.get("id", "fulfillment_001"),
                            "start": {
                                "location": {
                                    "id": selected_location.get("id"),
                                    "gps": selected_location.get("gps"),
                                    "address": selected_location.get("address", {})
                                }
                            }
                        }
                    ],
                    "authorization": {
                        "token": "USER_AUTH_TOKEN_123"
                    }
                }
            }
        }

        logger.info("Beckn Select Request:")
        logger.info(json.dumps(select_request, indent=2))

        # Process select request using existing init module
        select_response = handle_beckn_select_request(
            select_request,
            self.ocpi_base_url,
            self.ocpi_token,
            mock_mode=self.mock_mode
        )

        # Extract session ID for future use
        self.session_id = select_response.get(
            'message', {}).get('order', {}).get('id')

        logger.info("Beckn Select Response:")
        logger.info(f"Session ID: {self.session_id}")
        logger.info(
            f"Session Status: {select_response.get('message', {}).get('order', {}).get('state', 'Unknown')}")

        return select_response

    def step_3_confirm_charging_session(self) -> Dict[str, Any]:
        """
        Step 3: Confirm the charging session

        Flow: Beckn Confirm Request → OCPI Session Confirmation → Beckn Confirm Response
        """
        logger.info("\n" + "="*60)
        logger.info("STEP 3: CONFIRMING CHARGING SESSION")
        logger.info("="*60)

        if not self.session_id:
            raise ValueError(
                "No session ID available. Please complete select step first.")

        # Create Beckn confirm request
        confirm_request = {
            "context": {
                **self.base_context,
                "action": "confirm",
                "message_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            "message": {
                "order": {
                    "id": self.session_id
                }
            }
        }

        logger.info("Beckn Confirm Request:")
        logger.info(json.dumps(confirm_request, indent=2))

        # Process confirm request using existing confirm module
        confirm_response = handle_beckn_confirm_request(
            confirm_request,
            self.ocpi_base_url,
            self.ocpi_token,
            mock_mode=self.mock_mode
        )

        logger.info("Beckn Confirm Response:")
        logger.info(
            f"Session ID: {confirm_response.get('message', {}).get('order', {}).get('id')}")
        logger.info(
            f"Session Status: {confirm_response.get('message', {}).get('order', {}).get('state')}")
        logger.info(
            f"Confirmation Time: {confirm_response.get('message', {}).get('order', {}).get('confirmation_time')}")

        return confirm_response

    def step_4_check_session_status(self) -> Dict[str, Any]:
        """
        Step 4: Check charging session status

        Flow: Beckn Status Request → OCPI Session Status → Beckn Status Response
        """
        logger.info("\n" + "="*60)
        logger.info("STEP 4: CHECKING SESSION STATUS")
        logger.info("="*60)

        if not self.session_id:
            raise ValueError(
                "No session ID available. Please complete select step first.")

        # Create Beckn status request
        status_request = {
            "context": {
                **self.base_context,
                "action": "status",
                "message_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            "message": {
                "order": {
                    "id": self.session_id
                }
            }
        }

        logger.info("Beckn Status Request:")
        logger.info(json.dumps(status_request, indent=2))

        # Process status request using existing status module
        status_response = handle_beckn_status_request(
            status_request,
            self.ocpi_base_url,
            self.ocpi_token,
            mock_mode=self.mock_mode
        )

        logger.info("Beckn Status Response:")
        logger.info(
            f"Session ID: {status_response.get('message', {}).get('order', {}).get('id')}")
        logger.info(
            f"Session Status: {status_response.get('message', {}).get('order', {}).get('state')}")
        logger.info(
            f"Energy Consumed: {status_response.get('message', {}).get('order', {}).get('kwh', 0)} kWh")
        logger.info(
            f"Last Updated: {status_response.get('message', {}).get('order', {}).get('last_updated')}")

        return status_response

    def step_5_stop_charging_session(self) -> Dict[str, Any]:
        """
        Step 5: Stop the charging session

        Flow: Beckn Update Request → OCPI Session Update → Beckn Update Response
        """
        logger.info("\n" + "="*60)
        logger.info("STEP 5: STOPPING CHARGING SESSION")
        logger.info("="*60)

        if not self.session_id:
            raise ValueError(
                "No session ID available. Please complete select step first.")

        # Create Beckn update (stop) request with final charging data
        update_request = {
            "context": {
                **self.base_context,
                "action": "update",
                "message_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            "message": {
                "order": {
                    "id": self.session_id,
                    "update": {
                        "status": "STOPPED",
                        "reason": "User requested session termination",
                        "final_kwh": 25.5,
                        "end_datetime": datetime.now(timezone.utc).isoformat()
                    }
                }
            }
        }

        logger.info("Beckn Update (Stop) Request:")
        logger.info(json.dumps(update_request, indent=2))

        # Process update request using existing update module (with CDR generation)
        update_response = handle_stop_charging_session(
            json.dumps(update_request),
            self.ocpi_base_url,
            self.ocpi_token,
            mock_mode=self.mock_mode,
            generate_cdr=True
        )

        logger.info("Beckn Update Response:")
        logger.info(
            f"Session ID: {update_response.get('message', {}).get('order', {}).get('id')}")
        logger.info(
            f"Session Status: {update_response.get('message', {}).get('order', {}).get('state')}")
        logger.info(
            f"Last Updated: {update_response.get('message', {}).get('order', {}).get('last_updated')}")

        # Check if CDR was generated
        cdr_info = update_response.get(
            'message', {}).get('order', {}).get('cdr', {})
        if cdr_info:
            self.cdr_id = cdr_info.get('id')
            logger.info(f"CDR Generated: {self.cdr_id}")
            logger.info(f"CDR Status: {cdr_info.get('status')}")
            logger.info(f"CDR Push Status: {cdr_info.get('push_status')}")
            logger.info(f"Total Cost: {cdr_info.get('total_cost', {})}")
            logger.info(f"Total Energy: {cdr_info.get('total_energy', 0)} kWh")

        return update_response

    def step_6_generate_detailed_cdr(self) -> Dict[str, Any]:
        """
        Step 6: Generate detailed CDR for billing and settlement

        Flow: Beckn CDR Request → OCPI CDR Generation → Beckn CDR Response
        """
        logger.info("\n" + "="*60)
        logger.info("STEP 6: GENERATING DETAILED CDR")
        logger.info("="*60)

        if not self.session_id:
            raise ValueError(
                "No session ID available. Please complete select step first.")

        # Create Beckn CDR request
        cdr_request = {
            "context": {
                **self.base_context,
                "action": "cdr",
                "message_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat()
            },
            "message": {
                "order": {
                    "id": self.session_id,
                    "billing": {
                        "email": "user@example.com",
                        "phone": "+91-9876543210",
                        "name": "John Doe",
                        "address": "123 User Street, Bangalore, Karnataka"
                    },
                    "payment": {
                        "method": "UPI",
                        "reference": "UPI_REF_123",
                        "status": "COMPLETED"
                    }
                }
            }
        }

        logger.info("Beckn CDR Request:")
        logger.info(json.dumps(cdr_request, indent=2))

        # Process CDR request using update module
        cdr_response = handle_beckn_cdr_request(
            json.dumps(cdr_request),
            self.ocpi_base_url,
            self.ocpi_token,
            mock_mode=self.mock_mode
        )

        # Extract CDR details
        cdr_details = cdr_response.get(
            'message', {}).get('order', {}).get('cdr', {})

        logger.info("Beckn CDR Response:")
        logger.info(f"CDR ID: {cdr_details.get('id')}")
        logger.info(f"Session ID: {cdr_details.get('session_id')}")
        logger.info(
            f"Energy Consumed: {cdr_details.get('energy', {}).get('consumed', 0)} kWh")
        logger.info(
            f"Total Duration: {cdr_details.get('duration', {}).get('total_time', 0)} hours")

        cost_info = cdr_details.get('cost', {})
        logger.info(f"Currency: {cost_info.get('currency', 'INR')}")
        logger.info(f"Total Amount: {cost_info.get('total_amount', 0)}")
        logger.info(f"Energy Cost: {cost_info.get('energy_cost', 0)}")

        breakdown = cost_info.get('breakdown', {})
        logger.info(f"Base Amount: {breakdown.get('base_amount', 0)}")
        logger.info(f"Tax Amount: {breakdown.get('tax_amount', 0)}")
        logger.info(f"Tax Rate: {breakdown.get('tax_rate', 0)}%")

        payment_info = cdr_details.get('payment', {})
        logger.info(f"Payment Method: {payment_info.get('method', 'Unknown')}")
        logger.info(f"Invoice ID: {payment_info.get('invoice_id', 'N/A')}")

        return cdr_response

    def run_complete_session(self, user_location: str = "12.9716,77.5946") -> Dict[str, Any]:
        """
        Run the complete end-to-end charging session flow including CDR generation
        """
        logger.info("\n" + "="*30)
        logger.info("STARTING COMPLETE EV CHARGING SESSION FLOW WITH CDR")
        logger.info("="*30)

        session_results = {}

        try:
            # Step 1: Search for charging stations
            session_results['search'] = self.step_1_search_charging_stations(
                user_location)

            # Step 2: Select a charging station
            session_results['select'] = self.step_2_select_charging_station(
                session_results['search'])

            # Step 3: Confirm the session
            session_results['confirm'] = self.step_3_confirm_charging_session()

            # Step 4: Check session status
            session_results['status'] = self.step_4_check_session_status()

            # Step 5: Stop the session (with automatic CDR generation)
            session_results['stop'] = self.step_5_stop_charging_session()

            # Step 6: Generate detailed CDR for billing
            session_results['cdr'] = self.step_6_generate_detailed_cdr()

            logger.info("\n" + "="*30)
            logger.info("COMPLETE SESSION FLOW WITH CDR SUCCESSFULLY EXECUTED")
            logger.info("="*30)

            return session_results

        except Exception as e:
            logger.error(f"Error in session flow: {str(e)}")
            raise

    def print_session_summary(self, session_results: Dict[str, Any]):
        """
        Print a summary of the complete session including CDR information
        """
        logger.info("\n" + "="*30)
        logger.info("SESSION SUMMARY")
        logger.info("="*30)

        logger.info(f"Transaction ID: {self.transaction_id}")
        logger.info(f"Session ID: {self.session_id}")
        logger.info(f"CDR ID: {self.cdr_id}")

        # Extract key metrics from responses
        search_count = len(session_results.get('search', {}).get(
            'message', {}).get('catalog', {}).get('items', []))
        logger.info(f"Charging Stations Found: {search_count}")

        final_status = session_results.get('stop', {}).get(
            'message', {}).get('order', {}).get('state', 'Unknown')
        logger.info(f"Final Session Status: {final_status}")

        # CDR Summary
        cdr_details = session_results.get('cdr', {}).get(
            'message', {}).get('order', {}).get('cdr', {})
        if cdr_details:
            logger.info(
                f"Energy Consumed: {cdr_details.get('energy', {}).get('consumed', 0)} kWh")
            logger.info(
                f"Total Cost: {cdr_details.get('cost', {}).get('total_amount', 0)} {cdr_details.get('cost', {}).get('currency', 'INR')}")
            logger.info(
                f"Session Duration: {cdr_details.get('duration', {}).get('total_time', 0)} hours")

        # Show OCPI transformations count
        logger.info(
            f"OCPI Transformations: 6 (Search, Select, Confirm, Status, Update, CDR)")
        logger.info(f"OCPI Modules Used: Locations, Sessions, Tariffs, CDRs")

        logger.info("="*30)

    def demonstrate_ocpi_modules_usage(self):
        """
        Demonstrate direct usage of OCPI modules for advanced scenarios
        """
        logger.info("\n" + "="*50)
        logger.info("DEMONSTRATING DIRECT OCPI MODULE USAGE")
        logger.info("="*50)

        # Initialize OCPI client directly
        ocpi_client = OCPIClient(
            base_url=self.ocpi_base_url,
            token=self.ocpi_token,
            mock_mode=self.mock_mode
        )

        # 1. Direct location query
        logger.info("\n1. Direct OCPI Locations Query:")
        locations_response = ocpi_client.get_locations(limit=3)
        logger.info(
            f"Found {len(locations_response.get('data', []))} locations via direct OCPI call")

        # 2. Direct CDR retrieval
        if self.cdr_id:
            logger.info(
                f"\n2. Direct OCPI CDR Retrieval for CDR: {self.cdr_id}")
            cdr_data = ocpi_client.get_cdr(self.cdr_id)
            logger.info(f"Retrieved CDR: {cdr_data.get('id', 'Unknown')}")
            logger.info(f"Total Energy: {cdr_data.get('total_energy', 0)} kWh")
            logger.info(f"Total Cost: {cdr_data.get('total_cost', {})}")

        # 3. Bridge transformation demo
        logger.info("\n3. Demonstrating Bridge Transformations:")
        bridge = BecknOCPIBridge(ocpi_client)

        # Show tariffs integration
        tariffs = ocpi_client._get_mock_tariffs()
        logger.info(f"Available tariffs: {list(tariffs.keys())}")
        for tariff_id, tariff_info in tariffs.items():
            logger.info(
                f"  {tariff_id}: {tariff_info['price']} {tariff_info['currency']} - {tariff_info['desc']}")

        logger.info("="*50)


def demonstrate_beckn_ocpi_integration():
    """
    Main demonstration function showing the complete integration with all modules
    """
    logger.info("STARTING COMPREHENSIVE BECKN-OCPI INTEGRATION DEMONSTRATION")

    # Initialize session manager
    session_manager = MockChargingSessionManager(
        ocpi_base_url="https://mock-ocpi-cpo.example.com",
        ocpi_token="mock_token_demo_123",
        mock_mode=True
    )

    # Run complete session flow
    session_results = session_manager.run_complete_session(
        user_location="12.9716,77.5946"  # Bangalore coordinates
    )

    # Print session summary
    session_manager.print_session_summary(session_results)

    # Demonstrate direct OCPI module usage
    session_manager.demonstrate_ocpi_modules_usage()

    return session_results


def demonstrate_individual_modules():
    """
    Demonstrate individual module functionality
    """
    logger.info("\n" + "="*60)
    logger.info("DEMONSTRATING INDIVIDUAL MODULE FUNCTIONALITY")
    logger.info("="*60)

    # Test individual modules
    from beckn_modules import BecknSearchRequest, BecknCDRRequest
    from datetime import datetime, timezone
    import uuid

    # 1. Test Beckn modules
    logger.info("\n1. Testing Beckn Modules:")

    search_req_data = {
        "context": {"bap_id": "test", "transaction_id": str(uuid.uuid4())},
        "message": {
            "intent": {
                "fulfillment": {
                    "start": {
                        "location": {"gps": "12.9716,77.5946"}
                    }
                },
                "item": {"category": {"id": "ev_charging"}}
            }
        }
    }

    beckn_search = BecknSearchRequest(**search_req_data)
    location_criteria = beckn_search.get_location_criteria()
    logger.info(f"Extracted location criteria: {location_criteria}")

    # 2. Test OCPI modules
    logger.info("\n2. Testing OCPI Modules:")
    ocpi_client = OCPIClient("https://test.com", "token", mock_mode=True)
    mock_locations = ocpi_client._get_mock_locations_response()
    logger.info(f"Mock locations count: {len(mock_locations['data'])}")

    # 3. Test Bridge
    logger.info("\n3. Testing Beckn-OCPI Bridge:")
    bridge = BecknOCPIBridge(ocpi_client)
    ocpi_query = bridge.transform_beckn_to_ocpi_query(beckn_search)
    logger.info(f"Transformed to OCPI query: {ocpi_query}")

    logger.info("="*60)


if __name__ == "__main__":
    """
    Run the complete end-to-end mock charging session with all modules
    """
    try:
        # Configure logging for main execution
        logging.getLogger().setLevel(logging.INFO)

        # Run comprehensive demonstration
        results = demonstrate_beckn_ocpi_integration()

        # Run individual module demonstrations
        # demonstrate_individual_modules()

        print("\n" + "="*60)
        print("COMPREHENSIVE END-TO-END MOCK SESSION COMPLETED SUCCESSFULLY!")
        print("="*60)
        print("\nSee above logs for detailed step-by-step flow")
        print("Each step shows Beckn → OCPI → Beckn transformations")
        print("CDR generation and billing information included")
        print("All OCPI modules demonstrated: Locations, Sessions, Tariffs, CDRs")
        print("Check concept_note.md for architecture details")

    except Exception as e:
        logger.error(f"Failed to complete session: {str(e)}")
        raise
