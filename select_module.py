"""
Select Module for Beckn-OCPI Bridge
==================================

This module constructs a Beckn select request based on the on_search response.
It extracts relevant information from the search response to create a proper select request.

Author: Beckn-OCPI Bridge Team
"""

import json
import os
from datetime import datetime, timezone
import uuid
from typing import Dict, Any
from beckn_ocpi_bridge import BecknOCPIBridge, OCPILocationClient
from dotenv import load_dotenv

load_dotenv()


def create_select_request_from_search_response(search_response: Dict[Any, Any]) -> Dict[Any, Any]:
    """
    Create a Beckn select request based on the on_search response.

    Args:
        search_response: The on_search response dictionary

    Returns:
        Beckn select request dictionary
    """
    # Extract context from search response
    context = search_response.get('context', {})

    # Create new context for select request
    select_context = {
        "domain": context.get("domain", "uei:ev_charging"),
        "action": "select",
        "location": context.get("location", {
            "country": {"code": "IND"},
            "city": {"code": "std:080"}
        }),
        "version": context.get("version", "1.1.0"),
        "bap_id": context.get("bap_id", "example-bap.com"),
        "bap_uri": context.get("bap_uri", "https://api.example-bap.com/pilot/bap/energy/v1"),
        "bpp_id": context.get("bpp_id", "example-bpp.com"),
        "bpp_uri": context.get("bpp_uri", "https://api.example-bpp.com/pilot/bpp/"),
        "transaction_id": context.get("transaction_id", str(uuid.uuid4())),
        "message_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    }

    # Extract catalog information
    catalog = search_response.get('message', {}).get('catalog', {})
    providers = catalog.get('providers', [])

    if not providers:
        raise ValueError("No providers found in search response")

    # Use the first provider for the select request
    provider = providers[0]
    provider_id = provider.get('id', 'chargezone.in')

    # Extract fulfillments from the provider
    fulfillments = provider.get('fulfillments', [])
    if not fulfillments:
        raise ValueError("No fulfillments found in provider")

    # Select the first fulfillment (CHARGING type)
    selected_fulfillment = fulfillments[0]
    fulfillment_id = selected_fulfillment.get('id', '1')
    fulfillment_type = selected_fulfillment.get('type', 'CHARGING')

    # Extract items from the provider
    items = provider.get('items', [])
    if not items:
        raise ValueError("No items found in provider")

    # Use the first item for the select request
    item = items[0]
    item_id = item.get('id', 'pe-charging-01')

    # Check if this item supports the selected fulfillment
    item_fulfillment_ids = item.get('fulfillment_ids', [])
    if fulfillment_id not in item_fulfillment_ids:
        # If the item doesn't support this fulfillment, try to find a compatible one
        for fulfillment in fulfillments:
            if fulfillment.get('id') in item_fulfillment_ids:
                selected_fulfillment = fulfillment
                fulfillment_id = fulfillment.get('id')
                fulfillment_type = fulfillment.get('type')
                break

    # Extract add-ons from the item
    add_ons = item.get('add_ons', [])
    selected_add_ons = []
    if add_ons:
        # Use the first add-on
        add_on = add_ons[0]
        selected_add_ons.append({
            "id": add_on.get('id', 'pe-charging-01-addon-1')
        })

    # Create the order message with proper fulfillment selection
    order_message = {
        "order": {
            "provider": {
                "id": provider_id
            },
            "items": [
                {
                    "id": item_id,
                    "quantity": {
                        "selected": {
                            "measure": {
                                "value": "4",
                                "unit": "kWh"
                            }
                        }
                    },
                    "add_ons": selected_add_ons
                }
            ],
            "fulfillments": [
                {
                    "id": fulfillment_id,
                    "type": fulfillment_type
                }
            ]
        }
    }

    # Construct the complete select request
    select_request = {
        "context": select_context,
        "message": order_message
    }

    return select_request


def save_select_request(select_request: Dict[Any, Any], filename: str = None) -> str:
    """
    Save the select request to a JSON file in the requests/ folder.

    Args:
        select_request: The select request dictionary
        filename: Optional filename, if not provided will generate one with timestamp

    Returns:
        Path to the saved file
    """
    if filename is None:
        filename = f"requests/select_request.json"

    # Ensure requests directory exists
    os.makedirs("requests", exist_ok=True)

    # Save to file
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(select_request, f, indent=2, ensure_ascii=False)

    return filename


def save_select_response(select_response: Dict[Any, Any], filename: str = None) -> str:
    """
    Save the select response to a JSON file in the responses/ folder.

    Args:
        select_response: The select response dictionary
        filename: Optional filename, if not provided will generate one with timestamp

    Returns:
        Path to the saved file
    """
    if filename is None:
        filename = f"responses/on_select.json"

    # Ensure responses directory exists
    os.makedirs("responses", exist_ok=True)

    # Save to file
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(select_response, f, indent=2, ensure_ascii=False)

    return filename


def process_select_request_with_bridge(select_request: Dict[Any, Any], ocpi_base_url: str = None, ocpi_token: str = None) -> Dict[Any, Any]:
    """
    Process the select request using the BecknOCPIBridge.

    Args:
        select_request: The select request dictionary
        ocpi_base_url: OCPI base URL (optional, will use default if not provided)
        ocpi_token: OCPI token (optional, will use default if not provided)

    Returns:
        The processed select response
    """
    # Create a mock select request object that mimics the structure expected by the bridge
    class MockSelectRequest:
        def __init__(self, context, message):
            self.context = context
            self.message = message

    # Create the mock request object
    mock_select_request = MockSelectRequest(
        context=select_request.get('context', {}),
        message=select_request.get('message', {})
    )

    # Initialize OCPI client and bridge
    if ocpi_base_url and ocpi_token:
        ocpi_client = OCPILocationClient(ocpi_base_url, ocpi_token)
    else:
        # Use default values or create a mock client
        ocpi_client = OCPILocationClient(
            "https://mock-ocpi-server.com", "mock-token")

    bridge = BecknOCPIBridge(ocpi_client)

    # Process the select request
    select_response = bridge.process_select_request(mock_select_request)

    return select_response


def main():
    """Main function to demonstrate creating select request from search response and processing it"""

    # Load the on_search response
    try:
        with open('responses/search_response.json', 'r', encoding='utf-8') as f:
            search_response = json.load(f)
    except FileNotFoundError:
        print("Error: responses/search_response.json not found")
        return
    except json.JSONDecodeError:
        print("Error: Invalid JSON in responses/search_response.json")
        return

    try:
        # Create select request
        select_request = create_select_request_from_search_response(
            search_response)

        # Save the select request
        select_request_filename = save_select_request(select_request)

        print("Select request created successfully!")
        print(f"Saved to: {select_request_filename}")
        print("\nSelect Request:")
        print(json.dumps(select_request, indent=2, ensure_ascii=False))

        # Process the select request using the bridge
        print("\nProcessing select request with BecknOCPIBridge...")

        select_response = process_select_request_with_bridge(
            select_request, os.getenv("OCPI_BASE_URL"), os.getenv("OCPI_TOKEN"))

        # Save the select response
        select_response_filename = save_select_response(select_response)

        print("Select response processed successfully!")
        print(f"Saved to: {select_response_filename}")
        print("\nSelect Response:")
        print(json.dumps(select_response, indent=2, ensure_ascii=False))

        # Print summary
        context = select_request.get('context', {})
        order = select_request.get('message', {}).get('order', {})
        provider = order.get('provider', {})
        items = order.get('items', [])

        print(f"\nSummary:")
        print(f"Provider: {provider.get('id', 'Unknown')}")
        print(f"Items selected: {len(items)}")
        if items:
            item = items[0]
            print(f"Item ID: {item.get('id', 'Unknown')}")
            print(f"Quantity: {item.get('quantity', {}).get('selected', {}).get('measure', {}).get('value', 'Unknown')} {item.get('quantity', {}).get('selected', {}).get('measure', {}).get('unit', 'Unknown')}")

        # Print response summary
        response_context = select_response.get('context', {})
        response_order = select_response.get('message', {}).get('order', {})
        response_provider = response_order.get('provider', {})
        response_items = response_order.get('items', [])
        response_quote = response_order.get('quote', {})

        print(f"\nResponse Summary:")
        print(f"Provider: {response_provider.get('id', 'Unknown')}")
        print(
            f"Provider Name: {response_provider.get('descriptor', {}).get('name', 'Unknown')}")
        print(f"Items in response: {len(response_items)}")
        print(
            f"Total Price: {response_quote.get('price', {}).get('value', 'Unknown')} {response_quote.get('price', {}).get('currency', 'Unknown')}")

    except Exception as e:
        print(f"Error processing select request: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
