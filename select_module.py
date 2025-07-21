import json
from datetime import datetime, timezone
import uuid

from beckn_modules import BecknSearchRequest  # Beckn-specific
from ocpi_modules import OCPIClient           # OCPI-specific
from beckn_ocpi_bridge import BecknOCPIBridge  # Bridge logic


def process_beckn_select_request(beckn_select_data, ocpi_base_url, ocpi_token, mock_mode=False):
    """
    Process a Beckn select request, fetch selected OCPI locations, and return a Beckn on_select response.
    """
    # 1. Parse the Beckn select request
    context = beckn_select_data.get('context', {})
    message = beckn_select_data.get('message', {})
    selected_item_ids = []
    selected_location_ids = []

    # 2. Extract selected item/location IDs from the select request
    # Beckn select request usually has message['order']['items'] or similar
    order = message.get('order', {})
    items = order.get('items', [])
    for item in items:
        # item['location_ids'] is a list of location IDs associated with the item
        if 'location_ids' in item:
            selected_location_ids.extend(item['location_ids'])
        if 'id' in item:
            selected_item_ids.append(item['id'])

    # Remove duplicates
    selected_location_ids = list(set(selected_location_ids))

    # 3. Initialize OCPI client and bridge
    ocpi_client = OCPIClient(ocpi_base_url, ocpi_token, mock_mode=mock_mode)
    bridge = BecknOCPIBridge(ocpi_client)

    # 4. Fetch OCPI locations for each selected location_id
    ocpi_locations = []
    for loc_id in selected_location_ids:
        ocpi_response = ocpi_client.get_locations(location_id=loc_id)
        ocpi_locations.extend(ocpi_response.get('data', []))

    # 5. Optionally, fetch tariffs if needed (for mock mode)
    tariffs = ocpi_client._get_mock_tariffs() if mock_mode else {}

    # 6. Build a minimal BecknSearchRequest for context (for transformation)
    beckn_request = BecknSearchRequest(context=context, message=message)

    # 7. Transform OCPI locations to Beckn on_select response
    ocpi_response = {'data': ocpi_locations}
    beckn_on_select_response = bridge.transform_ocpi_to_beckn_response(
        ocpi_response, beckn_request, tariffs
    )

    # 8. Adjust the context for on_select
    beckn_on_select_response['context']['action'] = 'on_select'
    beckn_on_select_response['context']['timestamp'] = datetime.now(
        timezone.utc).isoformat()
    beckn_on_select_response['context']['message_id'] = str(uuid.uuid4())

    return beckn_on_select_response


# Example usage
if __name__ == "__main__":
    # Sample Beckn select request (structure may vary based on your implementation)
    sample_select_request = {
        "context": {
            "domain": "mobility:ev_charging",
            "country": "IND",
            "city": "BLR",
            "action": "select",
            "core_version": "1.1.0",
            "bap_id": "example_bap.com",
            "bap_uri": "https://example_bap.com/",
            "transaction_id": str(uuid.uuid4()),
            "message_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ttl": "PT30S"
        },
        "message": {
            "order": {
                "items": [
                    {
                        "id": "item_LOC001_evse_EVSE001_conn_1",
                        "location_ids": ["LOC001"]
                    }
                ]
            }
        }
    }

    OCPI_BASE_URL = "https://example-cpo.com/ocpi"
    OCPI_TOKEN = "your_ocpi_token_here"
    MOCK_MODE = True

    response = process_beckn_select_request(
        sample_select_request,
        OCPI_BASE_URL,
        OCPI_TOKEN,
        mock_mode=MOCK_MODE
    )

    print("Beckn on_select Response:")
    print(json.dumps(response, indent=2))
