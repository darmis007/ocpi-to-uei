import json
import logging
from dataclasses import dataclass
from typing import Dict, Any, Optional

from beckn_modules import BecknConfirmRequest
from ocpi_modules import OCPIClient
from beckn_ocpi_bridge import BecknOCPIBridge

logger = logging.getLogger(__name__)


def handle_beckn_confirm_request(beckn_confirm_json: dict, ocpi_base_url: str, ocpi_token: str, mock_mode: bool = True):
    # Parse Beckn confirm request
    beckn_confirm_request = BecknConfirmRequest(
        context=beckn_confirm_json.get("context", {}),
        message=beckn_confirm_json.get("message", {})
    )

    # Initialize OCPI client and bridge
    ocpi_client = OCPIClient(base_url=ocpi_base_url,
                             token=ocpi_token, mock_mode=mock_mode)
    bridge = BecknOCPIBridge(ocpi_client)

    # Convert Beckn confirm to OCPI confirm params
    ocpi_confirm_params = bridge.transform_beckn_confirm_to_ocpi(
        beckn_confirm_request)
    session_id = ocpi_confirm_params["session_id"]

    # Call OCPI to confirm session
    ocpi_confirm_response = ocpi_client.confirm_session(session_id)

    # Convert OCPI confirm response to Beckn on_confirm
    beckn_on_confirm_response = bridge.transform_ocpi_confirm_to_beckn_on_confirm(
        ocpi_confirm_response, beckn_confirm_request
    )

    return beckn_on_confirm_response


# Example usage
if __name__ == "__main__":
    # Example Beckn confirm request JSON
    beckn_confirm_json = {
        "context": {
            "domain": "mobility",
            "action": "confirm",
            "bap_id": "sample_bap",
            "bpp_id": "sample_bpp",
            "transaction_id": "txn123",
            "message_id": "msg123"
        },
        "message": {
            "order": {
                "id": "SESSION123"
            }
        }
    }
    ocpi_base_url = "https://ocpi.example.com"
    ocpi_token = "testtoken"
    response = handle_beckn_confirm_request(
        beckn_confirm_json, ocpi_base_url, ocpi_token, mock_mode=True)
    print(json.dumps(response, indent=2))
