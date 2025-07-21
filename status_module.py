import json
import logging
from dataclasses import dataclass
from typing import Dict, Any, Optional

from beckn_modules import BecknStatusRequest
from ocpi_modules import OCPIClient
from beckn_ocpi_bridge import BecknOCPIBridge

logger = logging.getLogger(__name__)


def handle_beckn_status_request(beckn_status_json: dict, ocpi_base_url: str, ocpi_token: str, mock_mode: bool = True):
    # Parse Beckn status request
    beckn_status_request = BecknStatusRequest(
        context=beckn_status_json.get("context", {}),
        message=beckn_status_json.get("message", {})
    )

    # Initialize OCPI client and bridge
    ocpi_client = OCPIClient(base_url=ocpi_base_url,
                             token=ocpi_token, mock_mode=mock_mode)
    bridge = BecknOCPIBridge(ocpi_client)

    # Convert Beckn status to OCPI status params
    ocpi_status_params = bridge.transform_beckn_status_to_ocpi(
        beckn_status_request)
    session_id = ocpi_status_params["session_id"]

    # Call OCPI to get session status
    ocpi_status_response = ocpi_client.get_session_status(session_id)

    # Convert OCPI status response to Beckn on_status
    beckn_on_status_response = bridge.transform_ocpi_status_to_beckn_on_status(
        ocpi_status_response, beckn_status_request
    )

    return beckn_on_status_response


if __name__ == "__main__":
    # Example Beckn status request JSON
    beckn_status_json = {
        "context": {
            "domain": "mobility",
            "action": "status",
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
    response = handle_beckn_status_request(
        beckn_status_json, ocpi_base_url, ocpi_token, mock_mode=True)
    print(json.dumps(response, indent=2))
