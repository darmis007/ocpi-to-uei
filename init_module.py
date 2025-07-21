import logging
from beckn_modules import BecknSelectRequest
from ocpi_modules import OCPIClient
from beckn_ocpi_bridge import BecknOCPIBridge

logger = logging.getLogger(__name__)


def handle_beckn_select_request(beckn_select_payload: dict, ocpi_base_url: str, ocpi_token: str, mock_mode: bool = False):
    # Parse Beckn select request
    beckn_select_request = BecknSelectRequest(**beckn_select_payload)

    # Initialize OCPI client and bridge
    ocpi_client = OCPIClient(base_url=ocpi_base_url,
                             token=ocpi_token, mock_mode=mock_mode)
    bridge = BecknOCPIBridge(ocpi_client)

    # Transform Beckn select to OCPI session request
    ocpi_session_request = bridge.transform_beckn_select_to_ocpi_session(
        beckn_select_request)

    # Initiate OCPI session
    ocpi_session_response = ocpi_client.initiate_session(
        **ocpi_session_request)

    # Convert OCPI session response to Beckn on_select response
    beckn_on_select_response = bridge.transform_ocpi_session_to_beckn_on_select(
        ocpi_session_response, beckn_select_request
    )
    return beckn_on_select_response


# Example usage:
if __name__ == "__main__":
    import json
    # Example Beckn select request payload (replace with actual payload)
    with open("sample_beckn_select.json") as f:
        beckn_select_payload = json.load(f)
    ocpi_base_url = "https://ocpi.example.com"
    ocpi_token = "your-ocpi-token"
    response = handle_beckn_select_request(
        beckn_select_payload, ocpi_base_url, ocpi_token, mock_mode=True)
    print(json.dumps(response, indent=2))
