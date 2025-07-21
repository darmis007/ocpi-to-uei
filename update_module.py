import json
import logging

from beckn_modules import BecknUpdateRequest, BecknCDRRequest
from ocpi_modules import OCPIClient
from beckn_ocpi_bridge import BecknOCPIBridge

logger = logging.getLogger(__name__)


def handle_stop_charging_session(beckn_update_json: str, ocpi_base_url: str, ocpi_token: str, mock_mode: bool = True, generate_cdr: bool = True):
    """
    Handles a Beckn update request specifically to stop a charging session.
    Optionally generates and pushes a CDR after stopping the session.
    """
    beckn_update_dict = json.loads(beckn_update_json)
    beckn_update_request = BecknUpdateRequest(
        context=beckn_update_dict.get("context", {}),
        message=beckn_update_dict.get("message", {})
    )

    # Check if the update is a stop request
    update_data = beckn_update_request.get_update_data()
    status = update_data.get("status", "").upper()
    if status not in ["STOPPED", "STOP", "ENDED"]:
        raise ValueError("Update is not a stop charging request")

    # Initialize OCPI client and bridge
    ocpi_client = OCPIClient(base_url=ocpi_base_url,
                             token=ocpi_token, mock_mode=mock_mode)
    bridge = BecknOCPIBridge(ocpi_client)

    # Convert Beckn update to OCPI update params
    ocpi_update_params = bridge.transform_beckn_update_to_ocpi(
        beckn_update_request)
    session_id = ocpi_update_params["session_id"]
    update_data = ocpi_update_params["update_data"]

    # In a real OCPI implementation, you might need to set a specific field to stop the session.
    # For mock, just update status.
    update_data["status"] = "STOPPED"

    # Get session data before stopping (for CDR generation)
    session_data = None
    if generate_cdr:
        try:
            session_data = ocpi_client.get_session_status(session_id)
            # Update session data with final values for CDR
            session_data.update({
                "status": "COMPLETED",
                "end_datetime": update_data.get("end_datetime") or session_data.get("last_updated"),
                "kwh": update_data.get("final_kwh") or session_data.get("kwh", 0)
            })
        except Exception as e:
            logger.warning(
                f"Could not retrieve session data for CDR: {str(e)}")

    # Stop the session
    ocpi_update_response = ocpi_client.update_session(session_id, update_data)

    # Convert OCPI response to Beckn on_update
    beckn_on_update_response = bridge.transform_ocpi_update_to_beckn_on_update(
        ocpi_update_response, beckn_update_request
    )

    # Generate and push CDR if requested and session was successfully stopped
    cdr_response = None
    if generate_cdr and ocpi_update_response.get("status") in ["STOPPED", "COMPLETED"]:
        try:
            logger.info(f"Generating CDR for completed session: {session_id}")

            # Generate CDR
            cdr_data = bridge.generate_session_cdr(session_id, session_data)

            # Push CDR to network
            cdr_push_response = bridge.push_cdr_to_network(cdr_data)

            # Add CDR information to the response
            beckn_on_update_response["message"]["order"]["cdr"] = {
                "id": cdr_data.get("id"),
                "status": "GENERATED",
                "push_status": "SUCCESS" if cdr_push_response.get("status_code") == 1000 else "FAILED",
                "total_cost": cdr_data.get("total_cost", {}),
                "total_energy": cdr_data.get("total_energy", 0),
                "invoice_reference": cdr_data.get("invoice_reference_id")
            }

            logger.info(
                f"CDR generated and pushed successfully: {cdr_data.get('id')}")

        except Exception as e:
            logger.error(f"Failed to generate/push CDR: {str(e)}")
            # Add CDR error to response
            beckn_on_update_response["message"]["order"]["cdr"] = {
                "status": "FAILED",
                "error": str(e)
            }

    return beckn_on_update_response


def handle_beckn_update_request(beckn_update_json: str, ocpi_base_url: str, ocpi_token: str, mock_mode: bool = True):
    """
    Handles a generic Beckn update request, including stop charging if specified.
    """
    beckn_update_dict = json.loads(beckn_update_json)
    update_data = beckn_update_dict.get(
        "message", {}).get("order", {}).get("update", {})
    status = update_data.get("status", "").upper()
    if status in ["STOPPED", "STOP", "ENDED"]:
        return handle_stop_charging_session(beckn_update_json, ocpi_base_url, ocpi_token, mock_mode, generate_cdr=True)
    beckn_update_request = BecknUpdateRequest(
        context=beckn_update_dict.get("context", {}),
        message=beckn_update_dict.get("message", {})
    )

    # Initialize OCPI client and bridge
    ocpi_client = OCPIClient(base_url=ocpi_base_url,
                             token=ocpi_token, mock_mode=mock_mode)
    bridge = BecknOCPIBridge(ocpi_client)

    # Convert Beckn update to OCPI update params
    ocpi_update_params = bridge.transform_beckn_update_to_ocpi(
        beckn_update_request)
    session_id = ocpi_update_params["session_id"]
    update_data = ocpi_update_params["update_data"]

    # Call OCPI session update
    ocpi_update_response = ocpi_client.update_session(session_id, update_data)

    # Convert OCPI response to Beckn on_update
    beckn_on_update_response = bridge.transform_ocpi_update_to_beckn_on_update(
        ocpi_update_response, beckn_update_request)

    return beckn_on_update_response


def handle_beckn_cdr_request(beckn_cdr_json: str, ocpi_base_url: str, ocpi_token: str, mock_mode: bool = True):
    """
    Handle a Beckn CDR request - generate and return billing information.
    """
    beckn_cdr_dict = json.loads(beckn_cdr_json)
    beckn_cdr_request = BecknCDRRequest(
        context=beckn_cdr_dict.get("context", {}),
        message=beckn_cdr_dict.get("message", {})
    )

    # Initialize OCPI client and bridge
    ocpi_client = OCPIClient(base_url=ocpi_base_url,
                             token=ocpi_token, mock_mode=mock_mode)
    bridge = BecknOCPIBridge(ocpi_client)

    # Convert Beckn CDR request to OCPI format
    ocpi_cdr_params = bridge.transform_beckn_cdr_to_ocpi(beckn_cdr_request)
    session_id = ocpi_cdr_params["session_id"]

    # Generate CDR
    cdr_data = bridge.generate_session_cdr(session_id)

    # Push CDR to network
    cdr_push_response = bridge.push_cdr_to_network(cdr_data)

    # Convert OCPI CDR to Beckn response
    beckn_cdr_response = bridge.transform_ocpi_cdr_to_beckn_response(
        cdr_data, beckn_cdr_request, cdr_push_response
    )

    return beckn_cdr_response


# Example usage:
if __name__ == "__main__":
    # Example Beckn update request JSON for stopping a session
    beckn_stop_update_json = json.dumps({
        "context": {
            "domain": "mobility",
            "action": "update",
            "bap_id": "sample_bap",
            "bpp_id": "sample_bpp",
            "transaction_id": "txn123",
            "message_id": "msg123",
            "timestamp": "2024-06-01T12:00:00Z"
        },
        "message": {
            "order": {
                "id": "SESSION123",
                "update": {
                    "status": "STOPPED",
                    "reason": "User requested stop",
                    "final_kwh": 25.5,
                    "end_datetime": "2024-06-01T12:00:00Z"
                }
            }
        }
    })

    # Example Beckn CDR request JSON
    beckn_cdr_request_json = json.dumps({
        "context": {
            "domain": "mobility",
            "action": "cdr",
            "bap_id": "sample_bap",
            "bpp_id": "sample_bpp",
            "transaction_id": "txn123",
            "message_id": "msg124",
            "timestamp": "2024-06-01T12:05:00Z"
        },
        "message": {
            "order": {
                "id": "SESSION123",
                "billing": {
                    "email": "user@example.com",
                    "phone": "+91-9876543210"
                },
                "payment": {
                    "method": "UPI",
                    "reference": "UPI_REF_123"
                }
            }
        }
    })

    ocpi_base_url = "https://ocpi.example.com"
    ocpi_token = "testtoken"

    # Test stop charging with CDR generation
    print("Stop charging response with CDR:")
    response = handle_beckn_update_request(
        beckn_stop_update_json, ocpi_base_url, ocpi_token, mock_mode=True)
    print(json.dumps(response, indent=2))

    print("\n" + "="*50 + "\n")

    # Test CDR request
    print("CDR response:")
    cdr_response = handle_beckn_cdr_request(
        beckn_cdr_request_json, ocpi_base_url, ocpi_token, mock_mode=True)
    print(json.dumps(cdr_response, indent=2))
