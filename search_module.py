import json
import logging
from typing import Dict,Any
from datetime import datetime, timezone
import uuid
from enum import Enum
from beckn_ocpi_bridge import BecknOCPIBridge
from ocpi_modules import  OCPIClient
from beckn_modules import BecknSearchRequest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_beckn_search_request(beckn_request_data: Dict[str, Any], 
                               ocpi_base_url: str, 
                               ocpi_token: str,
                               mock_mode: bool = False) -> Dict[str, Any]:
    """
    Main function to process Beckn search request and return response
    
    Args:
        beckn_request_data: Raw Beckn search request JSON
        ocpi_base_url: OCPI API base URL
        ocpi_token: OCPI API authentication token
        mock_mode: If True, use mock OCPI response instead of calling real API
        
    Returns:
        Beckn search response
    """
    try:
        # Parse Beckn request
        beckn_request = BecknSearchRequest(
            context=beckn_request_data['context'],
            message=beckn_request_data['message']
        )
        
        logger.info(f"Processing Beckn search request: {beckn_request.context.get('message_id')}")
        
        # Initialize OCPI client with mock mode
        ocpi_client = OCPIClient(ocpi_base_url, ocpi_token, mock_mode=mock_mode)
        
        # Initialize bridge
        bridge = BecknOCPIBridge(ocpi_client)
        
        # Transform Beckn request to OCPI query
        ocpi_query_params = bridge.transform_beckn_to_ocpi_query(beckn_request)
        
        # Get tariffs (mock or real)
        tariffs = {}
        if mock_mode:
            tariffs = ocpi_client._get_mock_tariffs()
        # Get locations from OCPI (or mock)
        ocpi_response = ocpi_client.get_locations(**ocpi_query_params)
        logger.info(f"Received {len(ocpi_response.get('data', []))} locations from OCPI")
        # Transform OCPI response to Beckn format, passing tariffs
        beckn_response = bridge.transform_ocpi_to_beckn_response(ocpi_response, beckn_request, tariffs)
        
        logger.info(f"Generated Beckn response with {len(beckn_response['message']['catalog'].get('items', []))} items")
        
        return beckn_response
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"Error processing Beckn search request: {str(e)}")
        
        # Safely extract context information for error response
        context_info = {}
        if isinstance(beckn_request_data, dict) and 'context' in beckn_request_data:
            context_info = beckn_request_data['context']
        elif isinstance(beckn_request_data, dict):
            # Try to extract basic context info if available
            context_info = {
                k: v for k, v in beckn_request_data.items() 
                if k in ['domain', 'country', 'city', 'bap_id', 'transaction_id']
            }
        
        # Return error response in Beckn format
        error_response = {
            "context": {
                **context_info,
                "action": "on_search",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message_id": str(uuid.uuid4())
            },
            "error": {
                "type": "INTERNAL_ERROR",
                "code": "500",
                "path": "process_search",
                "message": str(e)
            }
        }
        return error_response

# Example usage and testing
if __name__ == "__main__":
    # Sample Beckn search request
    sample_beckn_request = {
        "context": {
            "domain": "mobility:ev_charging",
            "country": "IND",
            "city": "BLR",
            "action": "search",
            "core_version": "1.1.0",
            "bap_id": "example_bap.com",
            "bap_uri": "https://example_bap.com/",
            "transaction_id": str(uuid.uuid4()),
            "message_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ttl": "PT30S"
        },
        "message": {
            "intent": {
                "fulfillment": {
                    "start": {
                        "location": {
                            "gps": "12.9716,77.5946",  # Bangalore coordinates
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
    
    # Configuration (replace with actual OCPI endpoint details)
    OCPI_BASE_URL = "https://example-cpo.com/ocpi"  # Fixed URL
    OCPI_TOKEN = "your_ocpi_token_here"
    MOCK_MODE = True  # Set to False to use real OCPI API
    
    try:
        # Process the request
        response = process_beckn_search_request(
            sample_beckn_request, 
            OCPI_BASE_URL, 
            OCPI_TOKEN,
            mock_mode=MOCK_MODE
        )
        
        # Pretty print the response
        print("Beckn Search Response:")
        print(json.dumps(response, indent=2))
        
    except Exception as e:
        print(f"Error: {str(e)}")
