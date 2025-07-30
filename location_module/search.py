"""
End-to-End Search Module for Beckn-OCPI Bridge
==============================================

This module demonstrates the complete flow from Beckn search request to filtered OCPI locations
and back to Beckn on_search response using the BecknOCPIBridge.

This module is now simplified and uses the comprehensive BecknOCPIBridge class.

Author: Beckn-OCPI Bridge Team
"""

import logging
import json
import os
from datetime import datetime, timezone
import uuid
from dotenv import load_dotenv

from beckn_modules.beckn_modules import BecknSearchRequest
from beckn_ocpi_bridge import BecknOCPIBridge, OCPILocationClient, create_sample_beckn_search_request

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)


class SearchHandler:
    """Main handler for end-to-end search flow using BecknOCPIBridge"""

    def __init__(self, ocpi_base_url: str, ocpi_token: str):
        """
        Initialize SearchHandler with OCPI configuration.

        Args:
            ocpi_base_url: Base URL of the OCPI server
            ocpi_token: Authentication token for OCPI API
        """
        self.ocpi_client = OCPILocationClient(ocpi_base_url, ocpi_token)
        self.bridge = BecknOCPIBridge(self.ocpi_client)

    def process_search_request(
        self,
        beckn_search_request: BecknSearchRequest,
        search_radius_km: float = 10.0
    ) -> dict:
        """
        Process Beckn search request end-to-end using the bridge.

        Args:
            beckn_search_request: Parsed Beckn search request
            search_radius_km: Search radius in kilometers

        Returns:
            Beckn on_search response
        """
        try:
            logger.info("Processing search request using BecknOCPIBridge...")
            response = self.bridge.process_search_request(
                beckn_search_request, search_radius_km
            )
            logger.info("Search request processed successfully")
            return response
        except Exception as e:
            logger.error(f"Error processing search request: {e}")
            raise


def main():
    """Main function for testing the end-to-end search flow"""
    # Configuration from environment variables
    OCPI_BASE_URL = os.getenv("OCPI_BASE_URL")
    OCPI_TOKEN = os.getenv("OCPI_TOKEN")

    # Validate required environment variables
    if not OCPI_BASE_URL or not OCPI_TOKEN:
        raise ValueError(
            "Missing required environment variables: OCPI_BASE_URL and OCPI_TOKEN must be set in .env file")

    # Create sample search request (Delhi coordinates)
    LAT = 28.5502
    LONG = 77.2583
    sample_request = create_sample_beckn_search_request(
        latitude=LAT,
        longitude=LONG,
        radius_km=5.0
    )

    # Initialize handler
    handler = SearchHandler(OCPI_BASE_URL, OCPI_TOKEN)

    # Process search request
    print("Processing Beckn search request...")
    print(f"Search location: {LAT}, {LONG}")
    print(f"Search radius: 5.0 km")
    print("-" * 50)

    try:
        response = handler.process_search_request(
            sample_request, search_radius_km=5.0)

        # Print results
        catalog = response.get('message', {}).get('catalog', {})
        locations = catalog.get('locations', [])
        items = catalog.get('items', [])

        print("Beckn Response:")
        print(json.dumps(response, indent=2, ensure_ascii=False))
        print(f"Search completed successfully!")
        print(f"Locations found: {len(locations)}")
        print(f"Charging connectors found: {len(items)}")
        print("-" * 50)

        # Show first few results
        if locations:
            print("Top 3 nearest locations:")
            for i, loc in enumerate(locations[:3], 1):
                print(f"{i}. {loc.get('descriptor', {}).get('name', 'Unknown')}")
                print(
                    f"   Address: {loc.get('address', {}).get('full', 'Unknown')}")
                print(f"   Distance: {loc.get('distance', 'Unknown')}")
                print()

        if items:
            print("Available charging connectors:")
            for i, item in enumerate(items[:5], 1):
                desc = item.get('descriptor', {})
                tags = item.get('tags', {})
                print(f"{i}. {desc.get('name', 'Unknown')}")
                print(f"   Power: {tags.get('max_power', 'Unknown')}kW")
                print(f"   Type: {tags.get('connector_type', 'Unknown')}")
                print(f"   Status: {tags.get('availability', 'Unknown')}")
                print()

    except Exception as e:
        print(f"Error: {e}")
        logger.error(f"Error in main: {e}")


if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    main()
