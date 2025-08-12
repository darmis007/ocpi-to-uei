"""
Beckn-OCPI Bridge
================

A comprehensive bridge library for integrating OCPI (Open Charge Point Interface) 
charging networks with the Beckn protocol. This library enables OCPI providers to 
easily participate in the Beckn ecosystem.

Features:
- OCPI location and tariff fetching with pagination
- Location filtering by proximity
- Beckn-OCPI protocol transformations
- Complete search, select, confirm, status, update, and CDR flows
- Distance calculations using Haversine formula
- Error handling and logging
- Configurable tariff decomposition toggle via environment variable

Usage:
    from beckn_ocpi_bridge import BecknOCPIBridge, OCPILocationClient
    
    # Set environment variable to control tariff decomposition
    # TARIFF_DECOMPOSITION_ENABLED=true  # Decompose tariffs (default)
    # TARIFF_DECOMPOSITION_ENABLED=false # Pass OCPI response as-is
    
    # Initialize the bridge
    ocpi_client = OCPILocationClient(base_url="https://your-ocpi-server.com", token="your-token")
    bridge = BecknOCPIBridge(ocpi_client)
    
    # Process a Beckn search request (behavior depends on TARIFF_DECOMPOSITION_ENABLED)
    response = bridge.process_search_request(beckn_search_request)

Author: Beckn-OCPI Bridge Team
Version: 1.0.0
"""

import dotenv
import logging
import math
import requests
import json
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any, Tuple
import uuid

logger = logging.getLogger(__name__)

dotenv.load_dotenv()

# Environment variable for tariff decomposition toggle
# Set to 'true' to decompose tariffs and include complete tariff data in responses
# Set to 'false' to pass OCPI response as-is with only tariff IDs
TARIFF_DECOMPOSITION_ENABLED = os.getenv(
    'TARIFF_DECOMPOSITION_ENABLED', 'true').lower() == 'true'


class OCPILocationClient:
    """
    OCPI Location and Tariff Client for fetching locations and tariffs with pagination support.

    This client handles communication with OCPI servers to fetch charging locations
    and tariffs with automatic pagination support.
    """

    def __init__(self, base_url: str, token: str):
        """
        Initialize OCPI Location Client.

        Args:
            base_url: Base URL of the OCPI server
            token: Authentication token for OCPI API
        """
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.headers = {'Authorization': f'Token {token}'}

    def get_all_locations(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch all locations from OCPI with pagination support.

        Args:
            limit: Number of locations per page (default: 100)

        Returns:
            List of all location dictionaries from OCPI

        Raises:
            requests.RequestException: If API calls fail
        """
        all_locations = []
        offset = 0

        while True:
            try:
                url = f"{self.base_url}/ocpi/cpo/2.2.1/locations"
                params = {
                    'limit': limit,
                    'offset': offset
                }

                logger.info(
                    f"Fetching OCPI locations: offset={offset}, limit={limit}")
                response = requests.get(
                    url, headers=self.headers, params=params)
                response.raise_for_status()

                data = response.json()
                locations = data.get('data', [])

                if not locations:
                    logger.info(f"No more locations found at offset {offset}")
                    break

                all_locations.extend(locations)
                logger.info(
                    f"Fetched {len(locations)} locations, total: {len(all_locations)}")

                # Check if we've reached the end
                if len(locations) < limit:
                    break

                offset += limit

            except requests.RequestException as e:
                logger.error(
                    f"Error fetching OCPI locations at offset {offset}: {e}")
                break

        logger.info(f"Total locations fetched: {len(all_locations)}")
        return all_locations

    def get_all_tariffs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch all tariffs from OCPI with pagination support.

        Args:
            limit: Number of tariffs per page (default: 100)

        Returns:
            List of all tariff dictionaries from OCPI

        Raises:
            requests.RequestException: If API calls fail
        """
        all_tariffs = []
        offset = 0

        while True:
            try:
                url = f"{self.base_url}/ocpi/cpo/2.2.1/tariffs"
                params = {
                    'limit': limit,
                    'offset': offset
                }

                logger.info(
                    f"Fetching OCPI tariffs: offset={offset}, limit={limit}")
                response = requests.get(
                    url, headers=self.headers, params=params)
                response.raise_for_status()

                data = response.json()
                tariffs = data.get('data', [])

                if not tariffs:
                    logger.info(f"No more tariffs found at offset {offset}")
                    break

                all_tariffs.extend(tariffs)
                logger.info(
                    f"Fetched {len(tariffs)} tariffs, total: {len(all_tariffs)}")

                # Check if we've reached the end
                if len(tariffs) < limit:
                    break

                offset += limit

            except requests.RequestException as e:
                logger.error(
                    f"Error fetching OCPI tariffs at offset {offset}: {e}")
                break

        logger.info(f"Total tariffs fetched: {len(all_tariffs)}")
        return all_tariffs

    def get_locations_by_area(self, area_code: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch locations filtered by area code.

        Args:
            area_code: Area code to filter by
            limit: Number of locations per page

        Returns:
            List of location dictionaries filtered by area
        """
        all_locations = []
        offset = 0

        while True:
            try:
                url = f"{self.base_url}/ocpi/cpo/2.2.1/locations"
                params = {
                    'limit': limit,
                    'offset': offset,
                    'area_code': area_code
                }

                logger.info(
                    f"Fetching OCPI locations for area {area_code}: offset={offset}, limit={limit}")
                response = requests.get(
                    url, headers=self.headers, params=params)
                response.raise_for_status()

                data = response.json()
                locations = data.get('data', [])

                if not locations:
                    break

                all_locations.extend(locations)

                if len(locations) < limit:
                    break

                offset += limit

            except requests.RequestException as e:
                logger.error(
                    f"Error fetching OCPI locations for area {area_code}: {e}")
                break

        return all_locations


class LocationFilter:
    """
    Location filtering utilities for proximity-based searches.

    Provides methods to filter locations by distance from target coordinates
    using the Haversine formula for accurate distance calculations.
    """

    @staticmethod
    def calculate_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        """
        Calculate distance between two GPS coordinates using Haversine formula.

        Args:
            lat1, lon1: First coordinate (latitude, longitude)
            lat2, lon2: Second coordinate (latitude, longitude)

        Returns:
            Distance in kilometers
        """
        R = 6371  # Earth's radius in kilometers

        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)

        a = (math.sin(delta_lat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

        return R * c

    @staticmethod
    def filter_locations_by_proximity(
        locations: List[Dict[str, Any]],
        target_lat: float,
        target_lon: float,
        radius_km: float = 10.0
    ) -> List[Dict[str, Any]]:
        """
        Filter locations by proximity to target coordinates.

        Args:
            locations: List of OCPI location dictionaries
            target_lat: Target latitude
            target_lon: Target longitude
            radius_km: Search radius in kilometers (default: 10.0)

        Returns:
            Filtered list of locations with distance information, sorted by distance
        """
        filtered_locations = []

        for location in locations:
            coords = location.get('coordinates', {})
            if not coords:
                continue

            try:
                loc_lat = float(coords.get('latitude', 0))
                loc_lon = float(coords.get('longitude', 0))

                # Skip if coordinates are invalid
                if loc_lat == 0 and loc_lon == 0:
                    continue

                distance = LocationFilter.calculate_distance(
                    target_lat, target_lon, loc_lat, loc_lon
                )

                if distance <= radius_km:
                    location['distance_km'] = round(distance, 2)
                    filtered_locations.append(location)

            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Invalid coordinates for location {location.get('id', 'Unknown')}: {e}")
                continue

        # Sort by distance (closest first)
        filtered_locations.sort(
            key=lambda x: x.get('distance_km', float('inf')))

        logger.info(
            f"Filtered {len(filtered_locations)} locations within {radius_km}km radius")
        return filtered_locations


class BecknOCPIBridge:
    """
    Main bridge class for transforming between Beckn and OCPI protocols.

    This class provides comprehensive transformation methods for all Beckn-OCPI
    interactions including search (with location and tariff data), select, confirm, 
    status, update, and CDR flows.
    """

    def __init__(self, ocpi_client: Optional[OCPILocationClient] = None):
        """
        Initialize the Beckn-OCPI Bridge.

        Args:
            ocpi_client: OCPI client instance (optional, can be set later)
        """
        self.ocpi_client = ocpi_client

    def set_ocpi_client(self, ocpi_client: OCPILocationClient):
        """Set the OCPI client for the bridge."""
        self.ocpi_client = ocpi_client

    def _ensure_ocpi_client(self) -> None:
        """Ensure an OCPI client is available; create from environment if absent."""
        if not self.ocpi_client:
            base_url = os.getenv(
                "OCPI_BASE_URL", "https://your-ocpi-server.com")
            token = os.getenv("OCPI_TOKEN", "your-ocpi-token")
            self.ocpi_client = OCPILocationClient(base_url, token)

    def process_search_request(
        self,
        beckn_search_request,
        search_radius_km: float = 10.0
    ) -> Dict[str, Any]:
        """
        Process Beckn search request end-to-end.

        This method handles the complete flow from Beckn search request to
        filtered OCPI locations and back to Beckn on_search response.

        Args:
            beckn_search_request: Parsed Beckn search request
            search_radius_km: Search radius in kilometers (default: 10.0)

        Returns:
            Beckn on_search response

        Raises:
            ValueError: If location criteria are missing or invalid
            Exception: If processing fails
        """
        self._ensure_ocpi_client()

        try:
            # Step 1: Extract location criteria from Beckn request
            location_criteria = beckn_search_request.get_location_criteria()
            if not location_criteria:
                raise ValueError(
                    "No location criteria found in Beckn search request")

            gps = location_criteria.get('gps')
            if not gps:
                raise ValueError(
                    "No GPS coordinates found in location criteria")

            # Parse GPS coordinates
            try:
                target_lat, target_lon = map(float, gps.split(','))
                logger.info(
                    f"Search target: lat={target_lat}, lon={target_lon}")
            except ValueError as e:
                raise ValueError(
                    f"Invalid GPS format: {gps}. Expected 'lat,lon'") from e

            # Step 2: Fetch all OCPI locations
            logger.info("Fetching all OCPI locations...")
            all_locations = self.ocpi_client.get_all_locations()

            if not all_locations:
                logger.warning("No OCPI locations found")
                return self._create_empty_search_response(beckn_search_request)

            # Step 3: Filter locations by proximity
            logger.info(
                f"Filtering locations within {search_radius_km}km radius...")
            filtered_locations = LocationFilter.filter_locations_by_proximity(
                all_locations, target_lat, target_lon, search_radius_km
            )

            if not filtered_locations:
                logger.warning(
                    f"No locations found within {search_radius_km}km radius")
                return self._create_empty_search_response(beckn_search_request)

            # Step 4: Handle tariffs based on toggle
            tariff_lookup = {}
            if TARIFF_DECOMPOSITION_ENABLED:
                logger.info(
                    "Tariff decomposition enabled - fetching tariffs...")
                all_tariffs = self.ocpi_client.get_all_tariffs()
                if all_tariffs:
                    for tariff in all_tariffs:
                        tariff_id = tariff.get('id')
                        if tariff_id:
                            tariff_lookup[tariff_id] = tariff
                    logger.info(
                        f"Created tariff lookup with {len(tariff_lookup)} tariffs")
            else:
                logger.info(
                    "Tariff decomposition disabled - passing OCPI response as-is")

            # Step 5: Transform to Beckn on_search response
            logger.info(
                f"Transforming {len(filtered_locations)} locations to Beckn format...")
            beckn_response = self.transform_ocpi_locations_to_beckn_on_search_response(
                {'data': filtered_locations}, beckn_search_request, tariff_lookup
            )

            logger.info("Search request processed successfully")
            return beckn_response

        except Exception as e:
            logger.error(f"Error processing search request: {e}")
            return self._create_error_search_response(beckn_search_request, str(e))

    def transform_beckn_location_to_ocpi_query(self, beckn_request) -> Dict[str, Any]:
        """
        Transform Beckn search request to OCPI query parameters.

        Args:
            beckn_request: Beckn search request

        Returns:
            Dictionary of OCPI query parameters
        """
        query_params = {}
        location_criteria = beckn_request.get_location_criteria()
        if location_criteria:
            gps = location_criteria.get('gps')
            if gps:
                logger.info(f"Location search with GPS: {gps}")
            area = location_criteria.get('area', {})
            if area:
                area_code = area.get('code')
                if area_code:
                    query_params['area_filter'] = area_code
        category = beckn_request.get_category_criteria()
        if category:
            logger.info(f"Category filter: {category}")
        query_params.setdefault('limit', 50)
        query_params.setdefault('offset', 0)
        return query_params

    def filter_locations_by_proximity(self, locations: List[Dict], target_gps: str, radius_km: float = 10.0) -> List[Dict]:
        """
        Filter locations by proximity to target GPS coordinates.

        Args:
            locations: List of OCPI location dictionaries
            target_gps: Target GPS coordinates as "lat,lon" string
            radius_km: Search radius in kilometers

        Returns:
            Filtered list of locations with distance information
        """
        try:
            target_lat, target_lon = map(float, target_gps.split(','))
            return LocationFilter.filter_locations_by_proximity(locations, target_lat, target_lon, radius_km)
        except Exception as e:
            logger.error(f"Error filtering locations by proximity: {str(e)}")
            return locations

    def transform_ocpi_locations_to_beckn_on_search_response(self, ocpi_response: Dict[str, Any], beckn_request, tariffs: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Transform OCPI locations response to Beckn on_search response.

        Args:
            ocpi_response: OCPI locations response
            beckn_request: Original Beckn search request
            tariffs: Optional complete tariff data dictionary (tariff_id -> full tariff object)

        Returns:
            Beckn on_search response
        """
        try:
            if not isinstance(ocpi_response, dict):
                logger.error(
                    f"Expected dict for ocpi_response, got {type(ocpi_response)}: {ocpi_response}")
                ocpi_locations = []
            else:
                ocpi_locations = ocpi_response.get('data', [])

            location_criteria = beckn_request.get_location_criteria()
            if location_criteria and location_criteria.get('gps'):
                ocpi_locations = self.filter_locations_by_proximity(
                    ocpi_locations,
                    location_criteria['gps']
                )

            beckn_locations = []
            beckn_items = {}  # Changed to dict to track unique items by tariff_id
            beckn_fulfillments = []
            fulfillment_id_counter = 1

            # Collect operator and location names for provider descriptor
            operator_names = set()
            location_names = set()
            party_ids = set()

            for idx, ocpi_loc in enumerate(ocpi_locations):
                # Collect operator and location names
                operator_name = ocpi_loc.get('operator', {}).get(
                    'name', 'Unknown operator')
                location_name = ocpi_loc.get('name', 'Charging Location')
                operator_names.add(operator_name)
                location_names.add(location_name)
                party_ids.add(ocpi_loc.get('party_id', 'Unknown'))

                beckn_loc = {
                    "id": f"{ocpi_loc['id']}",
                    "descriptor": {
                        "name": ocpi_loc.get('name', 'Charging Location'),
                        "short_desc": f"Charging station at {ocpi_loc.get('address', 'Unknown address')}",
                        "long_desc": f"EV charging location operated by {ocpi_loc.get('operator', {}).get('name', 'Unknown operator')}"
                    },
                    "gps": f"{ocpi_loc['coordinates']['latitude']},{ocpi_loc['coordinates']['longitude']}",
                    "address": {
                        "full": ocpi_loc.get('address', ''),
                        "city": ocpi_loc.get('city', ''),
                        "state": ocpi_loc.get('state', ''),
                        "country": ocpi_loc.get('country', ''),
                        "area_code": ocpi_loc.get('postal_code', '')
                    }
                }
                if 'distance_km' in ocpi_loc:
                    beckn_loc['distance'] = f"{ocpi_loc['distance_km']} km"
                beckn_locations.append(beckn_loc)

                evses = ocpi_loc.get('evses', [])
                for evse_idx, evse in enumerate(evses):
                    connectors = evse.get('connectors', [])
                    for conn_idx, connector in enumerate(connectors):
                        # Create fulfillment for each connector (EVSE + connector combination)
                        fulfillment_id = f"{evse['uid']}_{connector['id']}"

                        beckn_fulfillment = {
                            "id": fulfillment_id,
                            "type": "charging",
                            "stops": {
                                "location": beckn_loc,
                                "time": {
                                    "label": "available",
                                    "range": {
                                        "start": datetime.now(timezone.utc).isoformat(),
                                        "end": (datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)).isoformat()
                                    }
                                }
                            },
                            "tags": {
                                "evse_uid": evse.get('uid', 'Unknown'),
                                "connector_type": connector.get('standard', 'Unknown'),
                                "power_type": connector.get('power_type', 'Unknown'),
                                "max_power": str(connector.get('max_electric_power', 0)),
                                "connector_format": connector.get('format', 'Unknown'),
                                "availability": evse.get('status', 'UNKNOWN'),
                                "facilities": ocpi_loc.get('facilities', []),
                                "opening_times": ocpi_loc.get('opening_times', {}),
                                "operator": ocpi_loc.get('operator', {}).get('name', 'Unknown')
                            }
                        }
                        beckn_fulfillments.append(beckn_fulfillment)

                        # Determine tariff ID and item ID
                        tariff_id = None
                        # Default fallback
                        item_id = f"item_{evse.get('uid', 'unknown')}_{connector.get('id', 'unknown')}"

                        # Get max_electric_power from connector for quantity
                        max_power = connector.get('max_electric_power', 0)
                        # Use max_electric_power as quantity value
                        quantity_value = str(max_power)

                        # Default price structure
                        price = {
                            "currency": "INR/kWh",
                            "value": "0"
                        }

                        # Extract tariff information if available
                        if TARIFF_DECOMPOSITION_ENABLED and tariffs and "tariff_ids" in connector and connector["tariff_ids"]:
                            tariff_id = connector["tariff_ids"][0]
                            tariff = tariffs.get(tariff_id)
                            if tariff:
                                # Use tariff ID + quantity value as item ID for uniqueness
                                item_id = f"{tariff_id}_{quantity_value}"

                                # Extract price from tariff elements - choose ENERGY type price component
                                price_value = "0"
                                if tariff.get("elements") and len(tariff["elements"]) > 0:
                                    element = tariff["elements"][0]
                                    if element.get("price_components") and len(element["price_components"]) > 0:
                                        # Find price component with type "ENERGY"
                                        energy_price_component = None
                                        for price_component in element["price_components"]:
                                            if price_component.get("type") == "ENERGY":
                                                energy_price_component = price_component
                                                break

                                        # If no ENERGY type found, fall back to first component
                                        if energy_price_component:
                                            price_component = energy_price_component
                                        else:
                                            price_component = element["price_components"][0]

                                        price_value = str(
                                            price_component.get("price", 0))

                                price = {
                                    "currency": tariff.get("currency", "INR"),
                                    "value": price_value,
                                    "description": f"Tariff ID: {tariff_id}",
                                    "tariff_data": tariff  # Include complete tariff data
                                }
                        elif not TARIFF_DECOMPOSITION_ENABLED and "tariff_ids" in connector and connector["tariff_ids"]:
                            # When tariff decomposition is disabled, use first tariff ID as item ID
                            tariff_id = connector["tariff_ids"][0]
                            item_id = f"{tariff_id}_{quantity_value}"

                            # Include tariff IDs as-is
                            price = {
                                "currency": "INR/kWh",
                                "value": "0",
                                "description": f"Tariff IDs: {connector['tariff_ids']}",
                                "tariff_ids": connector["tariff_ids"]
                            }
                        else:
                            # No tariff available, use quantity value in item_id for uniqueness
                            item_id = f"item_{evse.get('uid', 'unknown')}_{connector.get('id', 'unknown')}_{quantity_value}"

                        # Create or update item with consolidated location_ids and fulfillment_ids
                        if item_id not in beckn_items:
                            beckn_item = {
                                "id": item_id,
                                "descriptor": {
                                    "code": "energy"
                                },
                                "price": price,
                                "quantity": {
                                    "available": {
                                        "measure": {
                                            "unit": "Wh",
                                            "value": quantity_value  # Use max_electric_power value
                                        }
                                    }
                                },
                                "category_ids": ["ev_charging"],
                                "location_ids": [beckn_loc["id"]],
                                "fulfillment_ids": [fulfillment_id]
                            }
                            beckn_items[item_id] = beckn_item
                        else:
                            # Update existing item with additional location and fulfillment IDs
                            existing_item = beckn_items[item_id]
                            if beckn_loc["id"] not in existing_item["location_ids"]:
                                existing_item["location_ids"].append(
                                    beckn_loc["id"])
                            if fulfillment_id not in existing_item["fulfillment_ids"]:
                                existing_item["fulfillment_ids"].append(
                                    fulfillment_id)

            # Convert items dict to list for final response
            beckn_items_list = list(beckn_items.values())

            # Create provider name from operator and location information
            if len(operator_names) == 1:
                # Single operator, use operator name
                provider_name = list(operator_names)[0]
            elif len(operator_names) > 1:
                # Multiple operators, use a combined name
                provider_name = f"Multi-Operator EV Network ({', '.join(sorted(operator_names))})"
            else:
                # No operator info, use location names or default
                if location_names:
                    provider_name = f"EV Charging Network ({', '.join(sorted(location_names))})"
                else:
                    provider_name = "EV Charging Network"

            beckn_response = {
                "context": {
                    **beckn_request.context,
                    "action": "on_search",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "message_id": str(uuid.uuid4()),
                    "bap_id": beckn_request.context.get("bap_id"),
                    "bpp_id": list(party_ids)[0]
                },
                "message": {
                    "catalog": {
                        "providers": [{
                            "id": os.getenv("MY_PARTY_ID"),
                            "descriptor": {
                                "name": provider_name,
                                "short_desc": "Electric Vehicle Charging Stations",
                                "long_desc": "Network of EV charging stations via OCPI integration"
                            },
                            "locations": beckn_locations,
                            "items": beckn_items_list,
                            "fulfillments": beckn_fulfillments,
                            "categories": [
                                {
                                    "id": "ev_charging",
                                    "descriptor": {
                                        "name": "EV Charging",
                                        "short_desc": "Electric Vehicle Charging Services"
                                    }
                                }
                            ]
                        }]
                    }
                }
            }
            return beckn_response
        except Exception as e:
            logger.error(
                f"Error transforming OCPI response to Beckn: {str(e)}")
            raise

    def _create_empty_search_response(self, beckn_request) -> Dict[str, Any]:
        """Create empty search response when no locations found."""
        context = beckn_request.context.copy()
        context.update({
            "action": "on_search",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message_id": str(uuid.uuid4())
        })

        return {
            "context": context,
            "message": {
                "catalog": {
                    "descriptor": {
                        "name": "EV Charging Network",
                        "short_desc": "No charging stations found in the specified area",
                        "long_desc": "No EV charging stations available within the search radius"
                    },
                    "locations": [],
                    "items": [],
                    "fulfillments": [],
                    "categories": [
                        {
                            "id": "ev_charging",
                            "descriptor": {
                                "name": "EV Charging",
                                "short_desc": "Electric Vehicle Charging Services"
                            }
                        }
                    ]
                }
            }
        }

    def _create_error_search_response(self, beckn_request, error_message: str) -> Dict[str, Any]:
        """Create error search response."""
        context = beckn_request.context.copy()
        context.update({
            "action": "on_search",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message_id": str(uuid.uuid4())
        })

        return {
            "context": context,
            "message": {
                "catalog": {
                    "descriptor": {
                        "name": "EV Charging Network",
                        "short_desc": "Error occurred during search",
                        "long_desc": f"Search failed: {error_message}"
                    },
                    "locations": [],
                    "items": [],
                    "fulfillments": [],
                    "categories": [
                        {
                            "id": "ev_charging",
                            "descriptor": {
                                "name": "EV Charging",
                                "short_desc": "Electric Vehicle Charging Services"
                            }
                        }
                    ]
                }
            }
        }

    def transform_beckn_select_to_ocpi_session(self, beckn_select_request) -> dict:
        """
        Convert Beckn select request to OCPI session initiation parameters.
        """
        item = beckn_select_request.get_selected_item()
        location = beckn_select_request.get_selected_location()
        token = beckn_select_request.get_user_token() or "MOCK_TOKEN"
        # Assume item id format: item_{location_id}_evse_{evse_uid}_conn_{connector_id}
        item_id = item.get("id", "")
        try:
            parts = item_id.split("_")
            location_id = parts[1]
            evse_uid = parts[3]
            connector_id = parts[5]
        except Exception:
            raise ValueError(f"Invalid item id format: {item_id}")
        return {
            "location_id": location_id,
            "evse_uid": evse_uid,
            "connector_id": connector_id,
            "token": token
        }

    def transform_ocpi_session_to_beckn_on_select(self, ocpi_session_response: dict, beckn_select_request) -> dict:
        """
        Convert OCPI session response to Beckn on_select response.
        """
        context = beckn_select_request.context.copy()
        context.update({
            "action": "on_select"
        })
        session = ocpi_session_response
        return {
            "context": context,
            "message": {
                "order": {
                    "id": session.get("id"),
                    "state": session.get("status"),
                    "fulfillments": [{
                        "id": session.get("location_id"),
                        "start": {
                            "time": {
                                "timestamp": session.get("start_datetime")
                            }
                        }
                    }],
                    "authorization": {
                        "token": session.get("token"),
                        "method": session.get("auth_method"),
                        "reference": session.get("authorization_reference")
                    }
                }
            }
        }

    def transform_beckn_confirm_to_ocpi(self, beckn_confirm_request) -> dict:
        """
        Convert Beckn confirm request to OCPI session confirmation parameters.
        """
        session_id = beckn_confirm_request.get_session_id()
        if not session_id:
            raise ValueError("Session ID not found in Beckn confirm request")
        return {"session_id": session_id}

    def transform_ocpi_confirm_to_beckn_on_confirm(self, ocpi_confirm_response: dict, beckn_confirm_request) -> dict:
        """
        Convert OCPI confirm response to Beckn on_confirm response.
        """
        context = beckn_confirm_request.context.copy()
        context.update({
            "action": "on_confirm"
        })
        return {
            "context": context,
            "message": {
                "order": {
                    "id": ocpi_confirm_response.get("id"),
                    "state": ocpi_confirm_response.get("status"),
                    "confirmation_time": ocpi_confirm_response.get("confirmation_time"),
                    "message": ocpi_confirm_response.get("message")
                }
            }
        }

    def transform_beckn_status_to_ocpi(self, beckn_status_request) -> dict:
        """
        Convert Beckn status request to OCPI session status parameters.
        """
        session_id = beckn_status_request.get_session_id()
        if not session_id:
            raise ValueError("Session ID not found in Beckn status request")
        return {"session_id": session_id}

    def transform_ocpi_status_to_beckn_on_status(self, ocpi_status_response: dict, beckn_status_request) -> dict:
        """
        Convert OCPI session status response to Beckn on_status response.
        """
        context = beckn_status_request.context.copy()
        context.update({
            "action": "on_status"
        })
        session = ocpi_status_response
        return {
            "context": context,
            "message": {
                "order": {
                    "id": session.get("id"),
                    "state": session.get("status"),
                    "fulfillments": [{
                        "id": session.get("id"),
                        "start": {
                            "time": {
                                "timestamp": session.get("start_datetime")
                            }
                        }
                    }],
                    "authorization": {
                        "method": session.get("auth_method"),
                        "reference": session.get("authorization_reference")
                    },
                    "last_updated": session.get("last_updated"),
                    "kwh": session.get("kwh")
                }
            }
        }

    def transform_beckn_update_to_ocpi(self, beckn_update_request) -> dict:
        """
        Convert Beckn update request to OCPI session update parameters.
        """
        session_id = beckn_update_request.get_session_id()
        update_data = beckn_update_request.get_update_data()
        if not session_id:
            raise ValueError("Session ID not found in Beckn update request")
        return {"session_id": session_id, "update_data": update_data}

    def transform_ocpi_update_to_beckn_on_update(self, ocpi_update_response: dict, beckn_update_request) -> dict:
        """
        Convert OCPI update response to Beckn on_update response.
        """
        context = beckn_update_request.context.copy()
        context.update({
            "action": "on_update"
        })
        return {
            "context": context,
            "message": {
                "order": {
                    "id": ocpi_update_response.get("id"),
                    "state": ocpi_update_response.get("status"),
                    "updated_fields": ocpi_update_response.get("updated_fields"),
                    "last_updated": ocpi_update_response.get("last_updated"),
                    "message": ocpi_update_response.get("message")
                }
            }
        }

    def _extract_tariff_price_currency(self, tariff: Dict[str, Any]) -> Tuple[Optional[float], str]:
        """Extract price (prefer ENERGY component) and currency from an OCPI tariff."""
        price_value: Optional[float] = None
        currency: str = tariff.get("currency", "INR")
        try:
            elements = tariff.get("elements") or []
            if elements:
                element = elements[0]
                pcs = element.get("price_components") or []
                if pcs:
                    energy_pc = None
                    for pc in pcs:
                        if pc.get("type") == "ENERGY":
                            energy_pc = pc
                            break
                    chosen = energy_pc or pcs[0]
                    price_value = float(chosen.get("price", 0))
        except Exception as e:
            logger.warning(f"Failed extracting price from tariff: {e}")
        return price_value, currency

    def _find_connector_by_fulfillment_id(
        self,
        ocpi_locations_list: List[Dict[str, Any]],
        fulfillment_id: str
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        """Locate OCPI location/EVSE/connector matching the given fulfillment id (evse_uid + '_' + connector.id)."""
        for loc in ocpi_locations_list:
            for evse in (loc.get("evses", []) or []):
                for connector in (evse.get("connectors", []) or []):
                    fid = f"{evse.get('uid')}_{connector.get('id')}"
                    if fid == fulfillment_id:
                        return loc, evse, connector
        return None, None, None

    def create_confirm_request_from_on_init(self, on_init_response: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a Beckn confirm request based on the on_init response.

        Args:
            on_init_response: The on_init response dictionary

        Returns:
            Beckn confirm request dictionary
        """
        # Extract context from on_init response
        context_in = on_init_response.get('context', {})

        # Create new context for confirm request
        confirm_context = {
            "domain": context_in.get("domain", "ONDC:RET10"),
            "action": "confirm",
            "location": context_in.get("location", {
                "country": {"code": "IND"},
                "city": {"code": "std:080"}
            }),
            "version": context_in.get("version", "1.1.0"),
            "bap_id": context_in.get("bap_id"),
            "bap_uri": context_in.get("bap_uri"),
            "bpp_id": context_in.get("bpp_id"),
            "bpp_uri": context_in.get("bpp_uri"),
            "transaction_id": context_in.get("transaction_id"),
            "message_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
        }

        # Extract order information from on_init
        order_in = on_init_response.get('message', {}).get('order', {})

        # Extract items, billing, fulfillments, and payments
        items_in = order_in.get('items', [])
        billing_in = order_in.get('billing', {})
        fulfillments_in = order_in.get('fulfillments', [])
        payments_in = order_in.get('payments', [])
        quote_in = order_in.get('quote', {})

        # Create confirm request items (simplified - just id and quantity)
        confirm_items = []
        for item in items_in:
            confirm_item = {
                "id": item.get("id"),
                "quantity": {
                    "selected": {
                        "measure": {
                            "value": item.get("quantity", {}).get("selected", {}).get("measure", {}).get("value"),
                            "unit": item.get("quantity", {}).get("selected", {}).get("measure", {}).get("unit")
                        }
                    }
                }
            }
            confirm_items.append(confirm_item)

        # Create confirm request billing
        confirm_billing = {
            "name": billing_in.get("name", "John Doe"),
            "email": billing_in.get("email"),
            "phone": billing_in.get("phone")
        }

        # Create confirm request fulfillments (simplified)
        confirm_fulfillments = []
        for fulfillment in fulfillments_in:
            confirm_fulfillment = {
                "id": fulfillment.get("id"),
                "customer": {
                    "person": {
                        "name": billing_in.get("name", "John Doe")
                    },
                    "contact": {
                        "phone": billing_in.get("phone")
                    }
                }
            }
            confirm_fulfillments.append(confirm_fulfillment)

        # Create confirm request payments (mark as PAID)
        confirm_payments = []
        for payment in payments_in:
            confirm_payment = {
                "collected_by": "BPP",
                "params": {
                    "amount": payment.get("params", {}).get("amount"),
                    "currency": payment.get("params", {}).get("currency"),
                    "transaction_id": context_in.get("transaction_id")
                },
                "status": "PAID",
                "type": payment.get("type", "PRE-ORDER")
            }
            confirm_payments.append(confirm_payment)

        # Create confirm request quote
        confirm_quote = {
            "price": {
                "value": quote_in.get("price", {}).get("value"),
                "currency": quote_in.get("price", {}).get("currency")
            },
            "breakup": quote_in.get("breakup", [])
        }

        # Assemble the complete confirm request
        confirm_request = {
            "context": confirm_context,
            "message": {
                "order": {
                    "provider": {
                        "id": order_in.get("provider", {}).get("id")
                    },
                    "items": confirm_items,
                    "billing": confirm_billing,
                    "fulfillments": confirm_fulfillments,
                    "payments": confirm_payments,
                    "quote": confirm_quote
                }
            }
        }

        return confirm_request

    def process_init_request(self, beckn_init_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a Beckn init request: validate tariff against the requested connector/location
        using OCPI data, compute pricing, generate a payment link, and return an on_init response.

        Expected beckn_init_request shape aligns with requests/init_request.json.
        """
        self._ensure_ocpi_client()

        try:
            context_in = beckn_init_request.get("context", {})
            message_in = beckn_init_request.get("message", {})
            order_in = message_in.get("order", {})

            provider_in = order_in.get("provider", {})
            items_in = order_in.get("items", [])
            fulfillments_in = order_in.get("fulfillments", [])
            billing_in = order_in.get("billing", {})

            selected_item = items_in[0] if items_in else {}
            item_id = selected_item.get("id") or ""
            selected_measure = (selected_item.get("quantity", {})
                                .get("selected", {})
                                .get("measure", {}))
            selected_value = selected_measure.get("value")
            selected_unit = selected_measure.get("unit")

            fulfillment = fulfillments_in[0] if fulfillments_in else {}
            fulfillment_id = fulfillment.get("id") or ""

            # Parse tariff_id from item id if present in the pattern <tariff_id>_<quantity_value>
            tariff_id_from_item: Optional[str] = None
            if item_id and "_" in item_id:
                try:
                    tariff_id_from_item = item_id.split("_")[0]
                except Exception:
                    tariff_id_from_item = None

            # Fetch OCPI data
            ocpi_locations_resp = self.ocpi_client.get_all_locations()
            if isinstance(ocpi_locations_resp, dict) and 'data' in ocpi_locations_resp:
                ocpi_locations = ocpi_locations_resp.get('data') or []
            else:
                ocpi_locations = ocpi_locations_resp or []

            ocpi_tariffs_resp = self.ocpi_client.get_all_tariffs()
            if isinstance(ocpi_tariffs_resp, dict) and 'data' in ocpi_tariffs_resp:
                ocpi_tariffs = ocpi_tariffs_resp.get('data') or []
            else:
                ocpi_tariffs = ocpi_tariffs_resp or []

            # Locate requested connector by fulfillment id (evse_uid + '_' + connector.id)
            matched_location, matched_evse, matched_connector = self._find_connector_by_fulfillment_id(
                ocpi_locations, fulfillment_id
            )

            if not matched_connector:
                raise ValueError(
                    f"No connector found for fulfillment id: {fulfillment_id}")

            # Confirm tariff applicability
            connector_tariff_ids = matched_connector.get("tariff_ids") or []
            confirmed_tariff_id: Optional[str] = None
            if tariff_id_from_item and tariff_id_from_item in connector_tariff_ids:
                confirmed_tariff_id = tariff_id_from_item
            elif connector_tariff_ids:
                confirmed_tariff_id = connector_tariff_ids[0]
            else:
                raise ValueError(
                    "No applicable tariff found for selected connector")

            # Fetch the confirmed tariff details
            tariff_lookup = {
                t.get('id'): t for t in ocpi_tariffs if t.get('id')}
            confirmed_tariff = tariff_lookup.get(confirmed_tariff_id)
            if not confirmed_tariff:
                raise ValueError(f"Tariff not found: {confirmed_tariff_id}")

            price_per_unit, currency = self._extract_tariff_price_currency(
                confirmed_tariff)
            if price_per_unit is None:
                # Fallback default
                price_per_unit = 0.0

            # Compute totals
            try:
                consumption_value = float(
                    selected_value) if selected_value is not None else 0.0
            except (ValueError, TypeError):
                consumption_value = 0.0
            total_price = round(price_per_unit * consumption_value, 2)

            # Generate payment link
            tx_id = context_in.get("transaction_id") or str(uuid.uuid4())
            payment_base_url = os.getenv(
                "PAYMENT_BASE_URL", "https://payments.example.com/pay")
            payment_url = f"{payment_base_url}?tx_id={tx_id}&amount={total_price}&currency={currency}"

            # Provider details from OCPI location/operator
            operator = (matched_location.get("operator")
                        or {}) if matched_location else {}
            provider_descriptor = {
                "name": operator.get("name") or provider_in.get("id") or "EV Charging Network",
                "short_desc": operator.get("name") or "EV Charging Services",
            }

            # Build fulfillment tags from OCPI
            fulfillment_tags = [
                {
                    "descriptor": {"name": "Charging Point Specifications"},
                    "list": [
                        {"descriptor": {"name": "Charger type", "code": "charger-type"},
                         "value": matched_connector.get("power_type")},
                        {"descriptor": {"name": "Connector type", "code": "connector-type"},
                         "value": matched_connector.get("standard")},
                        {"descriptor": {"name": "Power Rating"},
                         "value": f"{matched_connector.get('max_electric_power')}kW"},
                        {"descriptor": {"name": "Availability"},
                         "value": matched_evse.get("status") if matched_evse else "UNKNOWN"},
                    ],
                    "display": True,
                }
            ]

            # Quantity available from connector power (if present)
            available_value = matched_connector.get("max_electric_power")
            # Assemble response
            context_out = {**context_in}
            context_out.update({
                "action": "on_init",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message_id": str(uuid.uuid4()),
            })
            # Prefer OCPI party_id for bpp_id if present
            if matched_location and matched_location.get("party_id"):
                context_out["bpp_id"] = matched_location.get("party_id")

            response = {
                "context": context_out,
                "message": {
                    "order": {
                        "provider": {
                            "id": provider_in.get("id") or matched_location.get("party_id"),
                            "descriptor": provider_descriptor,
                        },
                        "items": [
                            {
                                "id": item_id or f"{confirmed_tariff_id}",
                                "descriptor": {"code": "energy"},
                                "price": {
                                    "value": str(price_per_unit),
                                    "currency": f"{currency}/kWh",
                                },
                                "quantity": {
                                    "available": {
                                        "measure": {
                                            "value": str(available_value) if available_value is not None else "0",
                                            "unit": "kW",
                                        }
                                    },
                                    "selected": {
                                        "measure": {
                                            "value": str(selected_value) if selected_value is not None else "0",
                                            "unit": selected_unit or "kWh",
                                        }
                                    },
                                },
                            }
                        ],
                        "fulfillments": [
                            {
                                "id": fulfillment_id,
                                "type": "CHARGING",
                                "state": {"descriptor": {"code": "order-initiated"}},
                                "stops": [
                                    {
                                        "type": "start",
                                        "time": {"timestamp": datetime.now(timezone.utc).isoformat()},
                                    },
                                    {
                                        "type": "end",
                                        "time": {"timestamp": datetime.now(timezone.utc).isoformat()},
                                    },
                                ],
                                "tags": fulfillment_tags,
                            }
                        ],
                        "billing": {
                            "email": billing_in.get("email"),
                            "phone": billing_in.get("phone"),
                        },
                        "quote": {
                            "price": {"value": str(total_price), "currency": currency},
                            "breakup": [
                                {
                                    "item": {
                                        "descriptor": {"name": "Estimated units consumed"},
                                        "quantity": {
                                            "selected": {
                                                "measure": {
                                                    "value": str(selected_value) if selected_value is not None else "0",
                                                    "unit": selected_unit or "kWh",
                                                }
                                            }
                                        },
                                    },
                                    "price": {"value": str(total_price), "currency": currency},
                                }
                            ],
                        },
                        "payments": [
                            {
                                "url": payment_url,
                                "type": "PRE-ORDER",
                                "status": "NOT-PAID",
                                "params": {"amount": str(total_price), "currency": currency},
                                "time": {
                                    "range": {
                                        "start": datetime.now(timezone.utc).isoformat(),
                                        "end": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
                                    }
                                },
                            }
                        ],
                        "cancellation_terms": [
                            {
                                "fulfillment_state": {"descriptor": {"code": "charging-start"}},
                                "cancellation_fee": {"percentage": "30%"},
                                "external_ref": {
                                    "mimetype": "text/html",
                                    "url": os.getenv("CANCELLATION_TNC_URL", "https://example.com/tnc.html"),
                                },
                            }
                        ],
                    }
                },
            }

            return response
        except Exception as e:
            logger.error(f"Error processing init request: {e}")
            # Build minimal error response with on_init context
            context_in = (beckn_init_request or {}).get("context", {})
            context_out = {**context_in}
            context_out.update({
                "action": "on_init",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message_id": str(uuid.uuid4()),
            })
            return {
                "context": context_out,
                "message": {
                    "order": {
                        "provider": {
                            "descriptor": {"name": "Error Processing Init"}
                        },
                        "items": [],
                        "fulfillments": [],
                        "billing": {},
                        "quote": {"price": {"value": "0", "currency": "INR"}, "breakup": []},
                        "payments": [],
                    }
                },
            }

    def process_confirm_request(self, beckn_confirm_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process Beckn confirm request and return on_confirm response.
        Enriches response using OCPI location/connector details when possible.
        """
        # Ensure OCPI client available for enrichment
        self._ensure_ocpi_client()

        try:
            context_in = beckn_confirm_request.get("context", {})
            order_in = beckn_confirm_request.get(
                "message", {}).get("order", {})

            provider_in = order_in.get("provider", {})
            items_in = order_in.get("items", [])
            fulfillments_in = order_in.get("fulfillments", [])
            billing_in = order_in.get("billing", {})
            payments_in = order_in.get("payments", [])
            quote_in = order_in.get("quote", {})

            selected_item = items_in[0] if items_in else {}
            item_id = selected_item.get("id")
            selected_measure = (selected_item.get("quantity", {})
                                .get("selected", {})
                                .get("measure", {}))
            selected_value = selected_measure.get("value")
            selected_unit = selected_measure.get("unit")

            fulfillment = fulfillments_in[0] if fulfillments_in else {}
            fulfillment_id = fulfillment.get("id") or ""

            # Fetch OCPI locations for enrichment
            ocpi_locations_resp = self.ocpi_client.get_all_locations()
            if isinstance(ocpi_locations_resp, dict) and 'data' in ocpi_locations_resp:
                ocpi_locations = ocpi_locations_resp.get('data') or []
            else:
                ocpi_locations = ocpi_locations_resp or []

            matched_location, matched_evse, matched_connector = self._find_connector_by_fulfillment_id(
                ocpi_locations, fulfillment_id
            )

            # Build provider descriptor from OCPI operator if available
            operator = (matched_location.get("operator")
                        or {}) if matched_location else {}
            provider_descriptor = {
                "name": operator.get("name") or provider_in.get("id") or "EV Charging Network",
                "short_desc": operator.get("name") or "EV Charging Services",
                "images": provider_in.get("descriptor", {}).get("images", []) or []
            }

            # Compute unit price from quote if available
            total_price = None
            unit_price = None
            quote_price = quote_in.get("price", {})
            if quote_price and quote_price.get("value") is not None:
                try:
                    total_price = float(quote_price.get("value"))
                    if selected_value not in (None, "0", 0):
                        unit_price = round(
                            total_price / float(selected_value), 2)
                except (ValueError, TypeError):
                    unit_price = None
            currency = quote_price.get("currency", "INR")

            # Build item response
            item_descriptor_code = "energy"
            item_price = {
                "value": str(unit_price) if unit_price is not None else quote_price.get("value", "0"),
                "currency": f"{currency}/kWh"
            }
            available_value = None
            available_unit = "kW"
            if matched_connector and matched_connector.get("max_electric_power") is not None:
                available_value = str(
                    matched_connector.get("max_electric_power"))
            else:
                # Fallback
                available_value = "0"

            on_confirm_item = {
                "id": item_id,
                "descriptor": {"code": item_descriptor_code},
                "price": item_price,
                "quantity": {
                    "available": {
                        "measure": {
                            "value": available_value,
                            "unit": available_unit
                        }
                    },
                    "selected": {
                        "measure": {
                            "value": str(selected_value) if selected_value is not None else "0",
                            "unit": selected_unit or "kWh"
                        }
                    }
                }
            }

            # Fulfillment tags enrichment
            fulfillment_tags = [
                {
                    "descriptor": {"name": "Charging Point"},
                    "list": [
                        {"descriptor": {"name": "Charger type"},
                         "value": (matched_connector or {}).get("power_type", "")},
                        {"descriptor": {"name": "Connector type"},
                         "value": (matched_connector or {}).get("standard", "")},
                        {"descriptor": {"name": "Power Rating"},
                         "value": f"{(matched_connector or {}).get('max_electric_power', '')}kW"},
                        {"descriptor": {"name": "Availability"},
                         "value": (matched_evse or {}).get("status", "")},
                    ],
                    "display": True
                }
            ]

            # Build stops with optional GPS from location
            gps_value = None
            if matched_location and matched_location.get("coordinates"):
                lat = matched_location["coordinates"].get("latitude")
                lon = matched_location["coordinates"].get("longitude")
                if lat is not None and lon is not None:
                    gps_value = f"{lat},{lon}"

            now_iso = datetime.now(timezone.utc).isoformat()
            stops = [
                {
                    "type": "start",
                    **({"location": {"gps": gps_value}} if gps_value else {}),
                    "time": {
                        "timestamp": now_iso,
                        "range": {
                            "start": now_iso,
                            "end": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()
                        }
                    },
                    "instructions": {
                        "name": "Charging instructions",
                        "short_desc": "To start your charging, go to the allocated charger and press 'start' in your app"
                    }
                },
                {
                    "type": "end",
                    "time": {
                        "timestamp": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
                        "range": {
                            "start": (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat(),
                            "end": (datetime.now(timezone.utc) + timedelta(minutes=40)).isoformat()
                        }
                    }
                }
            ]

            # Context out
            context_out = {**context_in}
            context_out.update({
                "action": "on_confirm",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message_id": str(uuid.uuid4()),
            })

            # Build response
            response = {
                "context": context_out,
                "message": {
                    "order": {
                        "id": (context_in.get("transaction_id") or str(uuid.uuid4()))[:24],
                        "provider": {
                            "id": provider_in.get("id"),
                            "descriptor": provider_descriptor
                        },
                        "items": [on_confirm_item],
                        "fulfillments": [
                            {
                                "id": fulfillment_id,
                                "customer": {
                                    "person": {
                                        "name": billing_in.get("name", "")
                                    },
                                    "contact": {
                                        "phone": billing_in.get("phone", "")
                                    }
                                },
                                "type": "CHARGING",
                                "state": {"descriptor": {"code": "payment-completed"}},
                                "stops": stops,
                                "tags": fulfillment_tags
                            }
                        ],
                        "billing": {
                            "email": billing_in.get("email"),
                            "phone": billing_in.get("phone")
                        },
                        "quote": quote_in,
                        "payments": payments_in or [],
                        "cancellation_terms": [
                            {
                                "fulfillment_state": {"descriptor": {"code": "charging-start"}},
                                "cancellation_fee": {"percentage": "30%"},
                                "external_ref": {
                                    "mimetype": "text/html",
                                    "url": os.getenv("CANCELLATION_TNC_URL", "https://example.com/tnc.html")
                                }
                            }
                        ]
                    }
                }
            }

            return response
        except Exception as e:
            logger.error(f"Error processing confirm request: {e}")
            context_out = {**(beckn_confirm_request.get("context", {}))}
            context_out.update({
                "action": "on_confirm",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message_id": str(uuid.uuid4()),
            })
            return {
                "context": context_out,
                "message": {"order": {"id": None, "items": [], "fulfillments": [], "quote": {}, "payments": []}}
            }

    def transform_beckn_cdr_to_ocpi(self, beckn_cdr_request) -> dict:
        """
        Convert Beckn CDR request to OCPI CDR format.
        """
        session_id = beckn_cdr_request.get_session_id()
        billing_data = beckn_cdr_request.get_billing_data()
        payment_data = beckn_cdr_request.get_payment_data()

        if not session_id:
            raise ValueError("Session ID not found in Beckn CDR request")

        return {
            "session_id": session_id,
            "billing_data": billing_data,
            "payment_data": payment_data
        }

    def transform_ocpi_cdr_to_beckn_response(self, ocpi_cdr_data: dict, beckn_cdr_request, push_response: Optional[dict] = None) -> dict:
        """
        Convert OCPI CDR data to Beckn CDR response format.
        """
        context = beckn_cdr_request.context.copy()
        context.update({
            "action": "on_cdr",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message_id": str(uuid.uuid4())
        })

        # Extract cost information from CDR
        total_cost = ocpi_cdr_data.get("total_cost", {})
        energy_cost = ocpi_cdr_data.get("total_energy_cost", {})

        return {
            "context": context,
            "message": {
                "order": {
                    "id": ocpi_cdr_data.get("session_id"),
                    "state": "COMPLETED",
                    "cdr": {
                        "id": ocpi_cdr_data.get("id"),
                        "session_id": ocpi_cdr_data.get("session_id"),
                        "start_time": ocpi_cdr_data.get("start_date_time"),
                        "end_time": ocpi_cdr_data.get("end_date_time"),
                        "location": {
                            "id": ocpi_cdr_data.get("cdr_location", {}).get("id"),
                            "name": ocpi_cdr_data.get("cdr_location", {}).get("name"),
                            "address": ocpi_cdr_data.get("cdr_location", {}).get("address"),
                            "gps": f"{ocpi_cdr_data.get('cdr_location', {}).get('coordinates', {}).get('latitude', '')},{ocpi_cdr_data.get('cdr_location', {}).get('coordinates', {}).get('longitude', '')}"
                        },
                        "energy": {
                            "consumed": ocpi_cdr_data.get("total_energy", 0),
                            "unit": "kWh"
                        },
                        "duration": {
                            "total_time": ocpi_cdr_data.get("total_time", 0),
                            "unit": "hours"
                        },
                        "cost": {
                            "currency": ocpi_cdr_data.get("currency", "INR"),
                            "total_amount": total_cost.get("incl_vat", total_cost.get("excl_vat", 0)),
                            "energy_cost": energy_cost.get("incl_vat", energy_cost.get("excl_vat", 0)),
                            "breakdown": {
                                "base_amount": total_cost.get("excl_vat", 0),
                                "tax_amount": total_cost.get("incl_vat", 0) - total_cost.get("excl_vat", 0),
                                "tax_rate": 18.0
                            }
                        },
                        "payment": {
                            "method": ocpi_cdr_data.get("auth_method", "Unknown"),
                            "reference": ocpi_cdr_data.get("authorization_reference"),
                            "invoice_id": ocpi_cdr_data.get("invoice_reference_id")
                        }
                    },
                    "billing_status": "COMPLETED" if push_response and push_response.get("status_code") == 1000 else "PENDING",
                    "last_updated": ocpi_cdr_data.get("last_updated")
                }
            }
        }

    def generate_session_cdr(self, session_id: str, session_data: Optional[dict] = None) -> dict:
        """
        Generate a CDR for a completed session using OCPI client.
        """
        logger.info(f"Generating CDR for session: {session_id}")
        cdr_data = self.ocpi_client.generate_cdr(session_id, session_data)
        return cdr_data

    def push_cdr_to_network(self, cdr_data: dict) -> dict:
        """
        Push CDR to OCPI network.
        """
        logger.info(f"Pushing CDR to network: {cdr_data.get('id', 'Unknown')}")
        push_response = self.ocpi_client.push_cdr(cdr_data)
        return push_response

    def process_select_request(self, beckn_select_request) -> Dict[str, Any]:
        """
        Process Beckn select request and return on_select response.

        Args:
            beckn_select_request: Beckn select request object

        Returns:
            Beckn on_select response
        """
        try:
            # Ensure OCPI client
            self._ensure_ocpi_client()
            # Extract information from the select request
            order = beckn_select_request.message.get("order", {})
            provider_id = order.get("provider", {}).get(
                "id", os.getenv("MY_PARTY_ID"))
            items = order.get("items", [])
            fulfillments = order.get("fulfillments", [])

            # Get the first item and fulfillment for processing
            selected_item = items[0] if items else {}
            selected_fulfillment = fulfillments[0] if fulfillments else {}

            # Extract item details
            item_id = selected_item.get("id", "")
            selected_quantity = selected_item.get(
                "quantity", {}).get("selected", {}).get("measure", {})
            selected_value = selected_quantity.get("value")
            selected_unit = selected_quantity.get("unit")

            # Extract fulfillment details
            fulfillment_id = selected_fulfillment.get("id", "")

            # Fetch OCPI locations to get connector information
            ocpi_locations = []
            try:
                ocpi_response = self.ocpi_client.get_all_locations()
                if isinstance(ocpi_response, dict) and 'data' in ocpi_response:
                    ocpi_locations = ocpi_response['data']
                elif isinstance(ocpi_response, list):
                    ocpi_locations = ocpi_response
            except Exception as e:
                logger.warning(f"Failed to fetch OCPI locations: {str(e)}")

            # Find the matching connector by fulfillment ID
            matched_connector = None
            matched_evse = None
            matched_location = None
            tariff_price = None  # Default price
            currency = "INR/kWh"

            logger.info(f"Looking for fulfillment ID: {fulfillment_id}")

            for ocpi_loc in ocpi_locations:
                evses = ocpi_loc.get('evses', [])
                for evse in evses:
                    connectors = evse.get('connectors', [])
                    for connector in connectors:
                        # Create the fulfillment ID format exactly as it was created in search
                        connector_fulfillment_id = f"{evse['uid']}_{connector['id']}"

                        logger.debug(
                            f"Checking connector fulfillment ID: {connector_fulfillment_id}")

                        # Check if this matches the fulfillment ID from the request
                        if connector_fulfillment_id == fulfillment_id:
                            logger.info(
                                f"Found matching connector: {connector_fulfillment_id}")
                            matched_connector = connector
                            matched_evse = evse
                            matched_location = ocpi_loc

                            # Fetch tariff information for this connector
                            if "tariff_ids" in connector and connector["tariff_ids"]:
                                tariff_id = connector["tariff_ids"][0]
                                logger.info(f"Found tariff ID: {tariff_id}")

                                try:
                                    # Fetch tariff details
                                    tariffs_response = self.ocpi_client.get_all_tariffs()
                                    if isinstance(tariffs_response, dict) and 'data' in tariffs_response:
                                        tariffs = {
                                            t['id']: t for t in tariffs_response['data']}
                                    elif isinstance(tariffs_response, list):
                                        tariffs = {
                                            t['id']: t for t in tariffs_response}
                                    else:
                                        tariffs = {}

                                    tariff = tariffs.get(tariff_id)

                                    if tariff and tariff.get("elements"):
                                        element = tariff["elements"][0]
                                        if element.get("price_components"):
                                            # Find ENERGY type price component
                                            energy_price_component = None
                                            for price_component in element["price_components"]:
                                                if price_component.get("type") == "ENERGY":
                                                    energy_price_component = price_component
                                                    break

                                            # Use ENERGY component or fall back to first component
                                            if energy_price_component:
                                                price_component = energy_price_component
                                            else:
                                                price_component = element["price_components"][0]

                                            tariff_price = str(
                                                price_component.get("price", 8))
                                            currency = tariff.get(
                                                "currency", "INR")
                                            logger.info(
                                                f"Extracted tariff price: {tariff_price} {currency}")
                                except Exception as e:
                                    logger.warning(
                                        f"Failed to fetch tariff information: {str(e)}")
                            else:
                                logger.info("No tariff IDs found in connector")

                            break
                    if matched_connector:
                        break
                if matched_connector:
                    break

            if not matched_connector:
                logger.warning(
                    f"No matching connector found for fulfillment ID: {fulfillment_id}")

            # Calculate total price based on tariff and selected quantity
            try:
                total_price = float(tariff_price) * float(selected_value)
            except (ValueError, TypeError):
                total_price = None  # Default fallback

            # Create provider descriptor
            provider_descriptor = {
                "name": matched_location.get("party_id"),
                "short_desc": "Electric Vehicle Charging Services",
                "images": [
                    {
                        "url": "https://example.com/images/ev-charging-logo.png"
                    }
                ]
            }

            # Create item response
            item_response = {
                "id": item_id,
                "descriptor": {
                    "code": "energy"
                },
                "price": {
                    "value": tariff_price,
                    "currency": f"{currency}/kWh"
                },
                "quantity": {
                    "available": {
                        "measure": {
                            "value": matched_connector.get("max_electric_power"),
                            "unit": "Wh"
                        }
                    },
                    "selected": {
                        "measure": {
                            "value": selected_value,
                            "unit": selected_unit
                        }
                    }
                }
            }

            # Create add-on items (if any)
            add_ons = selected_item.get("add_ons", [])
            add_on_items = []
            if add_ons:
                add_on_items.append({
                    "id": f"{item_id}-addon-1",
                    "descriptor": {
                        "code": "add-on-item",
                        "name": "Free car wash"
                    },
                    "price": {
                        "value": "0",
                        "currency": "INR"
                    }
                })

            # Create fulfillment response with connector details
            fulfillment_response = {
                "id": fulfillment_id,
                "type": "CHARGING",
                "stops": [
                    {
                        "type": "start",
                        "time": {
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                    },
                    {
                        "type": "end",
                        "time": {
                            "timestamp": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()
                        }
                    }
                ],
                "tags": [
                    {
                        "descriptor": {
                            "name": "Charging Point Specifications"
                        },
                        "list": [
                            {
                                "descriptor": {
                                    "name": "Charger type",
                                    "code": "charger-type"
                                },
                                "value": matched_connector.get("power_type")
                            },
                            {
                                "descriptor": {
                                    "name": "Connector type",
                                    "code": "connector-type"
                                },
                                "value": matched_connector.get("standard")
                            },
                            {
                                "descriptor": {
                                    "name": "Power Rating"
                                },
                                "value": f"{matched_connector.get('max_electric_power')}kW"
                            },
                            {
                                "descriptor": {
                                    "name": "Availability"
                                },
                                "value": matched_evse.get("status")
                            }
                        ],
                        "display": True
                    }
                ]
            }

            # Create quote breakdown
            quote_breakup = [
                {
                    "item": {
                        "id": item_id,
                        "descriptor": {
                            "name": "Estimated units consumed"
                        },
                        "quantity": {
                            "selected": {
                                "measure": {
                                    "value": selected_value,
                                    "unit": selected_unit
                                }
                            }
                        }
                    },
                    "price": {
                        "value": str(total_price),
                        "currency": "INR"
                    }
                }
            ]

            # Add add-on items to quote breakdown
            if add_on_items:
                quote_breakup.append({
                    "item": {
                        "add_ons": [
                            {
                                "id": f"{item_id}-addon-1"
                            }
                        ],
                        "descriptor": {
                            "name": "Free car wash"
                        }
                    },
                    "price": {
                        "value": "0",
                        "currency": "INR"
                    }
                })

            # Create the complete response
            context = beckn_select_request.context.copy()
            context.update({
                "action": "on_select",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message_id": str(uuid.uuid4())
            })

            response = {
                "context": context,
                "message": {
                    "order": {
                        "provider": {
                            "id": provider_id,
                            "descriptor": provider_descriptor
                        },
                        "items": [item_response] + add_on_items,
                        "fulfillments": [fulfillment_response],
                        "quote": {
                            "price": {
                                "value": str(total_price),
                                "currency": "INR"
                            },
                            "breakup": quote_breakup
                        }
                    }
                }
            }

            return response

        except Exception as e:
            logger.error(f"Error processing select request: {str(e)}")
            # Return error response
            context = beckn_select_request.context.copy()
            context.update({
                "action": "on_select",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message_id": str(uuid.uuid4())
            })

            return {
                "context": context,
                "message": {
                    "order": {
                        "provider": {
                            "id": os.getenv("MY_PARTY_ID"),
                            "descriptor": {
                                "name": "Error Processing Request",
                                "short_desc": "Failed to process select request"
                            }
                        },
                        "items": [],
                        "fulfillments": [],
                        "quote": {
                            "price": {
                                "value": "0",
                                "currency": "INR"
                            },
                            "breakup": []
                        }
                    }
                }
            }


# Utility functions for easy integration
def create_sample_beckn_search_request(
    latitude: float = 19.0760,
    longitude: float = 72.8777,
    radius_km: float = 5.0
):
    """
    Create a sample Beckn search request for testing.

    Args:
        latitude: Target latitude (default: Mumbai)
        longitude: Target longitude (default: Mumbai)
        radius_km: Search radius in kilometers

    Returns:
        Sample Beckn search request
    """
    context = {
        "domain": "ONDC:RET10",
        "country": "IND",
        "city": "std:080",
        "action": "search",
        "core_version": "1.2.0",
        "bap_id": "sample_bap",
        "bap_uri": "https://sample-bap.com",
        "transaction_id": str(uuid.uuid4()),
        "message_id": str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ttl": "PT30S"
    }

    message = {
        "intent": {
            "fulfillment": {
                "start": {
                    "location": {
                        "gps": f"{latitude},{longitude}",
                        "address": {
                            "area_code": "400001",
                            "city": "Mumbai",
                            "state": "Maharashtra",
                            "country": "India"
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

    # Import here to avoid circular imports
    from beckn_modules import BecknSearchRequest
    return BecknSearchRequest(context=context, message=message)


def quick_search_example():
    """
    Quick example of how to use the Beckn-OCPI Bridge.

    This function demonstrates the basic usage pattern for integrating
    OCPI charging networks with Beckn.
    """
    # Configuration
    OCPI_BASE_URL = "https://your-ocpi-server.com"
    OCPI_TOKEN = "your-ocpi-token"

    # Create OCPI client
    ocpi_client = OCPILocationClient(OCPI_BASE_URL, OCPI_TOKEN)

    # Create bridge
    bridge = BecknOCPIBridge(ocpi_client)

    # Create sample search request
    sample_request = create_sample_beckn_search_request(
        latitude=28.5502,  # Delhi coordinates
        longitude=77.2583,
        radius_km=5.0
    )

    # Process search request
    try:
        response = bridge.process_search_request(
            sample_request, search_radius_km=5.0)
        print("Search completed successfully!")
        print(
            f"Locations found: {len(response.get('message', {}).get('catalog', {}).get('locations', []))}")
        return response
    except Exception as e:
        print(f"Error: {e}")
        return None


# if __name__ == "__main__":
#     # Configure logging
#     logging.basicConfig(
#         level=logging.INFO,
#         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
#     )

#     # Run quick example
#     quick_search_example()
