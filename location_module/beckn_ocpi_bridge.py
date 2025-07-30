"""
Beckn-OCPI Bridge
================

A comprehensive bridge library for integrating OCPI (Open Charge Point Interface) 
charging networks with the Beckn protocol. This library enables OCPI providers to 
easily participate in the Beckn ecosystem.

Features:
- OCPI location fetching with pagination
- Location filtering by proximity
- Beckn-OCPI protocol transformations
- Complete search, select, confirm, status, update, and CDR flows
- Distance calculations using Haversine formula
- Error handling and logging

Usage:
    from beckn_ocpi_bridge import BecknOCPIBridge, OCPILocationClient
    
    # Initialize the bridge
    ocpi_client = OCPILocationClient(base_url="https://your-ocpi-server.com", token="your-token")
    bridge = BecknOCPIBridge(ocpi_client)
    
    # Process a Beckn search request
    response = bridge.process_search_request(beckn_search_request)

Author: Beckn-OCPI Bridge Team
Version: 1.0.0
"""

import logging
import math
import requests
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any, Tuple
import uuid

logger = logging.getLogger(__name__)


class OCPILocationClient:
    """
    OCPI Location Client for fetching locations with pagination support.

    This client handles communication with OCPI servers to fetch charging locations
    with automatic pagination support.
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
    interactions including search, select, confirm, status, update, and CDR flows.
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
        if not self.ocpi_client:
            raise ValueError(
                "OCPI client not set. Use set_ocpi_client() first.")

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

            # Step 4: Transform to Beckn on_search response
            logger.info(
                f"Transforming {len(filtered_locations)} locations to Beckn format...")
            beckn_response = self.transform_ocpi_locations_to_beckn_on_search_response(
                {'data': filtered_locations}, beckn_search_request
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

    def transform_ocpi_locations_to_beckn_on_search_response(self, ocpi_response: Dict[str, Any], beckn_request, tariffs: Optional[Dict[str, Dict[str, str]]] = None) -> Dict[str, Any]:
        """
        Transform OCPI locations response to Beckn on_search response.

        Args:
            ocpi_response: OCPI locations response
            beckn_request: Original Beckn search request
            tariffs: Optional tariff information for pricing

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
            beckn_items = []
            beckn_fulfillments = []

            for idx, ocpi_loc in enumerate(ocpi_locations):
                beckn_loc = {
                    "id": f"loc_{ocpi_loc['id']}",
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
                        item_id = f"item_{ocpi_loc['id']}_evse_{evse['uid']}_conn_{connector['id']}"
                        price = {"currency": "INR", "value": "0"}
                        if tariffs and "tariff_ids" in connector and connector["tariff_ids"]:
                            tariff_id = connector["tariff_ids"][0]
                            tariff = tariffs.get(tariff_id)
                            if tariff:
                                price = {
                                    "currency": tariff.get("currency", "INR"),
                                    "value": tariff.get("price", "0"),
                                    "description": tariff.get("desc", "")
                                }
                        beckn_item = {
                            "id": item_id,
                            "descriptor": {
                                "name": f"EV Charging - {connector.get('standard', 'Unknown')}",
                                "short_desc": f"Power: {connector.get('max_electric_power', 'Unknown')}kW",
                                "long_desc": f"Connector type: {connector.get('standard', 'Unknown')}, "
                                f"Format: {connector.get('format', 'Unknown')}"
                            },
                            "category_ids": ["ev_charging"],
                            "location_ids": [beckn_loc["id"]],
                            "price": price,
                            "tags": {
                                "connector_type": connector.get('standard', 'Unknown'),
                                "power_type": connector.get('power_type', 'Unknown'),
                                "max_power": str(connector.get('max_electric_power', 0)),
                                "availability": evse.get('status', 'UNKNOWN')
                            }
                        }
                        beckn_items.append(beckn_item)

                fulfillment = {
                    "id": f"fulfillment_{ocpi_loc['id']}",
                    "type": "charging",
                    "start": {
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
                        "facilities": ocpi_loc.get('facilities', []),
                        "opening_times": ocpi_loc.get('opening_times', {}),
                        "operator": ocpi_loc.get('operator', {}).get('name', 'Unknown')
                    }
                }
                beckn_fulfillments.append(fulfillment)

            beckn_response = {
                "context": {
                    **beckn_request.context,
                    "action": "on_search",
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "message_id": str(uuid.uuid4()),
                    "bap_id": beckn_request.context.get("bap_id"),
                    "bpp_id": "ocpi_charging_network"
                },
                "message": {
                    "catalog": {
                        "descriptor": {
                            "name": "EV Charging Network",
                            "short_desc": "Electric Vehicle Charging Stations",
                            "long_desc": "Network of EV charging stations via OCPI integration"
                        },
                        "locations": beckn_locations,
                        "items": beckn_items,
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
    from beckn_modules.beckn_modules import BecknSearchRequest
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
