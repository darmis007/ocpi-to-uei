import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import uuid

logger = logging.getLogger(__name__)


class BecknOCPIBridge:
    """Bridge between Beckn and OCPI protocols"""

    def __init__(self, ocpi_client):
        self.ocpi_client = ocpi_client

    def transform_beckn_to_ocpi_query(self, beckn_request) -> Dict[str, Any]:
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
        try:
            target_lat, target_lon = map(float, target_gps.split(','))
            filtered_locations = []
            for location in locations:
                coords = location.get('coordinates', {})
                if not coords:
                    continue
                loc_lat = float(coords.get('latitude', 0))
                loc_lon = float(coords.get('longitude', 0))
                distance = self._calculate_distance(
                    target_lat, target_lon, loc_lat, loc_lon)
                if distance <= radius_km:
                    location['distance_km'] = round(distance, 2)
                    filtered_locations.append(location)
            filtered_locations.sort(
                key=lambda x: x.get('distance_km', float('inf')))
            return filtered_locations
        except Exception as e:
            logger.error(f"Error filtering locations by proximity: {str(e)}")
            return locations

    def _calculate_distance(self, lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        import math
        R = 6371
        lat1_rad = math.radians(lat1)
        lat2_rad = math.radians(lat2)
        delta_lat = math.radians(lat2 - lat1)
        delta_lon = math.radians(lon2 - lon1)
        a = (math.sin(delta_lat / 2) ** 2 +
             math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

    def transform_ocpi_to_beckn_response(self, ocpi_response: Dict[str, Any], beckn_request, tariffs: Optional[Dict[str, Dict[str, str]]] = None) -> Dict[str, Any]:
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
