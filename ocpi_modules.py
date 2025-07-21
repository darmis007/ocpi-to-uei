import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class OCPIStatus(Enum):
    """OCPI status codes"""
    SUCCESS = 1000
    CLIENT_ERROR = 2000
    SERVER_ERROR = 3000


@dataclass
class OCPILocation:
    """OCPI Location structure"""
    country_code: str
    party_id: str
    id: str
    publish: bool
    name: Optional[str]
    address: str
    city: str
    country: str
    coordinates: Dict[str, str]  # latitude, longitude
    last_updated: str  # Moved this up with other required fields
    postal_code: Optional[str] = None
    state: Optional[str] = None
    related_locations: Optional[List[Dict]] = None
    parking_type: Optional[str] = None
    evses: Optional[List[Dict]] = None
    directions: Optional[List[Dict]] = None
    operator: Optional[Dict] = None
    suboperator: Optional[Dict] = None
    owner: Optional[Dict] = None
    facilities: Optional[List[str]] = None
    time_zone: Optional[str] = None
    opening_times: Optional[Dict] = None
    charging_when_closed: Optional[bool] = None
    images: Optional[List[Dict]] = None
    energy_mix: Optional[Dict] = None


@dataclass
class OCPICDR:
    """OCPI CDR (Charge Detail Record) structure"""
    # Required fields (no default values) - must come first
    country_code: str
    party_id: str
    id: str
    start_date_time: str
    end_date_time: str
    session_id: str
    cdr_token: Dict[str, Any]
    auth_method: str
    cdr_location: Dict[str, Any]
    currency: str
    last_updated: str

    # Optional fields (with default values) - must come after required fields
    authorization_reference: Optional[str] = None
    meter_id: Optional[str] = None
    tariffs: Optional[List[Dict]] = None
    charging_periods: Optional[List[Dict]] = None
    signed_data: Optional[Dict] = None
    total_cost: Optional[Dict] = None
    total_fixed_cost: Optional[Dict] = None
    total_energy: Optional[float] = None
    total_energy_cost: Optional[Dict] = None
    total_time: Optional[float] = None
    total_time_cost: Optional[Dict] = None
    total_parking_time: Optional[float] = None
    total_parking_cost: Optional[Dict] = None
    total_reservation_cost: Optional[Dict] = None
    remark: Optional[str] = None
    invoice_reference_id: Optional[str] = None
    credit: Optional[bool] = None
    credit_reference_id: Optional[str] = None


class OCPIClient:
    """OCPI API client for locations"""

    def __init__(self, base_url: str, token: str, version: str = "2.2", mock_mode: bool = False):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.version = version
        self.mock_mode = mock_mode
        self.headers = {
            'Authorization': f'Token {token}',
            'Content-Type': 'application/json',
            'OCPI-from-country-code': 'IN',  # Adjust as needed
            'OCPI-from-party-id': 'BEC',     # Adjust as needed
            'OCPI-to-country-code': 'IN',    # Adjust as needed
            'OCPI-to-party-id': 'CPO'       # Adjust as needed
        }

    def _get_mock_locations_response(self) -> Dict[str, Any]:
        from datetime import datetime, timezone
        return {
            "data": [
                {
                    "country_code": "IN",
                    "party_id": "CPO",
                    "id": "LOC001",
                    "publish": True,
                    "name": "Central Mall Charging Hub",
                    "address": "123 MG Road, Bangalore",
                    "city": "Bangalore",
                    "postal_code": "560001",
                    "state": "Karnataka",
                    "country": "IND",
                    "coordinates": {
                        "latitude": "12.9716",
                        "longitude": "77.5946"
                    },
                    "time_zone": "Asia/Kolkata",
                    "last_updated": "2023-12-15T10:30:00Z",
                    "operator": {
                        "name": "Green Energy Solutions",
                        "website": "https://greenenergy.com"
                    },
                    "facilities": ["PARKING", "RESTAURANT", "SHOPPING"],
                    "evses": [
                        {
                            "uid": "EVSE001",
                            "evse_id": "IN*CPO*E001",
                            "status": "AVAILABLE",
                            "connectors": [
                                {
                                    "id": "1",
                                    "standard": "CCS_2",
                                    "format": "CABLE",
                                    "power_type": "DC",
                                    "max_voltage": 400,
                                    "max_amperage": 125,
                                    "max_electric_power": 50000,
                                    "tariff_ids": ["TARIFF_001"],
                                    "last_updated": "2023-12-15T10:30:00Z"
                                }
                            ],
                            "last_updated": "2023-12-15T10:30:00Z"
                        }
                    ]
                },
                {
                    "country_code": "IN",
                    "party_id": "CPO",
                    "id": "LOC002",
                    "publish": True,
                    "name": "Tech Park Fast Charging",
                    "address": "45 Electronic City, Bangalore",
                    "city": "Bangalore",
                    "postal_code": "560100",
                    "state": "Karnataka",
                    "country": "IND",
                    "coordinates": {
                        "latitude": "12.8456",
                        "longitude": "77.6621"
                    },
                    "time_zone": "Asia/Kolkata",
                    "last_updated": "2023-12-15T10:30:00Z",
                    "operator": {
                        "name": "Power Grid Charging",
                        "website": "https://powergrid.com"
                    },
                    "facilities": ["PARKING", "WIFI"],
                    "evses": [
                        {
                            "uid": "EVSE002",
                            "evse_id": "IN*CPO*E002",
                            "status": "AVAILABLE",
                            "connectors": [
                                {
                                    "id": "1",
                                    "standard": "CHADEMO",
                                    "format": "CABLE",
                                    "power_type": "DC",
                                    "max_voltage": 500,
                                    "max_amperage": 100,
                                    "max_electric_power": 50000,
                                    "tariff_ids": ["TARIFF_002"],
                                    "last_updated": "2023-12-15T10:30:00Z"
                                },
                                {
                                    "id": "2",
                                    "standard": "TYPE_2",
                                    "format": "SOCKET",
                                    "power_type": "AC_3_PHASE",
                                    "max_voltage": 400,
                                    "max_amperage": 32,
                                    "max_electric_power": 22000,
                                    "tariff_ids": ["TARIFF_003"],
                                    "last_updated": "2023-12-15T10:30:00Z"
                                }
                            ],
                            "last_updated": "2023-12-15T10:30:00Z"
                        }
                    ]
                },
                {
                    "country_code": "IN",
                    "party_id": "CPO",
                    "id": "LOC003",
                    "publish": True,
                    "name": "Airport Express Charging",
                    "address": "Terminal 1, Kempegowda International Airport",
                    "city": "Bangalore",
                    "postal_code": "560300",
                    "state": "Karnataka",
                    "country": "IND",
                    "coordinates": {
                        "latitude": "13.1986",
                        "longitude": "77.7066"
                    },
                    "time_zone": "Asia/Kolkata",
                    "last_updated": "2023-12-15T10:30:00Z",
                    "operator": {
                        "name": "Airport Charging Network",
                        "website": "https://airportcharging.com"
                    },
                    "facilities": ["PARKING", "RESTAURANT", "HOTEL"],
                    "evses": [
                        {
                            "uid": "EVSE003",
                            "evse_id": "IN*CPO*E003",
                            "status": "CHARGING",
                            "connectors": [
                                {
                                    "id": "1",
                                    "standard": "CCS_2",
                                    "format": "CABLE",
                                    "power_type": "DC",
                                    "max_voltage": 800,
                                    "max_amperage": 200,
                                    "max_electric_power": 150000,
                                    "tariff_ids": ["TARIFF_004"],
                                    "last_updated": "2023-12-15T10:30:00Z"
                                }
                            ],
                            "last_updated": "2023-12-15T10:30:00Z"
                        }
                    ]
                }
            ],
            "status_code": 1000,
            "status_message": "Success",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }

    def _get_mock_tariffs(self) -> Dict[str, Dict[str, str]]:
        return {
            "TARIFF_001": {"currency": "INR", "price": "10.00", "desc": "Fast DC charging"},
            "TARIFF_002": {"currency": "INR", "price": "8.00", "desc": "CHAdeMO charging"},
            "TARIFF_003": {"currency": "INR", "price": "5.00", "desc": "Type 2 AC charging"},
            "TARIFF_004": {"currency": "INR", "price": "15.00", "desc": "Ultra-fast DC charging"},
        }

    def _generate_mock_cdr(self, session_id: str, session_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Generate a mock CDR for a completed session"""
        from datetime import datetime, timezone, timedelta
        import uuid

        # Use session data if provided, otherwise create mock session data
        if not session_data:
            session_data = {
                "id": session_id,
                "location_id": "LOC001",
                "evse_uid": "EVSE001",
                "connector_id": "1",
                "status": "COMPLETED",
                "start_datetime": (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
                "end_datetime": datetime.now(timezone.utc).isoformat(),
                "kwh": 25.5,
                "auth_method": "AUTH_REQUEST",
                "authorization_reference": "AUTHREF123"
            }

        start_time = session_data.get(
            "start_datetime", datetime.now(timezone.utc).isoformat())
        end_time = session_data.get(
            "end_datetime", datetime.now(timezone.utc).isoformat())
        energy_consumed = session_data.get("kwh", 25.5)

        # Calculate costs based on energy consumed
        rate_per_kwh = 10.0  # INR per kWh
        total_energy_cost = energy_consumed * rate_per_kwh

        return {
            "country_code": "IN",
            "party_id": "CPO",
            "id": f"CDR_{uuid.uuid4().hex[:8].upper()}",
            "start_date_time": start_time,
            "end_date_time": end_time,
            "session_id": session_id,
            "cdr_token": {
                "country_code": "IN",
                "party_id": "BEC",
                "uid": "USER_TOKEN_123",
                "type": "RFID",
                "contract_id": "CONTRACT_123"
            },
            "auth_method": session_data.get("auth_method", "AUTH_REQUEST"),
            "cdr_location": {
                "id": session_data.get("location_id", "LOC001"),
                "name": "Central Mall Charging Hub",
                "address": "123 MG Road, Bangalore",
                "city": "Bangalore",
                "postal_code": "560001",
                "state": "Karnataka",
                "country": "IND",
                "coordinates": {
                    "latitude": "12.9716",
                    "longitude": "77.5946"
                },
                "evse_uid": session_data.get("evse_uid", "EVSE001"),
                "evse_id": "IN*CPO*E001",
                "connector_id": session_data.get("connector_id", "1"),
                "connector_standard": "CCS_2",
                "connector_format": "CABLE",
                "connector_power_type": "DC"
            },
            "currency": "INR",
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "authorization_reference": session_data.get("authorization_reference", "AUTHREF123"),
            "meter_id": "METER_001",
            "tariffs": [
                {
                    "country_code": "IN",
                    "party_id": "CPO",
                    "id": "TARIFF_001",
                    "currency": "INR",
                    "type": "REGULAR",
                    "tariff_alt_text": [
                        {
                            "language": "en",
                            "text": "Fast DC charging tariff"
                        }
                    ],
                    "elements": [
                        {
                            "price_components": [
                                {
                                    "type": "ENERGY",
                                    "price": rate_per_kwh,
                                    "vat": 18.0,
                                    "step_size": 1
                                }
                            ]
                        }
                    ],
                    "last_updated": datetime.now(timezone.utc).isoformat()
                }
            ],
            "charging_periods": [
                {
                    "start_date_time": start_time,
                    "dimensions": [
                        {
                            "type": "ENERGY",
                            "volume": energy_consumed
                        },
                        {
                            "type": "TIME",
                            "volume": 2.0  # 2 hours
                        }
                    ],
                    "tariff_id": "TARIFF_001"
                }
            ],
            "total_cost": {
                "excl_vat": total_energy_cost,
                "incl_vat": total_energy_cost * 1.18
            },
            "total_fixed_cost": {
                "excl_vat": 0.0,
                "incl_vat": 0.0
            },
            "total_energy": energy_consumed,
            "total_energy_cost": {
                "excl_vat": total_energy_cost,
                "incl_vat": total_energy_cost * 1.18
            },
            "total_time": 2.0,
            "total_time_cost": {
                "excl_vat": 0.0,
                "incl_vat": 0.0
            },
            "total_parking_time": 0.0,
            "total_parking_cost": {
                "excl_vat": 0.0,
                "incl_vat": 0.0
            },
            "total_reservation_cost": {
                "excl_vat": 0.0,
                "incl_vat": 0.0
            },
            "remark": f"Charging session completed successfully. Energy delivered: {energy_consumed} kWh",
            "invoice_reference_id": f"INV_{uuid.uuid4().hex[:8].upper()}",
            "credit": False
        }

    def get_locations(self,
                      country_code: Optional[str] = None,
                      party_id: Optional[str] = None,
                      location_id: Optional[str] = None,
                      date_from: Optional[str] = None,
                      date_to: Optional[str] = None,
                      offset: int = 0,
                      limit: int = 50) -> Dict[str, Any]:
        import requests
        from datetime import datetime, timezone
        if self.mock_mode:
            logger.info("Using mock OCPI response")
            mock_response = self._get_mock_locations_response()
            locations = mock_response["data"]
            if location_id:
                locations = [
                    loc for loc in locations if loc["id"] == location_id]
            start_idx = offset
            end_idx = offset + limit
            locations = locations[start_idx:end_idx]
            mock_response["data"] = locations
            return mock_response
        try:
            if location_id and country_code and party_id:
                url = f"{self.base_url}/{self.version}/locations/{country_code}/{party_id}/{location_id}"
            else:
                url = f"{self.base_url}/{self.version}/locations"
            params = {}
            if date_from:
                params['date_from'] = date_from
            if date_to:
                params['date_to'] = date_to
            if offset > 0:
                params['offset'] = offset
            if limit != 50:
                params['limit'] = limit
            logger.info(f"Making OCPI request to: {url}")
            logger.info(f"Query params: {params}")
            response = requests.get(
                url, headers=self.headers, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logger.error(f"OCPI API request failed: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in OCPI request: {str(e)}")
            raise

    def initiate_session(self, location_id: str, evse_uid: str, connector_id: str, token: str, **kwargs) -> dict:
        """
        Initiate a charging session via OCPI.
        For mock_mode, return a mock response.
        """
        if self.mock_mode:
            from datetime import datetime, timezone
            return {
                "id": "SESSION123",
                "location_id": location_id,
                "evse_uid": evse_uid,
                "connector_id": connector_id,
                "token": token,
                "status": "PENDING",
                "start_datetime": datetime.now(timezone.utc).isoformat(),
                "kwh": 0.0,
                "auth_method": "AUTH_REQUEST",
                "authorization_reference": "AUTHREF123",
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        # Real implementation (pseudo-code, adapt as per actual OCPI API)
        import requests
        url = f"{self.base_url}/{self.version}/sessions"
        session_payload = {
            "location_id": location_id,
            "evse_uid": evse_uid,
            "connector_id": connector_id,
            "token": token,
            # Add other OCPI session fields as needed
        }
        response = requests.post(
            url, headers=self.headers, json=session_payload, timeout=30)
        response.raise_for_status()
        return response.json()

    def confirm_session(self, session_id: str) -> dict:
        """
        Confirm a charging session via OCPI.
        For mock_mode, return a mock response.
        """
        if self.mock_mode:
            from datetime import datetime, timezone
            return {
                "id": session_id,
                "status": "ACTIVE",
                "confirmation_time": datetime.now(timezone.utc).isoformat(),
                "message": "Session confirmed successfully"
            }
        # Real implementation (pseudo-code, adapt as per actual OCPI API)
        import requests
        url = f"{self.base_url}/{self.version}/sessions/{session_id}/confirm"
        response = requests.post(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_session_status(self, session_id: str) -> dict:
        """
        Fetch the status of a charging session via OCPI.
        For mock_mode, return a mock response.
        """
        if self.mock_mode:
            from datetime import datetime, timezone
            return {
                "id": session_id,
                "status": "ACTIVE",
                "start_datetime": datetime.now(timezone.utc).isoformat(),
                "kwh": 5.2,
                "auth_method": "AUTH_REQUEST",
                "authorization_reference": "AUTHREF123",
                "last_updated": datetime.now(timezone.utc).isoformat()
            }
        # Real implementation (pseudo-code, adapt as per actual OCPI API)
        import requests
        url = f"{self.base_url}/{self.version}/sessions/{session_id}"
        response = requests.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def update_session(self, session_id: str, update_data: dict) -> dict:
        """
        Update a charging session via OCPI.
        For mock_mode, return a mock response.
        """
        if self.mock_mode:
            from datetime import datetime, timezone
            return {
                "id": session_id,
                "status": update_data.get("status", "UPDATED"),
                "updated_fields": update_data,
                "last_updated": datetime.now(timezone.utc).isoformat(),
                "message": "Session updated successfully"
            }
        # Real implementation (pseudo-code, adapt as per actual OCPI API)
        import requests
        url = f"{self.base_url}/{self.version}/sessions/{session_id}"
        response = requests.put(
            url, headers=self.headers, json=update_data, timeout=30)
        response.raise_for_status()
        return response.json()

    def generate_cdr(self, session_id: str, session_data: Optional[Dict[str, Any]] = None) -> dict:
        """
        Generate a CDR for a completed session.
        For mock_mode, return a mock CDR.
        """
        if self.mock_mode:
            logger.info(f"Generating mock CDR for session: {session_id}")
            return self._generate_mock_cdr(session_id, session_data)

        # Real implementation would typically fetch session data and generate CDR
        import requests
        url = f"{self.base_url}/{self.version}/sessions/{session_id}/cdr"
        response = requests.post(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()

    def push_cdr(self, cdr_data: dict) -> dict:
        """
        Push a CDR to the OCPI network.
        For mock_mode, return a mock success response.
        """
        if self.mock_mode:
            from datetime import datetime, timezone
            logger.info(
                f"Mock CDR push for CDR ID: {cdr_data.get('id', 'Unknown')}")
            return {
                "status_code": 1000,
                "status_message": "CDR pushed successfully",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "data": {
                    "cdr_id": cdr_data.get("id"),
                    "session_id": cdr_data.get("session_id"),
                    "total_cost": cdr_data.get("total_cost", {}),
                    "invoice_reference_id": cdr_data.get("invoice_reference_id")
                }
            }

        # Real implementation
        import requests
        url = f"{self.base_url}/{self.version}/cdrs"
        response = requests.post(
            url, headers=self.headers, json=cdr_data, timeout=30)
        response.raise_for_status()
        return response.json()

    def get_cdr(self, cdr_id: str) -> dict:
        """
        Retrieve a specific CDR by ID.
        For mock_mode, return a mock CDR.
        """
        if self.mock_mode:
            logger.info(f"Retrieving mock CDR: {cdr_id}")
            return self._generate_mock_cdr(f"SESSION_{cdr_id}")

        # Real implementation
        import requests
        url = f"{self.base_url}/{self.version}/cdrs/{cdr_id}"
        response = requests.get(url, headers=self.headers, timeout=30)
        response.raise_for_status()
        return response.json()
