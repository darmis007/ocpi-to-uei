import logging
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class BecknLocation:
    """Beckn location structure"""
    gps: str
    address: Optional[str] = None
    city: Optional[str] = None
    country: Optional[str] = None
    area_code: Optional[str] = None


@dataclass
class BecknItem:
    """Beckn item structure"""
    id: str
    descriptor: Dict[str, Any]
    location_ids: List[str]
    category_ids: Optional[List[str]] = None
    fulfillment_ids: Optional[List[str]] = None


@dataclass
class BecknSearchRequest:
    """Beckn search request structure"""
    context: Dict[str, Any]
    message: Dict[str, Any]

    def get_location_criteria(self) -> Optional[Dict[str, Any]]:
        """Extract location criteria from search request"""
        if not isinstance(self.message, dict):
            logger.error(
                f"Expected dict for message, got {type(self.message)}: {self.message}")
            return {}
        intent = self.message.get('intent', {})
        if not isinstance(intent, dict):
            logger.error(
                f"Expected dict for intent, got {type(intent)}: {intent}")
            return {}
        fulfillment = intent.get('fulfillment', {})
        if not isinstance(fulfillment, dict):
            logger.error(
                f"Expected dict for fulfillment, got {type(fulfillment)}: {fulfillment}")
            return {}
        return fulfillment.get('start', {}).get('location', {})

    def get_category_criteria(self) -> Optional[str]:
        """Extract category criteria from search request"""
        if not isinstance(self.message, dict):
            logger.error(
                f"Expected dict for message, got {type(self.message)}: {self.message}")
            return None
        intent = self.message.get('intent', {})
        if not isinstance(intent, dict):
            logger.error(
                f"Expected dict for intent, got {type(intent)}: {intent}")
            return None
        item = intent.get('item', {})
        if not isinstance(item, dict):
            logger.error(f"Expected dict for item, got {type(item)}: {item}")
            return None
        category = item.get('category', {})
        if not isinstance(category, dict):
            logger.error(
                f"Expected dict for category, got {type(category)}: {category}")
            return None
        return category.get('id')


@dataclass
class BecknSelectRequest:
    """Beckn select request structure"""
    context: Dict[str, Any]
    message: Dict[str, Any]

    def get_selected_item(self) -> Optional[Dict[str, Any]]:
        """Extract selected item from select request"""
        return self.message.get("order", {}).get("items", [{}])[0]  # Simplified: first item

    def get_selected_location(self) -> Optional[Dict[str, Any]]:
        """Extract selected location from select request"""
        return self.message.get("order", {}).get("fulfillments", [{}])[0].get("start", {}).get("location", {})

    def get_user_token(self) -> Optional[str]:
        """Extract user token (for authorization)"""
        return self.message.get("order", {}).get("authorization", {}).get("token")


@dataclass
class BecknOnSelectResponse:
    """Beckn on_select response structure"""
    context: Dict[str, Any]
    message: Dict[str, Any]

    def get_order_details(self) -> Optional[Dict[str, Any]]:
        """Extract order details from on_select response"""
        return self.message.get("order", {})

    def get_quote_details(self) -> Optional[Dict[str, Any]]:
        """Extract quote details from on_select response"""
        return self.message.get("order", {}).get("quote", {})

    def get_payment_details(self) -> Optional[Dict[str, Any]]:
        """Extract payment details from on_select response"""
        return self.message.get("order", {}).get("payment", {})


@dataclass
class BecknConfirmRequest:
    """Beckn confirm request structure"""
    context: Dict[str, Any]
    message: Dict[str, Any]

    def get_session_id(self) -> Optional[str]:
        """Extract session/order id from confirm request"""
        return self.message.get("order", {}).get("id")


@dataclass
class BecknStatusRequest:
    """Beckn status request structure"""
    context: Dict[str, Any]
    message: Dict[str, Any]

    def get_session_id(self) -> Optional[str]:
        """Extract session/order id from status request"""
        return self.message.get("order", {}).get("id")


@dataclass
class BecknUpdateRequest:
    """Beckn update request structure"""
    context: Dict[str, Any]
    message: Dict[str, Any]

    def get_session_id(self) -> Optional[str]:
        """Extract session/order id from update request"""
        return self.message.get("order", {}).get("id")

    def get_update_data(self) -> Dict[str, Any]:
        """Extract update data (e.g., new state, additional info)"""
        return self.message.get("order", {}).get("update", {})


@dataclass
class BecknCDRRequest:
    """Beckn CDR (Charge Detail Record) request structure"""
    context: Dict[str, Any]
    message: Dict[str, Any]

    def get_session_id(self) -> Optional[str]:
        """Extract session/order id from CDR request"""
        return self.message.get("order", {}).get("id")

    def get_billing_data(self) -> Dict[str, Any]:
        """Extract billing data from CDR request"""
        return self.message.get("order", {}).get("billing", {})

    def get_payment_data(self) -> Dict[str, Any]:
        """Extract payment data from CDR request"""
        return self.message.get("order", {}).get("payment", {})
