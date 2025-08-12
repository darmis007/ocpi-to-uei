import json
import os
from typing import Any, Dict

from dotenv import load_dotenv
from beckn_ocpi_bridge import BecknOCPIBridge

# Load environment variables
load_dotenv()


def load_on_init_response(path: str) -> Dict[str, Any]:
    """Load the on_init response from the specified path."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_confirm_request(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    # Load on_init response
    on_init_path = os.getenv(
        "ON_INIT_PATH",
        os.path.join(os.path.dirname(__file__), "responses", "on_init.json")
    )

    on_init_response = load_on_init_response(on_init_path)

    # Create bridge and generate confirm request
    bridge = BecknOCPIBridge()
    confirm_request = bridge.create_confirm_request_from_on_init(
        on_init_response)

    # Save to requests/confirm_request.json
    requests_dir = os.path.join(os.path.dirname(__file__), 'requests')
    os.makedirs(requests_dir, exist_ok=True)
    confirm_request_path = os.path.join(
        requests_dir, "confirm_request.json")
    with open(confirm_request_path, "w", encoding="utf-8") as f:
        json.dump(confirm_request, f, indent=2, ensure_ascii=False)
    print(f"Confirm request saved to: {confirm_request_path}")

    # Process confirm request to generate on_confirm response
    on_confirm_response = bridge.process_confirm_request(confirm_request)

    # Save on_confirm response
    responses_dir = os.path.join(os.path.dirname(__file__), 'responses')
    os.makedirs(responses_dir, exist_ok=True)
    on_confirm_path = os.path.join(responses_dir, 'on_confirm.json')
    with open(on_confirm_path, 'w', encoding='utf-8') as f:
        json.dump(on_confirm_response, f, indent=2, ensure_ascii=False)
    print(f"on_confirm response saved to: {on_confirm_path}")


if __name__ == "__main__":
    main()
