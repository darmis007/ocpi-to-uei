import json
import os
from typing import Any, Dict

from dotenv import load_dotenv

from beckn_ocpi_bridge import BecknOCPIBridge

# Load environment variables
load_dotenv()


def load_init_request(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:

    init_request_path = os.getenv(
        "INIT_REQUEST_PATH", os.path.join(os.path.dirname(
            __file__), "requests", "init_request.json")
    )

    # Load request
    beckn_init_request = load_init_request(init_request_path)

    # Bridge setup (OCPI client is ensured internally from env)
    bridge = BecknOCPIBridge()

    # Process and print response
    response = bridge.process_init_request(beckn_init_request)
    print(json.dumps(response, indent=2))

    # Save to responses/on_init.json
    responses_dir = os.path.join(os.path.dirname(__file__), "responses")
    os.makedirs(responses_dir, exist_ok=True)
    out_path = os.path.join(responses_dir, "on_init.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(response, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()
