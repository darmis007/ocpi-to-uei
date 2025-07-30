import requests


def get_ocpi_locations(base_url: str, token: str) -> dict:
    """Get OCPI locations from the specified endpoint"""
    url = f"{base_url}/ocpi/cpo/2.2.1/locations"

    headers = {
        'Authorization': f'Token {token}'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()


if __name__ == "__main__":

    try:
        locations = get_ocpi_locations()
        print(locations)
    except requests.RequestException as e:
        print(f"Error fetching locations: {e}")
