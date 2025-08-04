import requests


def get_ocpi_tariffs(base_url: str, token: str) -> dict:
    """Get OCPI tariffs from the specified endpoint"""
    url = f"{base_url}/ocpi/cpo/2.2.1/tariffs"

    headers = {
        'Authorization': f'Token {token}'
    }

    response = requests.get(url, headers=headers)
    response.raise_for_status()

    return response.json()


if __name__ == "__main__":

    try:
        tariffs = get_ocpi_tariffs()
        print(tariffs)
    except requests.RequestException as e:
        print(f"Error fetching tariffs: {e}")
