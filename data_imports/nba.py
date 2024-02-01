import requests
import pandas as pd

base_url = "https://www.balldontlie.io/api/v1"


# get player data
def get_player_data(player_id=472):
    url = f"{base_url}/players/{player_id}"
    response = requests.get(url)
    return response.json()


print(get_player_data())
