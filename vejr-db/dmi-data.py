import requests

# temp_dry, temp_dew, humidity, observed

# url = "https://opendataapi.dmi.dk/v2/climateData/collections/stationValue/items"
url = "https://opendataapi.dmi.dk/v2/metObs/collections/observation/items?stationId=06181"
response = requests.get(url).json()

print(response)