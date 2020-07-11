import requests
import json

class Connection:
    def __init__(self, auth_key):
        self.session = requests.Session()
        self.session.auth = (ApiAuth(auth_key))
        self.base_url = "https://fppdirectapi-prod.fuelpricesqld.com.au/"
 
    def get_fuels(self):
        fuels_request = self.session.get("{}Subscriber/GetCountryFuelTypes?countryId=21".format(self.base_url))
        return fuels_request.json()["Fuels"]

    def get_prices(self):
        prices_request = self.session.get("{}Price/GetSitesPrices?countryId=21&geoRegionLevel=3&geoRegionId=1".format(self.base_url))
        return prices_request.json()["SitePrices"]

    def get_brands(self):
        brands_request = self.session.get("{}Subscriber/GetCountryBrands?countryId=21".format(self.base_url))
        return brands_request.json()["Brands"]

    def get_sites(self):
        sites_request = self.session.get("{}Subscriber/GetFullSiteDetails?countryId=21&geoRegionLevel=3&geoRegionId=1".format(self.base_url))
        return sites_request.json()["S"]
    
    def get_regions(self):
        regions_request = self.session.get("{}Subscriber/GetCountryGeographicRegions?countryId=21".format(self.base_url))
        return regions_request.json()["GeographicRegions"]

class ApiAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        auth_parameter = "FPDAPI SubscriberToken={}".format(self.token)
        r.headers["Authorization"] = auth_parameter
        return r