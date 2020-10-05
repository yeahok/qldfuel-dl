import requests
import json
import psycopg2
import configparser
import shutil
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import os.path
import sys

import fuelscrape
import fuelapi
import dbapi


def downloadCsvList(csvList):
    for i in csvList:
        if os.path.isfile(i["filename"]):
            print("Skipping: {}".format(i["filename"]))
        else:
            print("Downloading: {}".format(i["filename"]))
            download(i["url"], i["filename"]) 

def download(url, file_name):
    with open(file_name, "wb") as file:
        response = requests.get(url)
        file.write(response.content)

def save_merged_csv(csv_list, filename):
    customHeaders = ["id","name","brand","address","suburb","state","postcode","lat", "long",
                     "fueltype", "price", "datetime"]
    with open(filename,'wb') as outFile:
        outFile.write(",".join(customHeaders).encode())
        outFile.write("\n".encode())
        for csv in csv_list:
            with open(csv["filename"],'rb') as csvread:
                next(csvread)

                for line in csvread:
                    outFile.write(line)

def save_filter_sites_csv(csv_list, filename):
    columnNames = ["SiteId", "Site_Name", "Site_Brand", "Sites_Address_Line_1", "Site_Suburb", "Site_State",
                                         "Site_Post_Code", "Site_Latitude", "Site_Longitude", "Fuel_Type", "Price", "TransactionDateutc"]
    csvContents = pd.DataFrame(columns = columnNames)
    for i in range(0, len(csv_list)):
        csvLoopContents = pd.read_csv(csv_list[i]["filename"], engine="python", names=columnNames, skiprows=1, header= 0)
        csvContents = pd.concat([csvLoopContents, csvContents], axis=0)
        csvContents = csvContents.drop_duplicates(subset=["SiteId"])
        print("Loading: {}".format(csv_list[i]["filename"]))

    print("Saving {} lines to {}".format(len(csvContents), filename))
    #need to use semicolon as delimiter because some fields contain commas 
    csvContents.to_csv(filename, index=False, columns=columnNames[0:-3], sep=";", encoding="cp1252")

def save_filter_prices_csv(csv_list, filename, api_conn):
    columnNames = ["SiteId", "Site_Name", "Site_Brand", "Sites_Address_Line_1", "Site_Suburb", "Site_State",
                                         "Site_Post_Code", "Site_Latitude", "Site_Longitude", "Fuel_Type", "Price", "TransactionDateutc"]
    csvContents = pd.DataFrame(columns = columnNames)
    for i in range(0, len(csv_list)):
        csvLoopContents = pd.read_csv(csv_list[i]["filename"], engine="python", names=columnNames, skiprows=1, header= 0)
        csvContents = pd.concat([csvLoopContents, csvContents], axis=0)
        print("Loading: {}".format(csv_list[i]["filename"]))

    site_id_list = get_site_id_list(api_conn)

    #remove prices not from qld
    csvContents = csvContents[(csvContents["SiteId"].isin(site_id_list))]

    #change from name of fuel to id
    fuel_mapping = get_fuel_mapping(api_conn)
    csvContents["Fuel_Type"].replace(fuel_mapping, inplace=True)

    csvContents["Collection_Method"] = "CSV"
    csvContents["Active"] = 1

    #drop rows that will violate the unique constraint in the table
    #assume the last duplicate row is the more 'correct' one
    csvContents.drop_duplicates(subset=["SiteId", "Fuel_Type", "TransactionDateutc"] , keep='last', inplace=True)
    
    print("Saving {} lines to {}".format(len(csvContents), filename))
    csvContents.to_csv(filename, index=False, columns=["SiteId", "Fuel_Type", "Collection_Method", "Price", "TransactionDateutc", "Active"], encoding="cp1252")

def get_fuel_mapping(api_conn):
    fuels = api_conn.get_fuels()
    fuel_mapping = {}
    for fuel in fuels:
        if fuel["FuelId"] == 5:
            fuel_mapping["PULP 95/96 RON"] = fuel["FuelId"]
        elif fuel["FuelId"] == 8:
            fuel_mapping["PULP 98 RON"] = fuel["FuelId"]
        else:
            fuel_mapping[fuel["Name"]] = fuel["FuelId"]
    return fuel_mapping

def get_site_id_list(api_conn):
    sites = api_conn.get_sites()

    def get_id(site):
        return site["S"]
    
    return list(map(get_id, sites))

def init():
    csvUrls = fuelscrape.getlinks("https://www.data.qld.gov.au/dataset/fuel-price-reporting")
    downloadCsvList(csvUrls)
    save_filter_prices_csv(csvUrls, "qldfuelprices.csv", api_conn)

    dbapi.setup_tables(db_conn, "setuptables.sql")

    regions = api_conn.get_regions()
    dbapi.import_regions(db_conn, regions)

    brands = api_conn.get_brands()
    dbapi.import_brands(db_conn, brands)

    fuels = api_conn.get_fuels()
    dbapi.import_fuels(db_conn, fuels)

    sites = api_conn.get_sites()
    dbapi.import_sites(db_conn, sites)

    dbapi.generate_site_region(db_conn, sites)

    dbapi.import_prices_csv(db_conn, "qldfuelprices.csv")

    dbapi.generate_site_fuel(db_conn, sites)

    prices = api_conn.get_prices()
    dbapi.import_prices_api(db_conn, prices)

def update():
    regions = api_conn.get_regions()
    dbapi.import_regions(db_conn, regions)

    brands = api_conn.get_brands()
    dbapi.import_brands(db_conn, brands)

    fuels = api_conn.get_fuels()
    dbapi.import_fuels(db_conn, fuels)

    sites = api_conn.get_sites()
    dbapi.import_sites(db_conn, sites)

    dbapi.set_brand_active(db_conn)

def price_only():
    prices = api_conn.get_prices()
    dbapi.import_prices_api(db_conn, prices)

config = configparser.ConfigParser()
config.read('settings.ini')

api_conn = fuelapi.Connection(config["api"]["key"])

db_conn = psycopg2.connect(host=config["postgres"]["host"], 
                            port=config["postgres"]["port"], 
                            dbname=config["postgres"]["dbname"], 
                            user=config["postgres"]["user"],
                            password=config["postgres"]["password"])
db_conn.autocommit = True

run = sys.argv[1]

if run == "init":
    init()
elif run == "update":
    update()
elif run == "price":
    price_only()
else:
    print("wrong argument")
    sys.exit()

db_conn.close()
print("Import done")