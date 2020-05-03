import requests
import json
import psycopg2
import configparser
import shutil
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd

def getlinks(page_url):
    colpage = requests.get(page_url)
    soup = BeautifulSoup(colpage.text, 'html.parser')

    test = soup.find('script', {"type":"application/ld+json"})
    fulljson = json.loads(test.get_text())

    linksList = parse_json(fulljson)

    linksList.sort(key=sortListDict)

    return linksList

def sortListDict(val): 
    return val["date"] 

def parse_json(json):
    linksList = []
    for objs in json["@graph"]:
        if "Prices" in objs["schema:name"]:
            name = objs["schema:name"].strip()
            year = name.split(" ")[-1]
            month = name.split(" ")[-2]

            objDate = datetime.strptime(year + month, '%Y%B')

            tempdict = {}
            tempdict["date"] = datetime.strftime(objDate,'%Y-%m')
            tempdict["url"] = objs["schema:url"]
            tempdict["filename"]= "{}.csv".format(tempdict["date"])
            linksList.append(tempdict)
    return linksList

def downloadCsvList(csvList):
    for i in csvList:
        print("Downloading: {}".format(i["filename"]))
        download(i["url"], i["filename"]) 

def download(url, file_name):
    with open(file_name, "wb") as file:
        response = requests.get(url)
        file.write(response.content)

def import_prices_csv(db_cursor, filename):
    file_contents = open(filename, 'r')
    next(file_contents)
    db_cursor.copy_from(file_contents, 'public.prices', columns=("\"SiteId\"", "\"Fuel_Type\"", "\"Price\"", "\"TransactionDateutc\""), sep=",")

def import_sites_csv(db_cursor, filename):
    file_contents = open(filename, 'r')
    next(file_contents)
    db_cursor.copy_from(file_contents, 'public.sites', sep=";")

def save_merged_csv(csv_list, filename):
    customHeaders = ["Id","Name","Brand","Address","Suburb","State","PostCode","Lat", "Long",
                     "FuelType", "Price", "DateTime"]
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
        csvLoopContents = pd.read_csv(csv_list[i]["filename"], engine="python", names=columnNames, header= 0)
        csvContents = pd.concat([csvLoopContents, csvContents], axis=0)
        csvContents = csvContents.drop_duplicates(subset=["SiteId"])
        print("Loading: {}".format(csv_list[i]["filename"]))

    print("Saving {} lines to {}".format(len(csvContents), filename))
    #need to use semicolon as delimiter because some fields contain commas 
    csvContents.to_csv(filename, index=False, columns=columnNames[0:-3], sep=";", encoding="cp1252")

def save_filter_prices_csv(csv_list, filename):
    columnNames = ["SiteId", "Site_Name", "Site_Brand", "Sites_Address_Line_1", "Site_Suburb", "Site_State",
                                         "Site_Post_Code", "Site_Latitude", "Site_Longitude", "Fuel_Type", "Price", "TransactionDateutc"]
    csvContents = pd.DataFrame(columns = columnNames)
    for i in range(0, len(csv_list)):
        csvLoopContents = pd.read_csv(csv_list[i]["filename"], engine="python", names=columnNames, header= 0)
        csvContents = pd.concat([csvLoopContents, csvContents], axis=0)
        print("Loading: {}".format(csv_list[i]["filename"]))
    
    print("Saving {} lines to {}".format(len(csvContents), filename))
    csvContents.to_csv(filename, index=False, columns=["SiteId", "Fuel_Type", "Price", "TransactionDateutc"], encoding="cp1252")

def setup_tables(db_cursor, filename):
    sqlCommands = open(filename, "r").read()
    db_cursor.execute(sqlCommands)



config = configparser.ConfigParser()
config.read('settings.ini')

csvUrls = getlinks("https://www.data.qld.gov.au/dataset/fuel-price-reporting")
downloadCsvList(csvUrls)

save_merged_csv(csvUrls, "qldfuelmergeall.csv")

save_filter_sites_csv(csvUrls, "qldfuelsites.csv")
save_filter_prices_csv(csvUrls, "qldfuelprices.csv")

db_conn = psycopg2.connect(host=config["postgres"]["host"], 
                            port=config["postgres"]["port"], 
                            dbname=config["postgres"]["dbname"], 
                            user=config["postgres"]["user"],
                            password=config["postgres"]["password"])
db_conn.autocommit = True
db_cursor = db_conn.cursor()

setup_tables(db_cursor,"setuptables.sql")

import_sites_csv(db_cursor, "qldfuelsites.csv")
import_prices_csv(db_cursor, "qldfuelprices.csv")

db_cursor.close()
db_conn.close()
print("Import done")