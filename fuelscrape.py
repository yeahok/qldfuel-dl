import requests
import json
from datetime import datetime
from bs4 import BeautifulSoup

def getlinks(page_url):
    colpage = requests.get(page_url)
    soup = BeautifulSoup(colpage.text, 'html.parser')

    test = soup.find('script', {"type":"application/ld+json"})
    fulljson = json.loads(test.string)

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