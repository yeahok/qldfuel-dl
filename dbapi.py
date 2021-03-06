import psycopg2
from datetime import datetime
from datetime import timedelta
from datetime import timezone

def import_regions(db_conn, regions):
    db_cursor = db_conn.cursor()
    #region list is reversed to prevent same table foreign key error
    for region in reversed(regions):    
    #need to find internal id of region not the one given by api
        if region["GeoRegionParentId"] is not None:
            parent_geographical_level = region["GeoRegionLevel"] + 1
            db_cursor.execute("SELECT id FROM region WHERE original_id = %s AND geographical_level = %s",
                (region["GeoRegionParentId"], parent_geographical_level))
            query_return = db_cursor.fetchone()
            
            region["GeoRegionParentId"] = query_return[0]

        db_cursor.execute("UPDATE region SET name = %s, abbreviation = %s WHERE original_id = %s AND geographical_level = %s", 
            (region["Name"], region["Abbrev"], region["GeoRegionId"], region["GeoRegionLevel"]))
        
        if db_cursor.rowcount == 0:
            #if nothing was updated in the previous query insert instead
            db_cursor.execute("INSERT INTO region (name, original_id, geographical_level, abbreviation, region_parent_id, active) VALUES (%s, %s, %s, %s, %s, TRUE)",
                (region["Name"], region["GeoRegionId"], region["GeoRegionLevel"], region["Abbrev"], region["GeoRegionParentId"]))

def import_brands(db_conn, brands):
    db_cursor = db_conn.cursor()
    for brand in brands:
        db_cursor.execute("""INSERT INTO brand (id, name, active) VALUES (%s, %s, TRUE)
            ON CONFLICT(id) DO UPDATE SET name = %s""",
            (brand["BrandId"], brand["Name"], brand["Name"]))

def import_fuels(db_conn, fuels):
    db_cursor = db_conn.cursor()
    for fuel in fuels:
        db_cursor.execute("""INSERT INTO fuel (id, name, active) VALUES (%s, %s, TRUE)
            ON CONFLICT(id) DO UPDATE SET name = %s""",
            (fuel["FuelId"], fuel["Name"], fuel["Name"]))

def import_sites(db_conn, sites):
    db_cursor = db_conn.cursor()
    db_cursor.execute("SET timezone = 'utc';")
    db_cursor.execute("SET datestyle = dmy;")
    for site in sites:
        db_cursor.execute("""INSERT INTO site (id, name, brand_id, address, post_code, latitude, longitude, modified_date, active) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE)
            ON CONFLICT(id) DO UPDATE SET name = %s, brand_id = %s, address = %s, post_code = %s, latitude = %s, longitude = %s, modified_date = %s""",
            (site["S"], site["N"], site["B"], site["A"], site["P"], site["Lat"], site["Lng"], site["M"],
            site["N"], site["B"], site["A"], site["P"], site["Lat"], site["Lng"], site["M"]))

def generate_site_region(db_conn, sites):
    region_fields = ["G1", "G2", "G3", "G4", "G5"]
    db_cursor = db_conn.cursor()

    for site in sites:
        for index, field in enumerate(region_fields):
            if site[field] != 0:
                #need to find internal id of region not the one given by api
                level = index + 1
                db_cursor.execute("SELECT id FROM region WHERE original_id = %s AND geographical_level = %s",
                    (site[field], level))
                
                query_return = db_cursor.fetchone()

                insert_id = query_return[0]
                db_cursor.execute("INSERT INTO site_region (site_id, region_id) VALUES (%s, %s)",
                    (site["S"], insert_id))

#requires prices to already be imported into the database
def generate_site_fuel(db_conn, sites):
    db_cursor1 = db_conn.cursor()
    db_cursor2 = db_conn.cursor()
    for site in sites:
        db_cursor1.execute("SELECT DISTINCT fuel_id FROM price WHERE site_id = %s",
            (site["S"],))
        fuel_ids = db_cursor1.fetchall()
        for fuel_id in fuel_ids:
            db_cursor2.execute("INSERT INTO site_fuel (site_id, fuel_id, active) VALUES (%s, %s, TRUE)",
                (site["S"], fuel_id))

def parse_datetime(datetime_string):
    #need to make strings the same format before parsing
    if len(datetime_string) == 19:
        datetime_string = "".join((datetime_string, "."))
    datetime_string = datetime_string.ljust(26, '0')

    parsed_date = datetime.strptime(datetime_string, "%Y-%m-%dT%H:%M:%S.%f")
    parsed_date = parsed_date.replace(tzinfo= timezone.utc)
    return parsed_date

def import_prices_api(db_conn, prices):
    db_cursor = db_conn.cursor()
    db_cursor.execute("SET timezone = 'utc';")
    db_cursor.execute("SET datestyle = dmy;")

    new_prices_counter = 0
    unavailable_fuels_counter = 0

    for price in prices:
        if price["Price"] == 9999:
            #9999 is apparently used to denote the fuel is unavailable
            #Will not be added to the 'price' table for now
            db_cursor.execute("UPDATE site_fuel SET active = False WHERE site_id = %s AND fuel_id = %s",
                (price["SiteId"], price["FuelId"]))
            unavailable_fuels_counter += 1
        else:
            #grab last row of matching site_id and fuel_id for comparison
            db_cursor.execute("SELECT id, amount, transaction_date FROM price WHERE site_id = %s AND fuel_id = %s ORDER BY transaction_date DESC LIMIT 1",
                (price["SiteId"], price["FuelId"]))
            row = db_cursor.fetchone()

            if db_cursor.rowcount == 0 or (row[2] != parse_datetime(price["TransactionDateUtc"]) and row[1] != price["Price"]):
                if price["Price"] > 9999:
                    #assume prices above 9999 are not correct
                    active = "False"
                else:
                    active = "True"
                db_cursor.execute("""INSERT INTO price (site_id, fuel_id, collection_method, amount, transaction_date, active) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING""",
                    (price["SiteId"], price["FuelId"], price["CollectionMethod"], price["Price"], price["TransactionDateUtc"], active))
                new_prices_counter += 1
            
            #assume fuel is active again if the price isn't 9999
            db_cursor.execute("UPDATE site_fuel SET active = True WHERE site_id = %s AND fuel_id = %s",
                (price["SiteId"], price["FuelId"]))

    print("Prices inserted from api: {}".format(new_prices_counter))
    print("Unavailable fuels: {}".format(unavailable_fuels_counter))

def set_brand_active(db_conn):
    db_cursor = db_conn.cursor()
    db_cursor.execute("SELECT distinct brand_id FROM public.site WHERE active = true")
    active_brands = db_cursor.fetchall()

    db_cursor.execute("SELECT id FROM public.brand")
    brands = db_cursor.fetchall()

    for id in brands:
        if id in active_brands:
            db_cursor.execute("UPDATE brand SET active = True WHERE id = %s",
                (id))
        else:
            db_cursor.execute("UPDATE brand SET active = False WHERE id = %s",
                (id))

def import_prices_csv(db_conn, filename):
    db_cursor = db_conn.cursor()
    file_contents = open(filename, 'r')
    next(file_contents)
    db_cursor.copy_from(file_contents, 'public.price', columns=("\"site_id\"", "\"fuel_id\"", "\"collection_method\"", "\"amount\"", "\"transaction_date\"", "\"active\""), sep=",")

def import_sites_csv(db_conn, filename):
    db_cursor = db_conn.cursor()
    file_contents = open(filename, 'r')
    next(file_contents)
    db_cursor.copy_from(file_contents, 'public.sites', sep=";")

def setup_tables(db_conn, filename):
    db_cursor = db_conn.cursor()
    sqlCommands = open(filename, "r").read()
    db_cursor.execute(sqlCommands)