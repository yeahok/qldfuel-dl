import psycopg2

def import_regions(db_cursor, regions):
    #region list is reversed to prevent same table foreign key error
    for region in reversed(regions):    
    #need to find internal id of region not the one given by api
        if region["GeoRegionParentId"] is not None:
            parent_geographical_level = region["GeoRegionLevel"] + 1
            db_cursor.execute("SELECT id FROM region WHERE original_id = %s AND geographical_level = %s",
                (region["GeoRegionParentId"], parent_geographical_level))
            query_return = db_cursor.fetchone()
            
            region["GeoRegionParentId"] = query_return[0]

        db_cursor.execute("INSERT INTO region (name, original_id, geographical_level, abbrevation, region_parent_id, active) VALUES (%s, %s, %s, %s, %s, TRUE)",
            (region["Name"], region["GeoRegionId"], region["GeoRegionLevel"], region["Abbrev"], region["GeoRegionParentId"]))

def import_brands(db_cursor, brands):
    for brand in brands:
        db_cursor.execute("INSERT INTO brand (id, name, active) VALUES (%s, %s, TRUE)",
            (brand["BrandId"], brand["Name"]))

def import_fuels(db_cursor, fuels):
    for fuel in fuels:
        db_cursor.execute("INSERT INTO fuel (id, name, active) VALUES (%s, %s, TRUE)",
            (fuel["FuelId"], fuel["Name"]))

def import_sites(db_cursor, sites):
    db_cursor.execute("SET timezone = 'utc';")
    db_cursor.execute("SET datestyle = dmy;")
    for site in sites:
        db_cursor.execute("INSERT INTO site (id, name, brand_id, address, post_code, latitude, longitude, modified_date, active) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE)",
            (site["S"], site["N"], site["B"], site["A"], site["P"], site["Lat"], site["Lng"], site["M"]))

def generate_site_region(db_cursor, sites):
    region_fields = ["G1", "G2", "G3", "G4", "G5"]

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
            db_cursor2.execute("INSERT INTO site_fuel (site_id, fuel_id) VALUES (%s, %s)",
                (site["S"], fuel_id))

def import_prices_csv(db_cursor, filename):
    file_contents = open(filename, 'r')
    next(file_contents)
    db_cursor.copy_from(file_contents, 'public.price', columns=("\"site_id\"", "\"fuel_id\"", "\"collection_method\"", "\"amount\"", "\"transaction_date\""), sep=",")

def import_sites_csv(db_cursor, filename):
    file_contents = open(filename, 'r')
    next(file_contents)
    db_cursor.copy_from(file_contents, 'public.sites', sep=";")

def setup_tables(db_cursor, filename):
    sqlCommands = open(filename, "r").read()
    db_cursor.execute(sqlCommands)