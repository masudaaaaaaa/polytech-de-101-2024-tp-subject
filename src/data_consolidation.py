import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")
PARIS_CITY_NAME = 'Paris'
NANTES_CITY_NAME = 'Nantes'
TOULOUSE_CITY_NAME = 'Toulouse'

def get_commune_code(name):
    """Retrieve the commune code for a given city name.

    Args:
        name (str): The name of the city.

    Returns:
        str: The commune code associated with the given city name.
    """
    with open(f"data/raw_data/{today_date}/communes_data.json") as fd:
        data = json.load(fd)
        communes_data_df = pd.json_normalize(data)
        return(communes_data_df[communes_data_df['nom']==name]['code'].item())

def create_consolidate_tables():
    """Creates the necessary database tables for consolidating station and city data.

    Reads SQL statements from a file and executes them to create or update database tables.
    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)


def consolidate_city_data():
    """Consolidates city-level data, including city codes and population information.

    Reads raw city data, standardizes it, and inserts or updates the CONSOLIDATE_CITY table.
    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/communes_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None

    city_data_df = raw_data_df[[
        "code",
        "nom",
        "population"
    ]]
    city_data_df.rename(columns={
        "code": "id",
        "nom": "name",
        "population": "nb_inhabitants"
    }, inplace=True)
    city_data_df.drop_duplicates(inplace = True)

    city_data_df["created_date"] = date.today()
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

def consolidate_station_data(city_name, columns_selected, rename_columns, station_id):
    """Consolidates station data for a specific city.

    Args:
        city_name (str): The name of the city (e.g., 'Paris', 'Nantes').
        columns_selected (list): The list of columns to select from the raw data.
        rename_columns (dict): A mapping of raw column names to standardized column names.
        id (str): The column name in the raw data used to generate unique station IDs.

    Returns:
        pd.DataFrame: A DataFrame containing the consolidated station data with standardized columns.
    """
    data = {}

    with open(f"data/raw_data/{today_date}/{city_name.lower()}_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    city_code = get_commune_code(city_name)
    raw_data_df = pd.json_normalize(data)
    raw_data_df["id"] = raw_data_df[station_id].apply(lambda x: f"{city_code}-{x}")
    raw_data_df["created_date"] = date.today()

    # Creating empty columns for missing data
    missing_columns = [col for col in columns_selected if col not in raw_data_df.columns]

    for col in missing_columns:
        raw_data_df[col] = None  

    station_data_df = raw_data_df[columns_selected]

    station_data_df.rename(columns=rename_columns, inplace=True)

    return station_data_df

def consolidate_all_station_data():
    """Consolidates station data for all cities (Paris, Nantes and Toulouse).

    Reads raw data for each city, transforms it into a standardized format, and inserts it
    into the CONSOLIDATE_STATION table in the database.
    """
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    paris_city_code = get_commune_code(PARIS_CITY_NAME)
    nantes_city_code = get_commune_code(NANTES_CITY_NAME)
    toulouse_city_code = get_commune_code(TOULOUSE_CITY_NAME)

    # ----- PARIS
    paris_selected_columns = [
        "id",
        "stationcode",
        "name",
        "nom_arrondissement_communes",
        "code_insee_commune",
        "address",  # column added
        "coordonnees_geo.lon",
        "coordonnees_geo.lat",
        "is_installed",
        "created_date",
        "capacity"
    ]

    paris_rename_columns = {
        "stationcode": "code",
        "name": "name",
        "coordonnees_geo.lon": "longitude",
        "coordonnees_geo.lat": "latitude",
        "is_installed": "status",
        "nom_arrondissement_communes": "city_name",
        "code_insee_commune": "city_code"
    }
    paris_id = "stationcode"
    paris_station_data_df = consolidate_station_data('Paris', paris_selected_columns, paris_rename_columns, paris_id)
    paris_station_data_df['city_name'] = PARIS_CITY_NAME
    paris_station_data_df['city_code'] = paris_city_code
    paris_station_data_df['status'] = paris_station_data_df['status'].map({'OUI': 'OPEN', 'NON': 'CLOSED'})


    # ----- NANTES
    nantes_selected_columns = [
        "id", 
        "number", 
        "name", 
        "city_name",
        "city_code",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands",
    ]
    nantes_rename_columns = {
        "number": "code",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "bike_stands": "capacity"
    }

    nantes_id = "number"
    nantes_station_data_df = consolidate_station_data(NANTES_CITY_NAME, nantes_selected_columns, nantes_rename_columns, nantes_id)
    nantes_station_data_df['city_name'] = NANTES_CITY_NAME
    nantes_station_data_df['city_code'] = nantes_city_code

    # ----- TOULOUSE 
    toulouse_selected_columns = [
        "id", 
        "number", 
        "name", 
        "city_name",
        "city_code",
        "address",
        "position.lon",
        "position.lat",
        "status",
        "created_date",
        "bike_stands"
    ]

    toulouse_rename_columns = {
        "number": "code",
        "position.lon": "longitude",
        "position.lat": "latitude",
        "bike_stands": "capacity"
    }

    toulouse_id = "number"
    toulouse_station_data_df = consolidate_station_data(TOULOUSE_CITY_NAME, toulouse_selected_columns, toulouse_rename_columns, toulouse_id)
    toulouse_station_data_df['city_name'] = TOULOUSE_CITY_NAME
    toulouse_station_data_df['city_code'] = toulouse_city_code


    # ----- INSERTION
    con.execute(f"INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM paris_station_data_df;")
    con.execute(f"INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM nantes_station_data_df;")
    con.execute(f"INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM toulouse_station_data_df;")




def consolidate_station_statement_data(city_name, city_code, selected_columns, rename_columns, station_id):
    """Consolidates real-time station statement data for a specific city.

    Args:
        city_name (str): The name of the city (e.g., 'Paris', 'Nantes').
        city_code (str): The code of the city retrieved from the city dataset.
        selected_columns (list): The list of columns to select from the raw data.
        rename_columns (dict): A mapping of raw column names to standardized column names.
        station_id (str): The column in the raw data used to identify stations.

    Returns:
        pd.DataFrame: A DataFrame containing consolidated real-time station statement data.
    """

    data = {}

    with open(f"data/raw_data/{today_date}/{city_name.lower()}_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["station_id"] = raw_data_df[station_id].apply(lambda x: f"{city_code}-{x}")
    raw_data_df["created_date"] = date.today()
    station_statement_data_df = raw_data_df[selected_columns]
    
    station_statement_data_df.rename(columns=rename_columns, inplace=True)

    return station_statement_data_df


def consolidate_all_station_statement_data():
    """Consolidates real-time station statement data for all cities (Paris, Nantes, Toulouse).

    Reads raw real-time data for each city, transforms it into a standardized format, and inserts it
    into the CONSOLIDATE_STATION_STATEMENT table in the database.
    """

    paris_city_code = get_commune_code(PARIS_CITY_NAME)
    nantes_city_code = get_commune_code(NANTES_CITY_NAME)
    toulouse_city_code = get_commune_code(TOULOUSE_CITY_NAME)

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    # ----- PARIS
    paris_selected_columns = [
        "station_id",
        "numdocksavailable",
        "numbikesavailable",
        "duedate",
        "created_date"  # column added
    ]
    paris_rename_columns = {
        "numdocksavailable": "bicycle_docks_available",
        "numbikesavailable": "bicycle_available",
        "duedate": "last_statement_date",
    }

    paris_station_statement_data_df = consolidate_station_statement_data(PARIS_CITY_NAME, paris_city_code, paris_selected_columns, paris_rename_columns, "stationcode")


    # ----- NANTES
    nantes_selected_columns = [
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"  # column added
    ]

    nantes_rename_columns = {
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }

    nantes_station_statement_data_df = consolidate_station_statement_data(NANTES_CITY_NAME, nantes_city_code, nantes_selected_columns, nantes_rename_columns, "number")
    
    # ----- TOULOUSE
    toulouse_selected_columns = [
        "station_id",
        "available_bike_stands",
        "available_bikes",
        "last_update",
        "created_date"  # column added
    ]

    toulouse_rename_columns = {
        "available_bike_stands": "bicycle_docks_available",
        "available_bikes": "bicycle_available",
        "last_update": "last_statement_date",
    }

    toulouse_station_statement_data_df = consolidate_station_statement_data(TOULOUSE_CITY_NAME, toulouse_city_code, toulouse_selected_columns, toulouse_rename_columns, "number")

    # ----- INSERTION
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM paris_station_statement_data_df;")
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM nantes_station_statement_data_df;")
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM toulouse_station_statement_data_df;")
