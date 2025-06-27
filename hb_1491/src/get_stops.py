import transit_service_analyst as tsa
from pathlib import Path
import numpy as np
import pandas as pd
import geopandas as gpd
import psrcelmerpy
import requests
from dataclasses import dataclass

# gtfs_dir = Path(
#     "C:/Users/scoe/Puget Sound Regional Council/GIS - Sharing/Projects/Transportation/RTP_2026/transit/GTFS/combined"
# )


# class transit_data_frames(NamedTuple):
#     """a docstring"""
#     year: str
#     routes: gpd.GeoDataFrame
#     route_stops: gpd.GeoDataFrame
#     stops: gpd.GeoDataFrame
@dataclass
class transit_data_frames:
    """A class to hold transit data frames."""

    year: str
    routes: gpd.GeoDataFrame
    route_stops: gpd.GeoDataFrame
    stops: gpd.GeoDataFrame


def get_current_service(transit_dfs: transit_data_frames) -> gpd.GeoDataFrame:
    """
    Get the current 1491 service from the transit data frames.
    """
    # BRT
    routes = transit_dfs.routes[transit_dfs.routes["route_type"] == 5]
    routes.crs = 4326
    routes = routes.to_crs(2285)

    route_stops = transit_dfs.route_stops[
        transit_dfs.route_stops["rep_trip_id"].isin(routes["rep_trip_id"].to_list())
    ]
    route_stops.crs = 4326
    route_stops = route_stops.to_crs(2285)

    stops = transit_dfs.stops[
        transit_dfs.stops["stop_id"].isin(route_stops["stop_id"].to_list())
    ]
    stops = add_columns_to_stops(stops, 5, transit_dfs.year)
    stops.crs = 4326
    stops = stops.to_crs(2285)

    return transit_data_frames(
        year=transit_dfs.year, routes=routes, route_stops=route_stops, stops=stops
    )


def get_future_service(transit_dfs: transit_data_frames) -> gpd.GeoDataFrame:
    """
    Get the future 1491 service from the transit data frames.
    """
    # BRT
    route_list = []
    route_stop_list = []
    stop_list = []

    # street car, light rail, commuter rail
    for route_type in [0, 1, 2, 5]:
        # get the routes for the route type
        if route_type == 5:
            routes = transit_dfs.routes[
                (transit_dfs.routes["route_type"] == route_type)
                & (transit_dfs.routes["agency_id"] == "6")
            ]
        else:
            routes = transit_dfs.routes[transit_dfs.routes["route_type"] == route_type]
        if not routes.empty:
            route_stops = transit_dfs.route_stops[
                transit_dfs.route_stops["rep_trip_id"].isin(
                    routes["rep_trip_id"].to_list()
                )
            ]
            stops = transit_dfs.stops[
                transit_dfs.stops["stop_id"].isin(route_stops["stop_id"].to_list())
            ]
            stops = add_columns_to_stops(stops, route_type, transit_dfs.year)
            route_list.append(routes)
            route_stop_list.append(route_stops)
            stop_list.append(stops)

    routes = gpd.GeoDataFrame(pd.concat(route_list))
    routes.crs = 4326
    routes = routes.to_crs(2285)

    route_stops = gpd.GeoDataFrame(pd.concat(route_stop_list))
    route_stops.crs = 4326
    route_stops = route_stops.to_crs(2285)

    stops = pd.concat(stop_list, ignore_index=True)
    stops.crs = 4326
    stops = stops.to_crs(2285)

    return transit_data_frames(
        year=transit_dfs.year, routes=routes, route_stops=route_stops, stops=stops
    )


def add_columns_to_stops(
    stops: gpd.GeoDataFrame, stop_type: int, year: int
) -> pd.DataFrame:
    """
    Add a column to the stops GeoDataFrame with a constant value.
    """
    stops["stop_type"] = stop_type
    # stops['buffer_size'] = buffer_size
    stops["year"] = year
    return stops


def get_transit_gdfs(gtfs_dir: Path, date: str, current_data: bool) -> tuple:
    """
    Get the transit GeoDataFrames for a given service definition.
    """
    tsa_instance = tsa.load_gtfs(gtfs_dir, date)
    # get the routes
    dfs = transit_data_frames(
        year=date[:4],
        routes=tsa_instance.get_lines_gdf(),
        route_stops=tsa_instance.get_line_stops_gdf(),
        stops=gpd.GeoDataFrame(
            tsa_instance.stops,
            geometry=gpd.points_from_xy(
                tsa_instance.stops["stop_lon"], tsa_instance.stops["stop_lat"]
            ),
        ),
    )
    if current_data:
        dfs = get_current_service(dfs)
    else:
        # get the routes
        dfs = get_future_service(dfs)
    return dfs


def merge_dfs(
    current_dfs: transit_data_frames, future_dfs: transit_data_frames, crs: int
) -> transit_data_frames:
    """
    Merge the current and future transit data frames.
    """
    routes = pd.concat([current_dfs.routes, future_dfs.routes], ignore_index=True)
    route_stops = pd.concat(
        [current_dfs.route_stops, future_dfs.route_stops], ignore_index=True
    )
    stops = pd.concat([current_dfs.stops, future_dfs.stops], ignore_index=True)
    stops.insert(0, "new_id", range(1, 1 + len(stops)))

    return transit_data_frames(
        year=current_dfs.year, routes=routes, route_stops=route_stops, stops=stops
    )


def get_city_pop_by_stop(stops, population_year) -> gpd.GeoDataFrame:
    # def get_city_pop():
    """
    Add a city_id column to the stops GeoDataFrame.
    """
    # This function is a placeholder for the actual implementation
    # that would assign city IDs based on some criteria.
    eg_conn = psrcelmerpy.ElmerGeoConn()
    cities = eg_conn.read_geolayer("cities")
    cities = cities.to_crs(2285)
    uga = eg_conn.read_geolayer("urban_growth_area")
    uga = uga.to_crs(2285)
    # filter the cities to only those in the UGA
    stops = stops.loc[stops.within(uga.geometry.unary_union)]

    # change the city names to match the OFM jurisdiction names
    cities["city_name"] = np.where(
        cities["city_name"] == "Sea Tac", "SeaTac", cities["city_name"]
    )
    cities["city_name"] = np.where(
        cities["city_name"] == "Beaux Arts", "Beaux Arts Village", cities["city_name"]
    )

    # get the city population from the OFM April 1 estimate facts table
    e_conn = psrcelmerpy.ElmerConn()
    cities_pop = e_conn.get_table(schema="ofm", table_name="april_1_estimate_facts")
    cities_pop = cities_pop[cities_pop["estimate_year"] == population_year]
    cities_pop = (
        cities_pop.groupby(["jurisdiction_dim_id"])["total_population"]
        .sum()
        .reset_index()
    )
    jurisdictions = e_conn.get_table(schema="ofm", table_name="jurisdiction_dim")

    # merge the city population with the jurisdictions
    cities_pop = cities_pop.merge(jurisdictions, on="jurisdiction_dim_id", how="left")
    # remove part of jurisdiction names if it contains "(part)"
    cities_pop["jurisdiction_name"] = (
        cities_pop["jurisdiction_name"]
        .str.replace(r"\s*\(part\)\s*", " ", regex=True, case=False)
        .str.replace(r"\s{2,}", " ", regex=True)
        .str.strip()
    )

    cities = cities.merge(
        cities_pop, left_on="city_name", right_on="jurisdiction_name", how="left"
    )
    assert not cities.jurisdiction_name.hasnans

    stops = gpd.sjoin(stops, cities, how="left", predicate="intersects")
    stops = stops[
        [
            "stop_id",
            "new_id",
            "stop_name",
            "stop_type",
            "year",
            "geometry",
            "city_name",
            "jurisdiction_dim_id",
            "total_population",
        ]
    ]

    # for index, row in stops.iterrows():
    #     city_name = row['city_name']
    #     stops.at[index, 'city_place_code'] = get_place_fips_by_name_and_state_fips(city_name, 53)
    #     stops.at[index, 'city_population'] = get_city_population(stops.at[index, 'city_place_code'], 53)
    # print ('done')
    return stops


def get_place_fips_by_name_and_state_fips(city, state_fips):
    """
    Returns the place FIPS code for a given city name and state FIPS code using the Census API.
    """
    url = f"https://api.census.gov/data/2020/dec/pl?get=NAME,PLACE&for=place:*&in=state:{state_fips}"
    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception(f"API request failed: {resp.status_code} {resp.text}")
    data = resp.json()
    for row in data[1:]:
        name, place_fips, _, _ = row  # NAME, PLACE, place, state
        if city.lower() in name.lower():
            return place_fips
    raise ValueError(f"City '{city}' not found in state FIPS '{state_fips}'.")


def get_city_population(city_place_code, state_fips):
    """
    Returns the place FIPS code for a given city name and state FIPS code using the Census API.
    """
    # url = f"https://api.census.gov/data/2020/dec/pl?get=NAME,PLACE&for=place:*&in=state:{state_fips}"
    url = f"https://api.census.gov/data/2020/dec/pl?get=NAME,P1_001N&for=place:{city_place_code}&in=state:{state_fips}"
    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception(f"API request failed: {resp.status_code} {resp.text}")
    data = resp.json()
    return int(data[1][1])


def get_buffer_size_by_stop_type(stops) -> gpd:
    """
    Returns the buffer size for a given stop type.
    """
    stops["buffer_size"] = 0
    stops["buffer_size"] = np.where(
        stops["stop_type"].isin([0, 1, 2]), 2640, stops["buffer_size"]
    )  # Light Rail, Street Car, Commuter Rail
    stops.loc[
        (stops["stop_type"] == 2) & (stops["total_population"] < 15000), "buffer_size"
    ] = 1320
    stops["buffer_size"] = np.where(
        stops["stop_type"] == 5, 1320, stops["buffer_size"]
    )  # bus
    assert not stops.buffer_size.hasnans, "Buffer size has NaN values"

    return stops


def run(config):
    """
    Main function to run the script.
    """
    # Get the GTFS data for 2024 and 2050
    dfs_2024 = get_transit_gdfs(
        Path(config["gtfs_dir"]) / "2024", "20250114", current_data=True
    )
    dfs_2050 = get_transit_gdfs(
        Path(config["gtfs_dir"]) / "2050", "20250408", current_data=False
    )

    dfs_merged = merge_dfs(dfs_2024, dfs_2050, crs=config["crs"])

    dfs_merged.stops = get_city_pop_by_stop(dfs_merged.stops, config["population_year"])
    dfs_merged.stops = get_buffer_size_by_stop_type(dfs_merged.stops)
    dfs_merged.stops.to_file(
        Path(config["output_gdb"]), driver="OpenFileGDB", layer="stations_1491"
    )
    dfs_merged.route_stops.to_file(
        config["output_gdb"], driver="OpenFileGDB", layer="route_stops_1491"
    )
    dfs_merged.routes.to_file(
        config["output_gdb"], driver="OpenFileGDB", layer="route_1491"
    )
    return dfs_merged.stops

    print("done")
