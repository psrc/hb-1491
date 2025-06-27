import pyogrio
import pymysql.cursors
import pymysql
import numpy as np
from pathlib import Path
import os
import pandas as pd
import geopandas as gpd
from pathlib import Path


def buffer_stops(stops, crs):
    dfs = []
    buffer_sizes = list(stops["buffer_size"].unique())
    for buffer_size in buffer_sizes:
        df = stops[stops["buffer_size"] == buffer_size]
        df["geometry"] = df.geometry.buffer(buffer_size)
        dfs.append(df)
    merged = pd.concat(dfs)
    return gpd.GeoDataFrame(merged, geometry="geometry", crs=crs)


def run(config):
    # h5_cache_dir = r'N:\base_year_2023_inputs\urbansim2_inputs\psrc_base_year_2023_alloc_py3.h5'
    # gis_data = Path(r'T:\60day-TEMP\Stefan\HB_transit.gdb')
    hb_1110_lookup = pd.read_csv(Path(config["data_dir"]) / "hb_1110_lookup.csv")

    plan_type_FAR = pd.read_csv(Path(config["data_dir"]) / "plan_type_FAR_lookup.csv")
    if config["use_walksheds"]:
        stops_gdf = gpd.read_file(config["output_gdb"], layer="stations_1491_walksheds")
    else:
        stops_gdf = gpd.read_file(config["output_gdb"], layer="stations_1491")

    # stops_gdf = stops_gdf.groupby('stop_name').first().reset_index()
    stops_gdf = stops_gdf[
        ["stop_name", "new_id", "geometry", "stop_type", "buffer_size"]
    ]
    stops_gdf = stops_gdf[stops_gdf["stop_name"] != "Untitled Stop"]
    if not config["use_walksheds"]:
        stops_gdf = buffer_stops(stops_gdf, stops_gdf.crs)
    # stops_gdf = stops_gdf.dissolve(by='stop_name', as_index=False)

    # stops_gdf['geometry'] = stops_gdf.geometry.buffer(2640)

    store = pd.HDFStore(config["h5_cache_dir"], mode="r")
    parcels = store["parcels"]  # extract the households dataset for the 2023 base year
    parcels.reset_index(inplace=True)  # reset index to avoid issues with merging
    # plan types
    plan_types = store[
        "development_constraints"
    ]  # extract the buildings dataset for 2050

    # get generic land use types
    res_plan_types = plan_types[
        plan_types["generic_land_use_type_id"].isin([1, 2, 6])
    ]  # filter for residential plan types
    res_plan_types = (
        res_plan_types.groupby("plan_type_id")["generic_land_use_type_id"]
        .max()
        .reset_index()
    )
    parcels = parcels.merge(
        res_plan_types[["plan_type_id", "generic_land_use_type_id"]],
        on="plan_type_id",
        how="left",
    )

    # only need residential parcels
    parcels = parcels[parcels["generic_land_use_type_id"].isin([2, 1, 6])]

    # create dummy for parcesl greater than 10k sqft
    parcels["10k_sqft_plus"] = (parcels["parcel_sqft"] > 10000).astype(int)

    # merge hb_1110_lookup to parcels for
    parcels = parcels.merge(
        hb_1110_lookup, on=["hb_tier", "hb_hct_buffer", "10k_sqft_plus"], how="left"
    )

    # get max FAR by plan type
    parcels = parcels.merge(plan_type_FAR, on="plan_type_id", how="left")
    # far_plan_types = plan_types[plan_types.constraint_type == 'far'] # filter for FAR constraints
    # far_plan_types = far_plan_types.groupby('plan_type_id').max().reset_index()
    # parcels = parcels.merge(far_plan_types[['plan_type_id', 'maximum']], on='plan_type_id', how='left')
    # parcels = parcels.rename(columns={'maximum': 'far_maximum'})
    # # set FAR to 0 for non-mixed use parcels
    # parcels['far_maximum'] = np.where(parcels['generic_land_use_type_id'] == 6, parcels['far_maximum'], 0)

    # # get max DU per acre by plan type
    # du_plan_types = plan_types[plan_types.constraint_type == 'units_per_acre'] # filter for DU constraints
    # du_plan_types = du_plan_types.groupby('plan_type_id').max().reset_index()
    # parcels = parcels.merge(du_plan_types[['plan_type_id', 'maximum']], on='plan_type_id', how='left')
    # parcels = parcels.rename(columns={'maximum': 'du_maximum'})
    # parcels = parcels.merge(du_to_far, left_on='du_maximum', right_on='zoned_max_du_acre', how='left')
    # #parcels['du_maximum_far'] = parcels['du_maximum'] * 0.0157 + 0.2283

    parcels = parcels.fillna(0)
    parcels["max_res_far_current_zoning"] = parcels.max_res_far_current_zoning.astype(
        float
    )

    # plan_types = plan_types.groupby('plan_type_id').agg({'maximum': 'max'}).reset_index()

    parcels["final_far"] = parcels[["max_res_far_current_zoning", "hb_res_far"]].max(
        axis=1
    )
    parcels["final_land_use"] = np.where(parcels["final_far"] <= 0.30, 1, 0)
    parcels["final_land_use"] = np.where(
        parcels["final_far"] == parcels["max_far_mixed_use"],
        6,
        parcels["final_land_use"],
    )
    parcels["final_land_use"] = np.where(
        parcels["final_land_use"] == 0, 2, parcels["final_land_use"]
    )  # 2 is for mixed use
    parcels["final_far_weight"] = parcels["final_far"] * parcels["parcel_sqft"]

    parcels_gdf = gpd.GeoDataFrame(
        parcels, geometry=gpd.points_from_xy(parcels.x_coord_sp, parcels.y_coord_sp)
    )

    data = []
    df_list = []
    # station_area_parcels = pd.GeoDataFrame(columns=[parcels_gdf.columns + ['stop_name']])
    for buffer in stops_gdf.iterrows():
        buffer = buffer[1]  # Get the row data
        parcels_in_buffer = parcels_gdf[parcels_gdf.geometry.within(buffer.geometry)]
        percent_weights_by_land_use = (
            parcels_in_buffer.groupby("final_land_use")["final_far_weight"].sum()
            / parcels_in_buffer.final_far_weight.sum()
        )
        percent_weights_by_land_use = percent_weights_by_land_use.to_dict()
        if 1 in percent_weights_by_land_use.keys():
            percent_weights_by_sf = percent_weights_by_land_use[1]
        else:
            percent_weights_by_sf = 0

        if 2 in percent_weights_by_land_use.keys():
            percent_weights_by_mf = percent_weights_by_land_use[2]
        else:
            percent_weights_by_mf = 0

        if 6 in percent_weights_by_land_use.keys():
            percent_weights_by_mixed_use = percent_weights_by_land_use[6]
        else:
            percent_weights_by_mixed_use = 0
        data.append(
            {
                "stop_name": buffer.stop_name,
                "new_id": buffer.new_id,
                "weighted_far": parcels_in_buffer["final_far_weight"].sum()
                / parcels_in_buffer["parcel_sqft"].sum(),
                "percent_weights_by_sf": percent_weights_by_sf,
                "percent_weights_by_mf": percent_weights_by_mf,
                "percent_weights_by_mixed_use": percent_weights_by_mixed_use,
            }
        )
        parcels_in_buffer["stop_name"] = buffer.stop_name
        parcels_in_buffer["new_id"] = buffer.new_id

        df_list.append(pd.DataFrame(parcels_in_buffer))

        print("done with buffer")
    results = pd.DataFrame(data)
    stops_gdf = stops_gdf.merge(results, on="new_id", how="left")
    stops_gdf.crs = 2285
    # gpd.options.io_engine = "fiona"

    station_parcels = pd.concat(df_list)
    station_parcels = gpd.GeoDataFrame(
        station_parcels, geometry="geometry", crs=parcels_gdf.crs
    )
    if config["use_walksheds"]:
        station_parcels.to_file(
            config["output_gdb"],
            driver="OpenFileGDB",
            layer="stations_parcels_walksheds",
        )
        stops_gdf.to_file(
            config["output_gdb"],
            driver="OpenFileGDB",
            layer="stations_weighted_far_walksheds",
        )
    else:
        stops_gdf.to_file(
            config["output_gdb"], driver="OpenFileGDB", layer="stations_weighted_far"
        )
        station_parcels.to_file(
            config["output_gdb"], driver="OpenFileGDB", layer="stations_parcels"
        )

    print("done with all buffers")
