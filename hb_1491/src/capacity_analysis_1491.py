import numpy as np
from pathlib import Path
import pandas as pd
import geopandas as gpd


def buffer_stops(stops, crs):
    """Buffers the stops GeoDataFrame by the 'buffer_size' column."""

    dfs = []
    buffer_sizes = list(stops["buffer_size"].unique())
    for buffer_size in buffer_sizes:
        df = stops[stops["buffer_size"] == buffer_size]
        df["geometry"] = df.geometry.buffer(buffer_size)
        dfs.append(df)
    merged = pd.concat(dfs)
    return gpd.GeoDataFrame(merged, geometry="geometry", crs=crs)


def group_by_sum_indices(arr, x):
    """Groups indices of an array such that the sum of the values in each group is at least x."""
    orig_indices = []
    group_indices = []
    group_sum = 0
    group_id = 0
    for idx, num in enumerate(arr):
        orig_indices.append(idx)
        group_indices.append(group_id)
        group_sum += num
        if group_sum >= x:
            group_id += 1
            group_sum = 0
    return orig_indices, group_indices


def find_closest_matches(list1, list2):
    """
    Matches each value in list1 to the closest value in list2.

    Args:
        list1 (list): The list of values to match.
        list2 (list): The list of values to find matches within.

    Returns:
        list: A list containing the closest matches.
    """
    matches = []
    for val1 in list1:
        closest_val2 = min(list2, key=lambda x: abs(x - val1))
        matches.append(closest_val2)
    return matches


def average_built_sqft_per_unit(
    buildings, parcels, max_du_acre=1400, min_group_size=50
):
    """Calculates the average built square footage per unit for multifamily
    buildings for groupings of dwelling units per acre."""

    # get multifamily buildings & apply filters
    buildings = buildings[buildings["building_type_id"].isin([4, 10, 12])]
    buildings = buildings[buildings["residential_units"] > 0]
    buildings = buildings[buildings["sqft_per_unit"] > 0]
    buildings["res_sqft"] = buildings["residential_units"] * buildings["sqft_per_unit"]
    buildings["buildings_count"] = 1

    # aggregate buildings by parcel_id
    buildings = (
        buildings.groupby("parcel_id")
        .agg({"res_sqft": "sum", "residential_units": "sum", "buildings_count": "sum"})
        .reset_index()
    )
    # merge buildings with parcels
    parcels = parcels.merge(buildings, on="parcel_id", how="inner")

    # further filtering
    parcels = parcels[parcels["residential_units"] > 1]
    parcels = parcels[parcels["res_sqft"] > 0]
    parcels["sqft_per_unit"] = parcels["res_sqft"] / parcels["residential_units"]
    parcels = parcels[parcels["sqft_per_unit"] < 5000]
    parcels = parcels[parcels["sqft_per_unit"] > 100]
    parcels = parcels[parcels["parcel_sqft"] > 0]

    parcels["as_built_du_acre"] = parcels.residential_units / (
        parcels.parcel_sqft / 43560
    )

    # round the as_built_du_acre to the nearest 10 to create groupings
    parcels["as_built_du_acre_rounded"] = (parcels.as_built_du_acre / 10).round() * 10
    freq = parcels["as_built_du_acre_rounded"].value_counts().reset_index()

    # find groups that have less than min_group_size
    small_sample = freq[freq["count"] < min_group_size].reset_index()

    # create groups that have at least min_group_size
    index, groupings = group_by_sum_indices(small_sample["count"].values, 50)
    small_sample["groupings"] = groupings
    small_sample["du_acre_final"] = small_sample.groupby("groupings")[
        "as_built_du_acre_rounded"
    ].transform("max")

    # merge the small sample with parcels, include 'du_per_acre_final' column,
    # which is used to join plan types outside this function
    parcels = parcels.merge(
        small_sample[["as_built_du_acre_rounded", "du_acre_final"]],
        on="as_built_du_acre_rounded",
        how="left",
    )
    parcels["du_acre_final"].fillna(parcels["as_built_du_acre_rounded"], inplace=True)
    parcels.loc[parcels["du_acre_final"] > max_du_acre, "du_acre_final"] = max_du_acre

    built_sqft_per_unit = (
        parcels.groupby("du_acre_final")
        .agg({"res_sqft": "sum", "residential_units": "sum", "buildings_count": "sum"})
        .reset_index()
    )
    built_sqft_per_unit["sqft_feet_per_unit"] = (
        built_sqft_per_unit["res_sqft"] / built_sqft_per_unit["residential_units"]
    )

    return built_sqft_per_unit


def create_plantype_FAR_lookup(plan_types):
    """Creates a (flattened) lookup table for each plan types with min and max DU (SF & MF) & FAR (MU)."""

    # rename constrant_type 'units_per_acre' to 'du' for consistency
    plan_types.loc[
        plan_types["constraint_type"] == "units_per_acre", "constraint_type"
    ] = "du"

    plan_types.rename(
        columns={
            "minimum": "min",
            "maximum": "max",
        },
        inplace=True,
    )

    plan_types["plan_label"] = (
        plan_types.generic_land_use_type_id.astype(str)
        + "_"
        + plan_types.constraint_type.astype(str)
    )

    df = (
        plan_types.groupby(["plan_type_id", "plan_label"])
        .agg({"min": "max", "max": "max"})
        .reset_index()
    )

    df["idx"] = df.groupby("plan_type_id").cumcount() + 1
    df = df.pivot_table(
        index=["plan_type_id"],
        columns="plan_label",
        values=["min", "max"],
        aggfunc="first",
    )

    df = df.sort_index(axis=1, level=1)
    df.columns = [f"{x}_{y}" for x, y in df.columns]
    df = df.reset_index()
    df.fillna(0, inplace=True)
    df["max_du"] = df[["max_1_du", "max_2_du", "max_6_du"]].max(axis=1)
    df["max_far_mixed_use"] = df["max_6_far"]
    return df

def percent_parcels_meet_far(parcels, far_threshold):
    """Calculates the percentage of parcels that meet the FAR threshold."""
    parcels = parcels[parcels["final_far"] > 0]
    parcels["meets_far_threshold"] = parcels["final_far"] >= far_threshold
    percent_meeting_far = (
        parcels["meets_far_threshold"].sum() / len(parcels) * 100
    )
    return percent_meeting_far

def run(config):
    # has FAR assumptions for single family & middle houseing
    hb_1110_lookup = pd.read_csv(Path(config["data_dir"]) / "hb_1110_lookup.csv")
    store = pd.HDFStore(config["h5_cache_dir"], mode="r")

    # parcels
    parcels = store["parcels"]
    parcels.reset_index(inplace=True)

    # builduings
    buildings = store["buildings"]
    buildings.reset_index(inplace=True)

    # plan types
    plan_types = store["development_constraints"]

    # walksheds have been created for the 1491 stops
    if config["use_walksheds"]:
        stops_gdf = gpd.read_file(config["output_gdb"], layer="stations_1491_walksheds")
    else:
        stops_gdf = gpd.read_file(config["output_gdb"], layer="stations_1491")

    stops_gdf = stops_gdf[
        ["stop_name", "new_id", "geometry", "stop_type", "buffer_size"]
    ]

    # stops_gdf = stops_gdf[stops_gdf["stop_name"] != "Untitled Stop"]

    # buffer stops if not using walksheds
    if not config["use_walksheds"]:
        stops_gdf = buffer_stops(stops_gdf, stops_gdf.crs)

    # get the built sqft per unit for ranges of dwelling units per acre
    built_sqft_by_units_acre = average_built_sqft_per_unit(buildings, parcels)
    
    # create a lookup table for plan types with max DU and FAR
    plan_type_FAR = create_plantype_FAR_lookup(plan_types)
    
    # create a crosswalk between plan types and built sqft per unit
    plan_type_FAR["du_acre_lookup"] = find_closest_matches(
        plan_type_FAR["max_du"].tolist(),
        built_sqft_by_units_acre["du_acre_final"].tolist(),
    )
    # merge the built sqft per unit with plan types
    plan_type_FAR = plan_type_FAR.merge(
        built_sqft_by_units_acre[["du_acre_final", "sqft_feet_per_unit"]],
        left_on="du_acre_lookup",
        right_on="du_acre_final",
        how="left",
    )
    # calculate max FAR for residential parcels
    plan_type_FAR["max_far_res"] = (
        plan_type_FAR["max_du"] * plan_type_FAR["sqft_feet_per_unit"] / 43560
    )
    # calculate max FAR for plan type
    plan_type_FAR["max_res_far_current_zoning"] = plan_type_FAR[
        ["max_far_res", "max_far_mixed_use"]
    ].max(axis=1)

    # get generic land use types
    res_plan_types = plan_types[
        plan_types["generic_land_use_type_id"].isin([1, 2, 6])
    ]  # filter for residential plan types
    res_plan_types = (
        res_plan_types.groupby("plan_type_id")["generic_land_use_type_id"]
        .max()
        .reset_index()
    )
    # parcels = parcels.merge(
    #     res_plan_types[["plan_type_id", "generic_land_use_type_id"]],
    #     on="plan_type_id",
    #     how="left",
    # )

    # # only need residential parcels
    # parcels = parcels[parcels["generic_land_use_type_id"].isin([2, 1, 6])]

    # create dummy for parcels greater than 10k sqft
    parcels["10k_sqft_plus"] = (parcels["parcel_sqft"] > 10000).astype(int)

    # merge hb_1110_lookup to parcels for
    parcels = parcels.merge(
        hb_1110_lookup, on=["hb_tier", "hb_hct_buffer", "10k_sqft_plus"], how="left"
    )

    # join plan types with parcels
    parcels = parcels.merge(plan_type_FAR, on="plan_type_id", how="left")

    # keep only parcels that have far greater than 0
    parcels = parcels[parcels['max_res_far_current_zoning']>0]
    
    # only apply hb_res_far to parcels that are not mixed use or mf:
    parcels["hb_res_far"] = np.where(
        parcels["max_du"] > 12, 0, parcels["hb_res_far"]
    )

    parcels = parcels.fillna(0)
    parcels["max_res_far_current_zoning"] = parcels.max_res_far_current_zoning.astype(
        float
    )

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
    # iterate through each buffer and calculate the weighted FAR
    for buffer in stops_gdf.iterrows():
        buffer = buffer[1]
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
        
        if buffer.stop_type == 5:
            percent_meets = percent_parcels_meet_far(parcels_in_buffer, 2.5)
        else:
            percent_meets = percent_parcels_meet_far(parcels_in_buffer, 3.5)
        data.append(
            {
                "stop_name": buffer.stop_name,
                "new_id": buffer.new_id,
                "weighted_far": parcels_in_buffer["final_far_weight"].sum()
                / parcels_in_buffer["parcel_sqft"].sum(),
                "percent_weights_by_sf": percent_weights_by_sf,
                "percent_weights_by_mf": percent_weights_by_mf,
                "percent_weights_by_mixed_use": percent_weights_by_mixed_use,
                "percent_meets_far": percent_meets,
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
