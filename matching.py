"""
Matching functions for spatial and temporal matching between MSD and LMD data.
"""

import polars as pl
import numpy as np
from sklearn.neighbors import BallTree
import logging
from config import EARTH_RADIUS_M


def perform_spatial_matching(msd_df, lmd_df, max_spatial_dist_m=100):
    """
    Perform spatial matching between MSD and LMD.
    """
    # Build BallTree on LMD
    logging.info("Building spatial index...")
    lmd_coords = np.radians(lmd_df.select(["Lat", "Lon"]).to_numpy())
    tree = BallTree(lmd_coords, metric="haversine")

    # Batch spatial query for all MSD points
    logging.info("Performing spatial queries...")
    msd_coords = np.radians(msd_df.select(["Lat", "Lon"]).to_numpy())
    dist_limit_rad = max_spatial_dist_m / EARTH_RADIUS_M

    # Query all at once - get distances and indices sorted by distance
    distances_list, indices_list = tree.query(msd_coords, k=len(lmd_df))  # Get all possible matches

    # Filter by distance limit and create candidate pairs
    logging.info("Creating candidate pairs...")
    pairs = []
    for msd_idx in range(len(msd_coords)):
        valid_indices = indices_list[msd_idx][distances_list[msd_idx] <= dist_limit_rad]
        valid_distances = distances_list[msd_idx][distances_list[msd_idx] <= dist_limit_rad]

        for lmd_idx, dist_rad in zip(valid_indices, valid_distances):
            pairs.append({
                "msd_idx": msd_idx,
                "lmd_idx": int(lmd_idx),
                "spatial_dist_rad": dist_rad
            })

    if not pairs:
        logging.warning("No spatial matches found")
        return pl.DataFrame()

    logging.info(f"Found {len(pairs)} candidate pairs from spatial query")

    # Create pairs dataframe
    pairs_df = pl.DataFrame(pairs)

    # Join with original data
    logging.info("Joining candidate pairs with original data...")

    # Join MSD data
    pairs_df = pairs_df.join(
        msd_df.select(["msd_idx", "Lane", "TestDateUTC_parsed", "Chain", "RoadName"]),
        on="msd_idx",
        how="left",
    ).rename(
        {
            "Lane": "Lane_msd",
            "TestDateUTC_parsed": "Time_msd",
            "Chain": "Chain_msd",
            "RoadName": "RoadName_msd",
        }
    )

    # Join LMD data
    pairs_df = pairs_df.join(
        lmd_df.select(["lmd_idx", "Lane", "TestDateUTC_parsed", "Chain", "RoadName"]),
        on="lmd_idx",
        how="left",
    ).rename(
        {
            "Lane": "Lane_lmd",
            "TestDateUTC_parsed": "Time_lmd",
            "Chain": "Chain_lmd",
            "RoadName": "RoadName_lmd",
        }
    )

    # Convert spatial distance to meters
    pairs_df = pairs_df.with_columns(
        (pl.col("spatial_dist_rad") * EARTH_RADIUS_M).alias("spatial_dist_m")
    ).drop("spatial_dist_rad")

    return pairs_df


def filter_and_select_matches(pairs_df, max_time_diff_sec=10, max_chainage_diff_m=5.0, enable_time=True, enable_road=True, enable_spatial=True):
    """
    Filter pairs and select best matches based on enabled criteria.
    """
    logging.info("Applying filters...")

    # Always calculate differences for output purposes, but only filter when enabled
    diff_columns = []

    # Check if Time columns exist, are datetime type, and have valid (non-null) data
    has_valid_time_data = ("Time_msd" in pairs_df.columns and "Time_lmd" in pairs_df.columns and
                          pairs_df.select("Time_msd").dtypes[0] == pl.Datetime and
                          pairs_df.select("Time_lmd").dtypes[0] == pl.Datetime and
                          pairs_df.select("Time_msd").null_count().item() < len(pairs_df) and
                          pairs_df.select("Time_lmd").null_count().item() < len(pairs_df))

    if has_valid_time_data:
        diff_columns.append(
            (pl.col("Time_msd") - pl.col("Time_lmd")).abs().alias("time_diff")
        )

    # Check if Chain columns exist
    if "Chain_msd" in pairs_df.columns and "Chain_lmd" in pairs_df.columns:
        diff_columns.append(
            (pl.col("Chain_msd") - pl.col("Chain_lmd")).abs().alias("chainage_diff")
        )

    if diff_columns:
        pairs_df = pairs_df.with_columns(diff_columns)

    # Apply filters
    filter_conditions = []

    # Lane matching: required unless road-based matching with LMD Lane = 'ALL'
    if enable_road and "Lane_lmd" in pairs_df.columns:
        # For road-based matching, allow LMD Lane = 'ALL' to match any MSD Lane
        lane_condition = (pl.col("Lane_lmd") == "ALL") | (pl.col("Lane_msd") == pl.col("Lane_lmd"))
        filter_conditions.append(lane_condition)
    else:
        # For spatial/time matching, require exact lane match
        filter_conditions.append(pl.col("Lane_msd") == pl.col("Lane_lmd"))

    if enable_time and has_valid_time_data:
        filter_conditions.append(pl.col("time_diff") <= pl.duration(seconds=max_time_diff_sec))

    # For road-based matching, only apply road name filter, not chainage_diff
    # (chainage matching was already done in perform_road_based_matching)
    if enable_road and not (enable_spatial or enable_time):
        # Pure road-based matching: only check road name
        filter_conditions.append(pl.col("RoadName_msd") == pl.col("RoadName_lmd"))
    elif enable_road:
        # Mixed matching (road + spatial/time): apply both chainage and road name filters
        filter_conditions.append(pl.col("chainage_diff") <= max_chainage_diff_m)
        filter_conditions.append(pl.col("RoadName_msd") == pl.col("RoadName_lmd"))

    # Apply all enabled filters
    filtered = pairs_df.filter(pl.all_horizontal(filter_conditions))

    logging.info(f"After filtering: {len(filtered)} valid pairs")

    if filtered.is_empty():
        logging.warning("No matches found after filtering")
        return pl.DataFrame()

    # Select best match per MSD row
    logging.info("Selecting best matches...")

    # Sort by time_diff if time matching is enabled and time_diff column exists,
    # otherwise by chainage_diff if road matching is enabled and chainage_diff column exists,
    # otherwise by spatial_dist_m if spatial matching is enabled, otherwise take first match
    if enable_time and "time_diff" in pairs_df.columns:
        sort_column = "time_diff"
    elif enable_road and "chainage_diff" in pairs_df.columns:
        sort_column = "chainage_diff"
    elif enable_spatial and "spatial_dist_m" in pairs_df.columns:
        sort_column = "spatial_dist_m"
    else:
        # If no sorting criteria enabled, just take first match
        sort_column = None

    if sort_column:
        best_matches = filtered.sort(sort_column).group_by("msd_idx").first()
    else:
        best_matches = filtered.group_by("msd_idx").first()

    logging.info(f"Final matches: {len(best_matches)}")

    return best_matches


def create_all_pairs(msd_df, lmd_df, enable_road=False):
    """
    Create all possible pairs between MSD and LMD data for non-spatial matching.
    If enable_road=True, only create pairs within the same road/lane groups to reduce memory usage.
    """
    if enable_road:
        logging.info("Creating road-based pairs for non-spatial matching...")
        return create_road_based_pairs(msd_df, lmd_df)
    else:
        logging.info("Creating all possible pairs for non-spatial matching...")
        return create_cartesian_pairs(msd_df, lmd_df)


def create_cartesian_pairs(msd_df, lmd_df):
    """
    Create cartesian product of all MSD and LMD pairs (use with caution for large datasets).
    """
    # Create cartesian product of indices
    msd_indices = list(range(len(msd_df)))
    lmd_indices = list(range(len(lmd_df)))

    pairs = []
    for msd_idx in msd_indices:
        for lmd_idx in lmd_indices:
            pairs.append({"msd_idx": msd_idx, "lmd_idx": lmd_idx})

    if not pairs:
        logging.warning("No pairs created")
        return pl.DataFrame()

    logging.info(f"Created {len(pairs)} candidate pairs")

    # Create pairs dataframe
    pairs_df = pl.DataFrame(pairs)

    # Join with original data
    logging.info("Joining pairs with original data...")

    # Join MSD data
    pairs_df = pairs_df.join(
        msd_df.select(["msd_idx", "Lane", "TestDateUTC_parsed", "Chain", "RoadName"]),
        on="msd_idx",
        how="left",
    ).rename(
        {
            "Lane": "Lane_msd",
            "TestDateUTC_parsed": "Time_msd",
            "Chain": "Chain_msd",
            "RoadName": "RoadName_msd",
        }
    )

    # Join LMD data
    pairs_df = pairs_df.join(
        lmd_df.select(["lmd_idx", "Lane", "TestDateUTC_parsed", "Chain", "RoadName"]),
        on="lmd_idx",
        how="left",
    ).rename(
        {
            "Lane": "Lane_lmd",
            "TestDateUTC_parsed": "Time_lmd",
            "Chain": "Chain_lmd",
            "RoadName": "RoadName_lmd",
        }
    )

    return pairs_df


def create_road_based_pairs(msd_df, lmd_df):
    """
    Create pairs only within the same road/lane groups to reduce memory usage.
    """
    logging.info("Grouping data by road attributes...")

    # Group MSD by road attributes
    msd_groups = {}
    for i, row in enumerate(msd_df.rows()):
        road_name = row[msd_df.columns.index("RoadName")]
        lane = row[msd_df.columns.index("Lane")]
        key = (road_name, lane)
        if key not in msd_groups:
            msd_groups[key] = []
        msd_groups[key].append(i)

    # Group LMD by road attributes
    lmd_groups = {}
    for i, row in enumerate(lmd_df.rows()):
        road_name = row[lmd_df.columns.index("RoadName")]
        lane = row[lmd_df.columns.index("Lane")]
        key = (road_name, lane)
        if key not in lmd_groups:
            lmd_groups[key] = []
        lmd_groups[key].append(i)

    # Create pairs only within matching groups
    pairs = []
    for road_lane_key in msd_groups:
        if road_lane_key in lmd_groups:
            msd_indices = msd_groups[road_lane_key]
            lmd_indices = lmd_groups[road_lane_key]

            for msd_idx in msd_indices:
                for lmd_idx in lmd_indices:
                    pairs.append({"msd_idx": msd_idx, "lmd_idx": lmd_idx})

    if not pairs:
        logging.warning("No road-based pairs created")
        return pl.DataFrame()

    logging.info(f"Created {len(pairs)} road-based candidate pairs")

    # Create pairs dataframe
    pairs_df = pl.DataFrame(pairs)

    # Join with original data
    logging.info("Joining road-based pairs with original data...")

    # Join MSD data
    pairs_df = pairs_df.join(
        msd_df.select(["msd_idx", "Lane", "TestDateUTC_parsed", "Chain", "RoadName"]),
        on="msd_idx",
        how="left",
    ).rename(
        {
            "Lane": "Lane_msd",
            "TestDateUTC_parsed": "Time_msd",
            "Chain": "Chain_msd",
            "RoadName": "RoadName_msd",
        }
    )

    # Join LMD data
    pairs_df = pairs_df.join(
        lmd_df.select(["lmd_idx", "Lane", "TestDateUTC_parsed", "Chain", "RoadName"]),
        on="lmd_idx",
        how="left",
    ).rename(
        {
            "Lane": "Lane_lmd",
            "TestDateUTC_parsed": "Time_lmd",
            "Chain": "Chain_lmd",
            "RoadName": "RoadName_lmd",
        }
    )

    return pairs_df


def perform_road_based_matching(msd_df, lmd_df):
    """
    Perform road-based matching using road_id, region_id, and chainage range.
    Matches when: road_id and region_id match, and MSD Chain is within LMD's [Start Chainage, End Chainage] range.
    """
    logging.info("Performing road-based matching...")

    # Check required columns
    required_msd_cols = ["road_id", "region_id", "Chain"]
    required_lmd_cols = ["road_id", "region_id", "Start Chainage (km)", "End Chainage (km)"]

    for col in required_msd_cols:
        if col not in msd_df.columns:
            logging.error(f"Required column '{col}' not found in MSD data")
            return pl.DataFrame()

    for col in required_lmd_cols:
        if col not in lmd_df.columns:
            logging.error(f"Required column '{col}' not found in LMD data")
            return pl.DataFrame()

    pairs = []

    # Convert LMD data to list of dicts for easier processing
    lmd_data = lmd_df.select(required_lmd_cols + ["lmd_idx"]).to_dicts()

    # Group MSD by road_id and region_id for faster lookup
    msd_groups = {}
    for row in msd_df.select(required_msd_cols + ["msd_idx"]).to_dicts():
        key = (row["road_id"], row["region_id"])
        if key not in msd_groups:
            msd_groups[key] = []
        msd_groups[key].append(row)

    # For each LMD row, find matching MSD rows
    for lmd_row in lmd_data:
        road_key = (lmd_row["road_id"], lmd_row["region_id"])

        if road_key not in msd_groups:
            continue  # No MSD data for this road/region

        # Convert chainage range to meters (multiply by 1000)
        try:
            start_chain_km = float(lmd_row["Start Chainage (km)"])
            end_chain_km = float(lmd_row["End Chainage (km)"])
            start_chain = start_chain_km * 1000
            end_chain = end_chain_km * 1000
        except (ValueError, TypeError) as e:
            logging.warning(f"Invalid chainage values for LMD row {lmd_row['lmd_idx']}: Start={lmd_row['Start Chainage (km)']}, End={lmd_row['End Chainage (km)']}. Skipping.")
            continue

        # Find MSD rows within the chainage range
        for msd_row in msd_groups[road_key]:
            if start_chain <= msd_row["Chain"] <= end_chain:
                pairs.append({
                    "msd_idx": msd_row["msd_idx"],
                    "lmd_idx": lmd_row["lmd_idx"]
                })

    if not pairs:
        logging.warning("No road-based matches found")
        return pl.DataFrame()

    logging.info(f"Found {len(pairs)} road-based candidate pairs")

    # Create pairs dataframe
    pairs_df = pl.DataFrame(pairs)

    # Join with original data
    logging.info("Joining road-based pairs with original data...")

    # Join MSD data
    pairs_df = pairs_df.join(
        msd_df.select(["msd_idx", "Lane", "TestDateUTC_parsed", "Chain", "RoadName", "road_id", "region_id"]),
        on="msd_idx",
        how="left",
    ).rename(
        {
            "Lane": "Lane_msd",
            "TestDateUTC_parsed": "Time_msd",
            "Chain": "Chain_msd",
            "RoadName": "RoadName_msd",
            "road_id": "road_id_msd",
            "region_id": "region_id_msd",
        }
    )

    # Join LMD data
    pairs_df = pairs_df.join(
        lmd_df.select(["lmd_idx", "Lanes", "TestDateUTC_parsed", "Chain", "RoadName", "road_id", "region_id", "Start Chainage (km)", "End Chainage (km)"]),
        on="lmd_idx",
        how="left",
    ).rename(
        {
            "Lanes": "Lane_lmd",
            "TestDateUTC_parsed": "Time_lmd",
            "Chain": "Chain_lmd",
            "RoadName": "RoadName_lmd",
            "road_id": "road_id_lmd",
            "region_id": "region_id_lmd",
            "Start Chainage (km)": "Start_Chainage_km_lmd",
            "End Chainage (km)": "End_Chainage_km_lmd",
        }
    )

    return pairs_df