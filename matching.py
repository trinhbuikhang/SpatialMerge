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

    # Query all at once
    indices_list = tree.query_radius(msd_coords, r=dist_limit_rad)

    # Create candidate pairs in bulk
    logging.info("Creating candidate pairs...")
    pairs = []
    for msd_idx, lmd_indices in enumerate(indices_list):
        for lmd_idx in lmd_indices:
            pairs.append({"msd_idx": msd_idx, "lmd_idx": int(lmd_idx)})

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

    return pairs_df


def filter_and_select_matches(pairs_df, max_time_diff_sec=10, max_chainage_diff_m=5.0):
    """
    Filter pairs and select best matches.
    """
    logging.info("Applying filters...")

    pairs_df = pairs_df.with_columns(
        [
            (pl.col("Time_msd") - pl.col("Time_lmd")).abs().alias("time_diff"),
            (pl.col("Chain_msd") - pl.col("Chain_lmd")).abs().alias("chainage_diff"),
        ]
    )

    # Apply all filters
    filtered = pairs_df.filter(
        (pl.col("Lane_msd") == pl.col("Lane_lmd"))
        & (pl.col("time_diff") <= pl.duration(seconds=max_time_diff_sec))
        & (pl.col("chainage_diff") <= max_chainage_diff_m)
    )

    logging.info(f"After filtering: {len(filtered)} valid pairs")

    if filtered.is_empty():
        logging.warning("No matches found after filtering")
        return pl.DataFrame()

    # Select best match per MSD row
    logging.info("Selecting best matches...")

    # Sort by time_diff and keep first (best) match per msd_idx
    best_matches = filtered.sort("time_diff").group_by("msd_idx").first()

    logging.info(f"Final matches: {len(best_matches)}")

    return best_matches