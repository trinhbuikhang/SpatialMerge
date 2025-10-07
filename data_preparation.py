"""
Data preparation functions for MSD and LMD data processing.
"""

import polars as pl
import logging
from config import VALID_LANES


def find_column_mapping(df, possible_names):
    """
    Find the best matching column name from a list of possible names.

    Args:
        df: Polars DataFrame
        possible_names: List of possible column names (case-insensitive)

    Returns:
        str: The actual column name found, or None if not found
    """
    available_cols = [col.lower() for col in df.columns]

    for name in possible_names:
        if name.lower() in available_cols:
            # Find the actual case-sensitive name
            for col in df.columns:
                if col.lower() == name.lower():
                    return col

    return None


def prepare_msd_data(msd_path):
    """
    Read and prepare MSD data with automatic column mapping.
    """
    msd_df = pl.read_csv(msd_path, infer_schema_length=0)

    # Automatic column mapping for coordinates
    lat_col = find_column_mapping(msd_df, ["latitude", "lat", "Latitude", "Lat"])
    lon_col = find_column_mapping(msd_df, ["longitude", "lon", "Longitude", "Lon"])
    chainage_col = find_column_mapping(msd_df, ["chainage", "chain", "Chainage", "Chain"])
    road_col = find_column_mapping(msd_df, ["roadname", "road_name", "road", "road_id", "roadid", "RoadName"])
    lane_col = find_column_mapping(msd_df, ["lane", "lane_id", "laneid", "Lane"])

    logging.info(f"MSD column mapping - Lat: {lat_col}, Lon: {lon_col}, Chainage: {chainage_col}, Road: {road_col}, Lane: {lane_col}")

    # Prepare columns with found mappings
    columns_to_add = []

    if road_col:
        columns_to_add.append(pl.col(road_col).str.strip_chars().alias("RoadName"))
    else:
        logging.warning("No road name column found in MSD data")

    if lane_col:
        columns_to_add.append(pl.col(lane_col).str.strip_chars().alias("Lane"))
    else:
        logging.warning("No lane column found in MSD data")

    if columns_to_add:
        msd_df = msd_df.with_columns(columns_to_add)

    # Map Lane values if Lane column exists
    if "Lane" in msd_df.columns:
        msd_df = msd_df.with_columns(
            pl.when(pl.col("Lane") == "1")
            .then(pl.lit("L1"))
            .when(pl.col("Lane") == "2")
            .then(pl.lit("L2"))
            .otherwise(pl.col("Lane"))
            .alias("Lane")
        )

        # Filter to valid lanes
        msd_df = msd_df.filter(pl.col("Lane").is_in(VALID_LANES))

    # Parse dates (only if TestDateUTC column exists)
    if "TestDateUTC" in msd_df.columns:
        msd_df = msd_df.with_columns(
            pl.col("TestDateUTC")
            .str.strptime(pl.Datetime, "%d/%m/%y %H:%M:%S%.f", strict=False)
            .fill_null(
                pl.col("TestDateUTC").str.strptime(
                    pl.Datetime, "%d/%m/%Y %H:%M:%S%.f", strict=False
                )
            )
            .fill_null(
                pl.col("TestDateUTC").str.strptime(
                    pl.Datetime, "%d/%m/%y %H:%M:%S", strict=False
                )
            )
            .fill_null(
                pl.col("TestDateUTC").str.strptime(
                    pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False
                )
            )
            .fill_null(
                pl.col("TestDateUTC").str.strptime(
                    pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ", strict=False
                )
            )
            .alias("TestDateUTC_parsed")
        )

        # Filter out rows with unparseable dates
        msd_df = msd_df.filter(pl.col("TestDateUTC_parsed").is_not_null())
    else:
        # Add null TestDateUTC_parsed column if no date column exists
        msd_df = msd_df.with_columns(pl.lit(None).cast(pl.Datetime).alias("TestDateUTC_parsed"))
        logging.warning("No TestDateUTC column found in MSD data - time matching will be disabled")

    # Parse coordinates and chainage with automatic mapping
    coord_columns = []

    if lat_col:
        coord_columns.append(pl.col(lat_col).cast(pl.Float64, strict=False).alias("Lat"))
    else:
        logging.error("No latitude column found in MSD data")
        return pl.DataFrame()

    if lon_col:
        coord_columns.append(pl.col(lon_col).cast(pl.Float64, strict=False).alias("Lon"))
    else:
        logging.error("No longitude column found in MSD data")
        return pl.DataFrame()

    if chainage_col:
        coord_columns.append(pl.col(chainage_col).cast(pl.Float64, strict=False).alias("Chain"))
    else:
        # Try to use location column as fallback for chainage
        location_col = find_column_mapping(msd_df, ["location", "Location"])
        if location_col:
            coord_columns.append((pl.col(location_col).cast(pl.Float64, strict=False) * 1000).alias("Chain"))
            logging.info(f"Using {location_col}*1000 as Chainage for MSD")
        else:
            logging.error("No chainage or location column found in MSD data")
            return pl.DataFrame()

    msd_df = msd_df.with_columns(coord_columns).filter(
        pl.col("Lat").is_not_null()
        & pl.col("Lon").is_not_null()
        & pl.col("Chain").is_not_null()
    )

    # Add row index
    msd_df = msd_df.with_row_index("msd_idx")

    logging.info(f"MSD data prepared: {len(msd_df)} rows")
    return msd_df


def prepare_lmd_data(lmd_path):
    """
    Read and prepare LMD data with automatic column mapping.
    """
    lmd_df = pl.read_csv(lmd_path, infer_schema_length=0)

    # Automatic column mapping
    lat_col = find_column_mapping(lmd_df, ["latitude", "lat", "Latitude", "Lat"])
    lon_col = find_column_mapping(lmd_df, ["longitude", "lon", "Longitude", "Lon"])
    chainage_col = find_column_mapping(lmd_df, ["chainage", "chain", "Chainage", "Chain"])
    road_col = find_column_mapping(lmd_df, ["roadname", "road_name", "road", "road_id", "roadid", "Road Name", "RoadName"])
    lane_col = find_column_mapping(lmd_df, ["lane", "lane_id", "laneid", "Lane"])

    logging.info(f"LMD column mapping - Lat: {lat_col}, Lon: {lon_col}, Chainage: {chainage_col}, Road: {road_col}, Lane: {lane_col}")

    # Prepare columns with found mappings
    columns_to_add = []

    if road_col:
        columns_to_add.append(pl.col(road_col).str.strip_chars().alias("RoadName"))

    if lane_col:
        columns_to_add.append(pl.col(lane_col).str.strip_chars().alias("Lane"))

    if columns_to_add:
        lmd_df = lmd_df.with_columns(columns_to_add)

    # Map LMD lane values if Lane column exists
    if "Lane" in lmd_df.columns:
        lmd_df = lmd_df.with_columns(
            pl.when(pl.col("Lane") == "1")
            .then(pl.lit("L1"))
            .when(pl.col("Lane") == "2")
            .then(pl.lit("L2"))
            .otherwise(pl.col("Lane"))
            .alias("Lane")
        )

        # Filter to valid lanes
        lmd_df = lmd_df.filter(pl.col("Lane").is_in(VALID_LANES))

    # Parse dates (only if TestDateUTC column exists)
    if "TestDateUTC" in lmd_df.columns:
        lmd_df = lmd_df.with_columns(
            pl.col("TestDateUTC")
            .str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ", strict=False)
            .fill_null(
                pl.col("TestDateUTC").str.strptime(
                    pl.Datetime, "%d/%m/%Y %H:%M:%S%.f", strict=False
                )
            )
            .fill_null(
                pl.col("TestDateUTC").str.strptime(
                    pl.Datetime, "%d/%m/%y %H:%M:%S%.f", strict=False
                )
            )
            .fill_null(
                pl.col("TestDateUTC").str.strptime(
                    pl.Datetime, "%d/%m/%Y %H:%M:%S", strict=False
                )
            )
            .fill_null(
                pl.col("TestDateUTC").str.strptime(
                    pl.Datetime, "%d/%m/%y %H:%M:%S", strict=False
                )
            )
            .fill_null(
                pl.col("TestDateUTC").str.strptime(
                    pl.Datetime, "%Y-%m-%dT%H:%M:%S%.fZ", strict=False
                )
            )
            .alias("TestDateUTC_parsed")
        )

        # Filter out rows with unparseable dates
        lmd_df = lmd_df.filter(pl.col("TestDateUTC_parsed").is_not_null())
    else:
        # Add null TestDateUTC_parsed column if no date column exists
        lmd_df = lmd_df.with_columns(pl.lit(None).cast(pl.Datetime).alias("TestDateUTC_parsed"))
        logging.warning("No TestDateUTC column found in LMD data - time matching will be disabled")

    # Parse coordinates and chainage with automatic mapping (coordinates are optional for LMD)
    coord_columns = []

    if lat_col:
        coord_columns.append(pl.col(lat_col).cast(pl.Float64, strict=False).alias("Lat"))
    else:
        # Add null Lat column if no latitude column exists
        coord_columns.append(pl.lit(None).cast(pl.Float64).alias("Lat"))
        logging.warning("No latitude column found in LMD data - spatial matching will be disabled")

    if lon_col:
        coord_columns.append(pl.col(lon_col).cast(pl.Float64, strict=False).alias("Lon"))
    else:
        # Add null Lon column if no longitude column exists
        coord_columns.append(pl.lit(None).cast(pl.Float64).alias("Lon"))
        logging.warning("No longitude column found in LMD data - spatial matching will be disabled")

    if chainage_col:
        coord_columns.append(pl.col(chainage_col).cast(pl.Float64, strict=False).alias("Chain"))
    elif "location" in lmd_df.columns:
        # Use location * 1000 as chainage if no chainage column found
        coord_columns.append((pl.col("location").cast(pl.Float64, strict=False) * 1000).alias("Chain"))
        logging.info("Using location*1000 as Chainage for LMD")
    elif "Start Chainage (km)" in lmd_df.columns:
        # Use start chainage * 1000 for LMD feedback files
        coord_columns.append((pl.col("Start Chainage (km)").cast(pl.Float64, strict=False) * 1000).alias("Chain"))
        logging.info("Using Start Chainage (km)*1000 as Chainage for LMD")
    else:
        # Add null Chain column if no chainage column exists
        coord_columns.append(pl.lit(None).cast(pl.Float64).alias("Chain"))
        logging.warning("No chainage column found in LMD data - road matching may be limited")

    lmd_df = lmd_df.with_columns(coord_columns)

    # Filter only for required columns (Chain is required for road matching, Lat/Lon are optional)
    filter_conditions = [pl.col("Chain").is_not_null()]
    lmd_df = lmd_df.filter(*filter_conditions)

    # Add row index
    lmd_df = lmd_df.with_row_index("lmd_idx")

    logging.info(f"LMD data prepared: {len(lmd_df)} rows")
    return lmd_df