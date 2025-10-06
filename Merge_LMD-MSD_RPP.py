import polars as pl
import re
import os
import logging
import math
import numpy as np
from sklearn.neighbors import BallTree
from datetime import timedelta
import tkinter as tk
from tkinter import filedialog, messagebox

# Set up logging - reduced to INFO for speed
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def select_file(title="Select file", filetypes=[("CSV files", "*.csv"), ("All files", "*.*")]):
    """
    Open dialog to select file.
    """
    root = tk.Tk()
    root.withdraw()  # Hide main window
    file_path = filedialog.askopenfilename(title=title, filetypes=filetypes)
    return file_path


def select_output_directory(title="Select output directory"):
    """
    Open dialog to select output directory.
    """
    root = tk.Tk()
    root.withdraw()
    dir_path = filedialog.askdirectory(title=title)
    return dir_path


def get_user_input(prompt, default_value=""):
    """
    Get input from user with default value.
    """
    value = input(f"{prompt} (default: {default_value}): ").strip()
    return value if value else default_value


def select_lmd_columns(lmd_df, default_cols=None):
    """
    Allow user to select LMD columns to include in output.
    """
    if default_cols is None:
        default_cols = [
            "BinViewerVersion", "Filename", "tsdSlope3000", "tsdSlope2000",
            "tsdSlope1000", "compositeModulus3000", "compositeModulus2000"
        ]

    available_cols = [col for col in lmd_df.columns if col not in ["lmd_idx", "Lat", "Lon", "Chain", "TestDateUTC_parsed"]]

    print("Available LMD columns:")
    for i, col in enumerate(available_cols, 1):
        print(f"{i}. {col}")

    print(f"\nDefault columns: {', '.join(default_cols)}")
    choice = get_user_input("Enter column numbers to select (comma-separated), or Enter for default", "")

    if not choice:
        return default_cols

    try:
        indices = [int(x.strip()) - 1 for x in choice.split(",")]
        selected_cols = [available_cols[i] for i in indices if 0 <= i < len(available_cols)]
        return selected_cols
    except (ValueError, IndexError):
        print("Invalid selection. Using default columns.")
        return default_cols


def prepare_msd_data(msd_path):
    """
    Read and prepare MSD data.
    """
    msd_df = pl.read_csv(msd_path, infer_schema_length=0)

    # Strip whitespace and prepare columns
    msd_df = msd_df.with_columns(
        [pl.col("RoadName").str.strip_chars(), pl.col("Lane").str.strip_chars()]
    )

    # Map Lane values
    msd_df = msd_df.with_columns(
        pl.when(pl.col("Lane") == "1")
        .then(pl.lit("L1"))
        .when(pl.col("Lane") == "2")
        .then(pl.lit("L2"))
        .otherwise(pl.col("Lane"))
        .alias("Lane")
    )

    # Filter to valid lanes
    valid_lanes = ["L1", "R1", "L2", "R2", "LSK1", "RSK1"]
    msd_df = msd_df.filter(pl.col("Lane").is_in(valid_lanes))

    # Parse dates
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
        .alias("TestDateUTC_parsed")
    )

    # Filter out rows with unparseable dates
    msd_df = msd_df.filter(pl.col("TestDateUTC_parsed").is_not_null())

    # Parse coordinates and chainage
    msd_df = msd_df.with_columns(
        [
            pl.col("Latitude").cast(pl.Float64, strict=False).alias("Lat"),
            pl.col("Longitude").cast(pl.Float64, strict=False).alias("Lon"),
            pl.col("Chainage").cast(pl.Float64, strict=False).alias("Chain"),
        ]
    ).filter(
        pl.col("Lat").is_not_null()
        & pl.col("Lon").is_not_null()
        & pl.col("Chain").is_not_null()
    )

    # Add row index
    msd_df = msd_df.with_row_index("msd_idx")

    return msd_df


def prepare_lmd_data(lmd_path):
    """
    Read and prepare LMD data.
    """
    lmd_df = pl.read_csv(lmd_path, infer_schema_length=0)

    # Prepare LMD columns
    lmd_columns = []
    if "Road Name" in lmd_df.columns:
        lmd_columns.append(pl.col("Road Name").str.strip_chars().alias("RoadName"))
    if "lane" in lmd_df.columns:
        lmd_columns.append(pl.col("lane").str.strip_chars().alias("Lane"))
    if "longitude" in lmd_df.columns:
        lmd_columns.append(
            pl.col("longitude").cast(pl.Float64, strict=False).alias("Longitude")
        )
    if "latitude" in lmd_df.columns:
        lmd_columns.append(
            pl.col("latitude").cast(pl.Float64, strict=False).alias("Latitude")
        )
    if lmd_columns:
        lmd_df = lmd_df.with_columns(lmd_columns)

    # Map LMD lane values
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
    valid_lanes = ["L1", "R1", "L2", "R2", "LSK1", "RSK1"]
    if "Lane" in lmd_df.columns:
        lmd_df = lmd_df.filter(pl.col("Lane").is_in(valid_lanes))

    # Parse dates
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
        .alias("TestDateUTC_parsed")
    )

    # Filter out rows with unparseable dates
    lmd_df = lmd_df.filter(pl.col("TestDateUTC_parsed").is_not_null())

    # Parse coordinates and chainage
    lmd_df = lmd_df.with_columns(
        [
            pl.col("Latitude").cast(pl.Float64, strict=False).alias("Lat"),
            pl.col("Longitude").cast(pl.Float64, strict=False).alias("Lon"),
            (pl.col("location").cast(pl.Float64, strict=False) * 1000).alias("Chain"),
        ]
    ).filter(
        pl.col("Lat").is_not_null()
        & pl.col("Lon").is_not_null()
        & pl.col("Chain").is_not_null()
    )

    # Add row index
    lmd_df = lmd_df.with_row_index("lmd_idx")

    return lmd_df


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
    R = 6371000  # Earth radius in meters
    dist_limit_rad = max_spatial_dist_m / R

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


def create_output_dataframe(best_matches, msd_df, lmd_df, lmd_output_cols):
    """
    Create final output dataframe.
    """
    logging.info("Building final output...")

    # Define columns to keep from LMD without suffix (priority columns)
    lmd_priority_cols = [
        "Latitude",
        "Longitude",
        "TestDateUTC",
        "RoadName",
        "Lane",
        "RoadID",
    ]

    # Define LMD columns that should always get _lmd suffix
    lmd_force_suffix_cols = ["Bearing", "GPSspeed", "DMIspeed"]

    # Get all column names from both dataframes
    msd_cols = set(msd_df.columns) - {
        "msd_idx",
        "Lat",
        "Lon",
        "Chain",
        "TestDateUTC_parsed",
    }
    lmd_cols = set(lmd_df.columns) - {
        "lmd_idx",
        "Lat",
        "Lon",
        "Chain",
        "TestDateUTC_parsed",
    }

    # Find common columns (need suffix)
    common_cols = msd_cols & lmd_cols

    # Remove priority columns from common cols (these won't get suffix from LMD)
    common_cols = common_cols - set(lmd_priority_cols)

    logging.info(f"Common columns needing suffix: {sorted(common_cols)}")
    logging.info(f"LMD columns forced with suffix: {sorted(lmd_force_suffix_cols)}")
    logging.info(f"LMD output columns: {len(lmd_output_cols)} columns specified")

    # Join with full MSD data - NO suffix for MSD columns
    msd_cols_to_join = [
        c
        for c in msd_df.columns
        if c not in ["msd_idx", "Lat", "Lon", "Chain", "TestDateUTC_parsed"]
    ]
    msd_renamed = msd_df.select(
        [
            pl.col("msd_idx"),
            *[pl.col(c) for c in msd_cols_to_join],  # No suffix for MSD columns
        ]
    )

    result = best_matches.join(msd_renamed, on="msd_idx", how="left")

    # Join with full LMD data
    # Priority columns: keep without suffix (always included)
    # Common columns (not priority): add suffix (always included)
    # Force suffix columns: always add suffix (always included)
    # Specified LMD output columns: keep without suffix (user-configurable)
    lmd_cols_to_join = []
    for c in lmd_df.columns:
        if c == "lmd_idx":
            lmd_cols_to_join.append(pl.col("lmd_idx"))
        elif c in ["Lat", "Lon", "Chain", "TestDateUTC_parsed"]:
            continue  # Skip helper columns
        elif c in lmd_priority_cols:
            # Keep priority columns without suffix (always included)
            lmd_cols_to_join.append(pl.col(c))
        elif c in lmd_force_suffix_cols or c in common_cols:
            # Force suffix columns and common columns get suffix (always included)
            lmd_cols_to_join.append(pl.col(c).alias(f"{c}_lmd"))
        elif c in lmd_output_cols:
            # Specified LMD output columns keep original name (user-configurable)
            lmd_cols_to_join.append(pl.col(c))
        # Skip any other LMD columns not in the output list

    lmd_renamed = lmd_df.select(lmd_cols_to_join)
    result = result.join(lmd_renamed, on="lmd_idx", how="left")

    # Add metadata columns and convert duration to seconds for CSV compatibility
    # First check which columns exist
    cols_to_drop = ["time_diff", "chainage_diff", "msd_idx", "lmd_idx"]

    # Add optional columns to drop if they exist
    optional_drops = [
        "Lane_msd",
        "Lane_lmd",
        "Time_msd",
        "Time_lmd",
        "Chain_msd",
        "Chain_lmd",
        "RoadName_msd",
        "RoadName_lmd",
    ]
    cols_to_drop.extend([c for c in optional_drops if c in result.columns])

    result = result.with_columns(
        [
            pl.col("time_diff").dt.total_seconds().alias("time_diff_seconds"),
            pl.col("chainage_diff").alias("chainage_diff_meters"),
        ]
    )

    # Sort by msd_idx to preserve original MSD row order
    result = result.sort("msd_idx")

    # Drop helper columns
    result = result.drop([c for c in cols_to_drop if c in result.columns])

    return result


def save_output(result, msd_path, lmd_path, output_dir=None, msd_df=None):
    """
    Save results to CSV file.
    """
    # Extract version
    version_match = re.search(r"V1\.0\.0\.(\d+)", lmd_path)
    version = version_match.group(1) if version_match else "unknown"

    # Save output in a subfolder within the MSD file directory
    output_filename = f"LMD_MSD_Merged_V{version}.csv"
    if output_dir:
        output_path = os.path.join(output_dir, output_filename)
    else:
        # Create a subfolder in the MSD file directory
        msd_dir = os.path.dirname(msd_path)
        output_subfolder = os.path.join(msd_dir, "LMD-MSD_Merged")

        # Create the subfolder if it doesn't exist
        os.makedirs(output_subfolder, exist_ok=True)

        output_path = os.path.join(output_subfolder, output_filename)

    # Check for any null columns and log warning
    null_counts = result.null_count()
    logging.info(f"Checking data quality before save...")

    try:
        result.write_csv(output_path)
    except Exception as e:
        logging.error(f"Error writing CSV: {e}")
        logging.info("Attempting to fix data inconsistencies...")

        # Try to identify and fix the issue
        # Convert all columns to string to ensure consistency
        result_fixed = result.select(
            [pl.col(c).cast(pl.Utf8, strict=False).alias(c) for c in result.columns]
        )

        # Try writing again
        result_fixed.write_csv(output_path)
        logging.info("Fixed and saved with all columns as strings")

    logging.info(f"✓ Saved: {output_path}")
    logging.info(f"✓ Total matches: {len(result)}")
    if msd_df is not None:
        match_rate = len(result) / len(msd_df) * 100
        logging.info(f"✓ Match rate: {match_rate:.1f}% of MSD rows")
    else:
        logging.info("✓ Match rate: N/A (msd_df not provided)")

    # Summary statistics
    logging.info("\n=== MATCH STATISTICS ===")
    logging.info(
        f"Time diff - Min: {result['time_diff_seconds'].min():.2f}s, "
        f"Mean: {result['time_diff_seconds'].mean():.2f}s, "
        f"Max: {result['time_diff_seconds'].max():.2f}s"
    )
    logging.info(
        f"Chainage diff - Min: {result['chainage_diff_meters'].min():.2f}m, "
        f"Mean: {result['chainage_diff_meters'].mean():.2f}m, "
        f"Max: {result['chainage_diff_meters'].max():.2f}m"
    )


def merge_msd_lmd(
    msd_path,
    lmd_path,
    output_dir=None,
    max_time_diff_sec=10,
    max_chainage_diff_m=5.0,
    max_spatial_dist_m=100,
    lmd_output_cols=None,
):
    """
    Merge MSD and LMD CSV files based on spatial proximity, lane, time, and chainage matching.

    Parameters:
    - msd_path: Path to MSD CSV file
    - lmd_path: Path to LMD CSV file
    - output_dir: Optional output directory (defaults to subfolder in MSD directory)
    - max_time_diff_sec: Maximum time difference in seconds (default: 10)
    - max_chainage_diff_m: Maximum chainage difference in meters (default: 5.0)
    - max_spatial_dist_m: Maximum spatial distance in meters (default: 100)
    - lmd_output_cols: List of LMD columns to include in output (default: comprehensive list)
                      Use smaller list to reduce output file size while preserving matching functionality

    Example:
        # Use only essential LMD columns for smaller output
        essential_cols = ['BinViewerVersion', 'Filename', 'tsdSlope3000', 'tsdSlope2000']
        merge_msd_lmd(msd_path, lmd_path, lmd_output_cols=essential_cols)
    """
    if lmd_output_cols is None:
        lmd_output_cols = [
            "BinViewerVersion",
            "Filename",
            "lmd_sequence_num",
            "tsdLagOffset",
            "tsdZeroChange",
            "tsdSlopeMaxX",
            "tsdSlopeMaxY",
            "tsdSlopeMinX",
            "tsdSlopeMinY",
            "tsdCurveMaxX",
            "tsdCurveMaxY",
            "tsdCurveMinX",
            "tsdCurveMinY",
            "tsdJoltMaxX1",
            "tsdJoltMaxY1",
            "tsdJoltMinX",
            "tsdJoltMinY",
            "tsdJoltMaxX2",
            "tsdJoltMaxY2",
            "FitMethod",
            "CurveFitA",
            "CurveFitB",
            "CurveFitC",
            "CurveFitD",
            "CurveFitX",
            "CurveFitY",
            "ZeroOffset",
            "TrailingFactor",
            "r2",
            "NumIts",
            "tsd_d0",
            "tsd_d200",
            "tsd_d300",
            "tsd_d450",
            "tsd_d600",
            "tsd_d750",
            "tsd_d900",
            "tsd_d1200",
            "tsd_d1500",
            "tsd_d0s",
            "tsd_curvature",
            "FitCorrel",
            "compositeModulus3000",
            "compositeModulus2950",
            "compositeModulus2800",
            "compositeModulus2000",
            "compositeModulus1500",
            "compositeModulus1200",
            "compositeModulus900",
            "compositeModulus750",
            "compositeModulus600",
            "compositeModulus450",
            "compositeModulus300",
            "compositeModulus200",
            "compositeModulus0",
            "h1",
            "h2",
            "h3",
            "h4",
            "E1",
            "E2",
            "E3",
            "E4",
            "E5",
            "c0",
            "n",
            "tsd_d0",
            "tsd_d200",
            "tsd_d300",
            "tsd_d450",
            "tsd_d600",
            "tsd_d750",
            "tsd_d900",
            "tsd_d1200",
            "tsd_d1500",
            "Governing Distress Mode",
            "Governing Rem Life",
            "Governing LDE",
            "Governing Testing Life",
            "Governing Rem MESA",
            "Surface Cracking Down_remLife",
            "Bound Base Cracking Down 1_remLife",
            "Base Flexure_remLife",
            "Base Flexure 1_remLife",
            "Base Flexure 2_remLife",
            "Base Flexure 3_remLife",
            "Bound Base Cracking Upward_remLife",
            "Base Spreading_remLife",
            "Bound Subbase Cracking Upward_remLife",
            "Subgrade Rutting_remLife",
            "Subgrade Shear_remLife",
            "Subbase Spreading_remLife",
            "Subgrade Spreading_remLife",
            "Transverse Cracking_remLife",
            "Shrinkage_remLife",
            "Subbase Rutting_remLife",
            "Subbase Shear_remLife",
            "Aggregate Spreading_remLife",
            "Surface Flexure_remLife",
            "Shallow Shear_remLife",
            "Shallow Shear 2_remLife",
            "Shallow Shear 4_remLife",
            "Surface Cracking Upward_remLife",
            "Shallow Instability_remLife",
            "Shallow Instability 5_remLife",
            "Shallow Instability 7_remLife",
            "Aggregate Rutting_remLife",
            "Aggregate Shear_remLife",
            "Shallow Instability 4_remLife",
            "Shallow Instability 1_remLife",
            "Shallow Instability 2_remLife",
        ]

    # Prepare data
    msd_df = prepare_msd_data(msd_path)
    lmd_df = prepare_lmd_data(lmd_path)

    logging.info(f"MSD: {len(msd_df)} rows, LMD: {len(lmd_df)} rows")
    logging.info(f"LMD columns: {list(lmd_df.columns)}")

    # Perform spatial matching
    pairs_df = perform_spatial_matching(msd_df, lmd_df, max_spatial_dist_m)

    if pairs_df.is_empty():
        return

    # Filter and select best matches
    best_matches = filter_and_select_matches(pairs_df, max_time_diff_sec, max_chainage_diff_m)

    if best_matches.is_empty():
        return

    # Create final output
    result = create_output_dataframe(best_matches, msd_df, lmd_df, lmd_output_cols)

    # Save output
    save_output(result, msd_path, lmd_path, output_dir, msd_df)


def main():
    """
    Main program for merging MSD and LMD data with user interface.
    """
    print("=== MSD-LMD DATA MERGER ===")
    print("Welcome! This program helps you merge MSD and LMD data.")

    # Select MSD file
    print("\n1. Select MSD file:")
    msd_path = select_file("Select MSD file (CSV)")
    if not msd_path:
        print("No MSD file selected. Exiting program.")
        return
    print(f"Selected MSD: {msd_path}")

    # Select LMD file
    print("\n2. Select LMD file:")
    lmd_path = select_file("Select LMD file (CSV)")
    if not lmd_path:
        print("No LMD file selected. Exiting program.")
        return
    print(f"Selected LMD: {lmd_path}")

    # Read temporarily to get column info
    try:
        lmd_temp = pl.read_csv(lmd_path, infer_schema_length=0)
    except Exception as e:
        print(f"Error reading LMD file: {e}")
        return

    # Select LMD columns
    print("\n3. Select LMD columns to include in output:")
    lmd_output_cols = select_lmd_columns(lmd_temp)

    # Configure matching parameters
    print("\n4. Configure matching parameters:")
    max_time_diff = int(get_user_input("Maximum time difference (seconds)", "10"))
    max_chainage_diff = float(get_user_input("Maximum chainage difference (m)", "5.0"))
    max_spatial_dist = int(get_user_input("Maximum spatial distance (m)", "100"))

    # Select output directory
    print("\n5. Select output directory (Enter for default):")
    output_dir = select_output_directory()
    if not output_dir:
        output_dir = None
        print("Using default directory (subfolder in MSD directory)")

    # Confirm and run
    print("\n=== CONFIGURATION SUMMARY ===")
    print(f"MSD file: {msd_path}")
    print(f"LMD file: {lmd_path}")
    print(f"LMD columns: {len(lmd_output_cols)} columns")
    print(f"Max time diff: {max_time_diff}s")
    print(f"Max chainage diff: {max_chainage_diff}m")
    print(f"Max spatial dist: {max_spatial_dist}m")
    if output_dir:
        print(f"Output dir: {output_dir}")
    else:
        print("Output dir: default")

    confirm = get_user_input("Confirm running the program? (y/n)", "y")
    if confirm.lower() not in ['y', 'yes']:
        print("Cancelled.")
        return

    # Run merge
    print("\n=== STARTING MERGE ===")
    try:
        merge_msd_lmd(
            msd_path=msd_path,
            lmd_path=lmd_path,
            output_dir=output_dir,
            max_time_diff_sec=max_time_diff,
            max_chainage_diff_m=max_chainage_diff,
            max_spatial_dist_m=max_spatial_dist,
            lmd_output_cols=lmd_output_cols
        )
        print("\n✓ Completed!")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        logging.error(f"Error in main: {e}")


if __name__ == "__main__":
    main()
