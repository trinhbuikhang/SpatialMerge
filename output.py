"""
Output functions for creating and saving merged data results.
"""

import polars as pl
import re
import os
import logging
from config import LMD_PRIORITY_COLS, LMD_FORCE_SUFFIX_COLS


def create_output_dataframe(best_matches, msd_df, lmd_df, lmd_output_cols, lmd_suffix="_lmd"):
    """
    Create final output dataframe with all MSD columns plus user-selected LMD columns.
    All MSD rows are preserved, LMD columns are null when no match exists.
    """
    logging.info("Building final output...")

    # Start with ALL MSD rows (not just matches)
    result = msd_df.clone()

    # Add match information (left join, so unmatched MSD rows get null values)
    match_columns = ["msd_idx", "lmd_idx"]
    if "time_diff" in best_matches.columns:
        match_columns.append("time_diff")
    if "chainage_diff" in best_matches.columns:
        match_columns.append("chainage_diff")

    result = result.join(
        best_matches.select(match_columns),
        on="msd_idx",
        how="left"
    )

    # Add user-selected LMD columns
    if lmd_output_cols:
        lmd_user_cols = []
        for col in lmd_output_cols:
            if col in lmd_df.columns:
                # Check for conflicts with MSD columns or existing columns in result
                existing_cols = set(result.columns)
                if col in existing_cols:
                    # Find a unique suffix starting with user-defined suffix
                    base_suffix = lmd_suffix
                    suffix = base_suffix
                    counter = 2
                    while f"{col}{suffix}" in existing_cols:
                        suffix = f"{base_suffix}{counter}"
                        counter += 1
                    lmd_user_cols.append(pl.col(col).alias(f"{col}{suffix}"))
                else:
                    # No conflict - keep original name
                    lmd_user_cols.append(pl.col(col))

        if lmd_user_cols:
            lmd_user_data = lmd_df.select(["lmd_idx", *lmd_user_cols])
            result = result.join(lmd_user_data, on="lmd_idx", how="left")

    # Convert duration columns to seconds for CSV compatibility
    convert_columns = []
    if "time_diff" in result.columns:
        convert_columns.append(pl.col("time_diff").dt.total_seconds().alias("time_diff_seconds"))
    if "chainage_diff" in result.columns:
        convert_columns.append(pl.col("chainage_diff").alias("chainage_diff_meters"))

    if convert_columns:
        result = result.with_columns(convert_columns)

    # Drop helper columns (preserve original MSD row order)
    cols_to_drop = [
        "time_diff", "chainage_diff", "msd_idx", "lmd_idx",
        "Lane_msd", "Time_msd", "Chain_msd", "RoadName_msd",
        "Lane_lmd", "Time_lmd", "Chain_lmd", "RoadName_lmd",
        "TestDateUTC_parsed", "Lat", "Lon", "Chain"
    ]
    result = result.drop([c for c in cols_to_drop if c in result.columns])

    logging.info(f"Output columns: {list(result.columns)}")
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

    # Time difference statistics (only if time_diff_seconds column exists)
    if "time_diff_seconds" in result.columns:
        time_stats = result.select("time_diff_seconds").filter(pl.col("time_diff_seconds").is_not_null())
        if len(time_stats) > 0:
            logging.info(
                f"Time diff - Min: {time_stats['time_diff_seconds'].min():.2f}s, "
                f"Mean: {time_stats['time_diff_seconds'].mean():.2f}s, "
                f"Max: {time_stats['time_diff_seconds'].max():.2f}s"
            )
        else:
            logging.info("Time diff - No valid time differences")
    else:
        logging.info("Time diff - Not available (time matching disabled)")

    # Chainage difference statistics (only if chainage_diff_meters column exists)
    if "chainage_diff_meters" in result.columns:
        chainage_stats = result.select("chainage_diff_meters").filter(pl.col("chainage_diff_meters").is_not_null())
        if len(chainage_stats) > 0:
            logging.info(
                f"Chainage diff - Min: {chainage_stats['chainage_diff_meters'].min():.2f}m, "
                f"Mean: {chainage_stats['chainage_diff_meters'].mean():.2f}m, "
                f"Max: {chainage_stats['chainage_diff_meters'].max():.2f}m"
            )
        else:
            logging.info("Chainage diff - No valid chainage differences")
    else:
        logging.info("Chainage diff - Not available (road-based matching)")