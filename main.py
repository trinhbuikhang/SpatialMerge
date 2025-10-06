"""
Main application for MSD-LMD data merging.
"""

import logging
from config import (
    DEFAULT_LMD_OUTPUT_COLS,
    DEFAULT_MAX_TIME_DIFF_SEC,
    DEFAULT_MAX_CHAINAGE_DIFF_M,
    DEFAULT_MAX_SPATIAL_DIST_M
)
from file_utils import select_file, select_output_directory, get_user_input, select_lmd_columns
from data_preparation import prepare_msd_data, prepare_lmd_data
from matching import perform_spatial_matching, filter_and_select_matches
from output import create_output_dataframe, save_output


def merge_msd_lmd(
    msd_path,
    lmd_path,
    output_dir=None,
    max_time_diff_sec=DEFAULT_MAX_TIME_DIFF_SEC,
    max_chainage_diff_m=DEFAULT_MAX_CHAINAGE_DIFF_M,
    max_spatial_dist_m=DEFAULT_MAX_SPATIAL_DIST_M,
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
        lmd_output_cols = DEFAULT_LMD_OUTPUT_COLS

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


def run_console():
    """
    Main console program for merging MSD and LMD data.
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

    # Read LMD temporarily to get column info
    try:
        import polars as pl
        lmd_temp = pl.read_csv(lmd_path, infer_schema_length=0)
    except Exception as e:
        print(f"Error reading LMD file: {e}")
        return

    # Select LMD columns
    print("\n3. Select LMD columns to include in output:")
    lmd_output_cols = select_lmd_columns(lmd_temp)

    # Configure matching parameters
    print("\n4. Configure matching parameters:")
    max_time_diff = int(get_user_input("Maximum time difference (seconds)", str(DEFAULT_MAX_TIME_DIFF_SEC)))
    max_chainage_diff = float(get_user_input("Maximum chainage difference (m)", str(DEFAULT_MAX_CHAINAGE_DIFF_M)))
    max_spatial_dist = int(get_user_input("Maximum spatial distance (m)", str(DEFAULT_MAX_SPATIAL_DIST_M)))

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
        logging.error(f"Error in run_console: {e}")


def main():
    """
    Main entry point - determines whether to run GUI or console mode.
    """
    import argparse
    import sys

    parser = argparse.ArgumentParser(description="MSD-LMD Data Merger")
    parser.add_argument('--gui', action='store_true', help='Run in GUI mode (default)')
    parser.add_argument('--console', action='store_true', help='Run in console mode')

    args = parser.parse_args()

    # Determine mode
    if args.console:
        run_console()
    elif args.gui:
        try:
            from gui import run_gui
            run_gui()
        except ImportError:
            print("PyQt6 not available. Cannot run GUI mode.")
            print("Install PyQt6 with: pip install PyQt6")
            sys.exit(1)
    else:
        # Default: try GUI first, fallback to console
        try:
            from gui import run_gui
            run_gui()
        except ImportError:
            print("PyQt6 not available. Running in console mode...")
            run_console()


if __name__ == "__main__":
    main()