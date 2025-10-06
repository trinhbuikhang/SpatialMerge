"""
File utilities and user input functions for MSD-LMD merge application.
"""

import tkinter as tk
from tkinter import filedialog


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
        from config import DEFAULT_LMD_ESSENTIAL_COLS
        default_cols = DEFAULT_LMD_ESSENTIAL_COLS

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