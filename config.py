"""
Configuration constants and settings for MSD-LMD merge application.
"""

import logging

# Set up logging - reduced to INFO for speed
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Default LMD output columns (comprehensive list)
DEFAULT_LMD_OUTPUT_COLS = [
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

# Default LMD columns for smaller output (essential columns only)
DEFAULT_LMD_ESSENTIAL_COLS = [
    "BinViewerVersion", "Filename", "tsdSlope3000", "tsdSlope2000",
    "tsdSlope1000", "compositeModulus3000", "compositeModulus2000"
]

# LMD priority columns (always included without suffix)
LMD_PRIORITY_COLS = [
    "Latitude",
    "Longitude",
    "TestDateUTC",
    "RoadName",
    "Lane",
    "RoadID",
]

# LMD columns that always get _lmd suffix
LMD_FORCE_SUFFIX_COLS = ["Bearing", "GPSspeed", "DMIspeed"]

# Valid lanes
VALID_LANES = ["L1", "R1", "L2", "R2", "LSK1", "RSK1"]

# Default matching parameters
DEFAULT_MAX_TIME_DIFF_SEC = 10
DEFAULT_MAX_CHAINAGE_DIFF_M = 5.0
DEFAULT_MAX_SPATIAL_DIST_M = 100

# Earth radius in meters (for spatial calculations)
EARTH_RADIUS_M = 6371000