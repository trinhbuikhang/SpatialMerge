"""
GUI application for MSD-LMD data merging using PyQt6.
"""

import sys
import os
import logging
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QSpinBox, QDoubleSpinBox,
    QTextEdit, QFileDialog, QProgressBar, QGroupBox, QListWidget,
    QListWidgetItem, QCheckBox, QSplitter, QTableWidget, QTableWidgetItem,
    QMessageBox, QStatusBar, QComboBox, QScrollArea
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
from PyQt6.QtGui import QFont, QIcon

import polars as pl
from config import (
    DEFAULT_LMD_OUTPUT_COLS,
    DEFAULT_MAX_TIME_DIFF_SEC,
    DEFAULT_MAX_CHAINAGE_DIFF_M,
    DEFAULT_MAX_SPATIAL_DIST_M
)
from file_utils import select_lmd_columns
from data_preparation import prepare_msd_data, prepare_lmd_data
from matching import perform_spatial_matching, filter_and_select_matches
from output import create_output_dataframe, save_output


class WorkerThread(QThread):
    """Worker thread for running the merge process without blocking GUI."""
    progress = pyqtSignal(str)  # Progress message
    finished = pyqtSignal(object)  # Result or None
    error = pyqtSignal(str)  # Error message

    def __init__(self, params):
        super().__init__()
        self.params = params

    def run(self):
        try:
            self.progress.emit("Preparing data...")

            # Prepare data
            msd_df = prepare_msd_data(self.params['msd_path'])
            lmd_df = prepare_lmd_data(self.params['lmd_path'])

            self.progress.emit(f"MSD: {len(msd_df)} rows, LMD: {len(lmd_df)} rows")

            # Perform spatial matching
            self.progress.emit("Performing spatial matching...")
            pairs_df = perform_spatial_matching(msd_df, lmd_df, self.params['max_spatial_dist'])

            if pairs_df.is_empty():
                self.finished.emit(None)
                return

            # Filter and select best matches
            self.progress.emit("Filtering and selecting best matches...")
            best_matches = filter_and_select_matches(pairs_df, self.params['max_time_diff'], self.params['max_chainage_diff'])

            if best_matches.is_empty():
                self.finished.emit(None)
                return

            # Create final output
            self.progress.emit("Creating final output...")
            result = create_output_dataframe(best_matches, msd_df, lmd_df, self.params['lmd_output_cols'], self.params.get('lmd_suffix', '_lmd'))

            # Save output
            self.progress.emit("Saving results...")
            save_output(result, self.params['msd_path'], self.params['lmd_path'], self.params['output_dir'], msd_df)

            self.progress.emit("Completed!")
            self.finished.emit(result)

        except Exception as e:
            self.error.emit(str(e))


class MSDLMDMergerGUI(QMainWindow):
    """Main GUI window for MSD-LMD data merging."""

    def __init__(self):
        super().__init__()
        self.msd_path = ""
        self.lmd_path = ""
        self.output_dir = ""
        self.lmd_df = None
        self.worker = None

        self.init_ui()

    def init_ui(self):
        """Initialize the user interface."""
        self.setWindowTitle("MSD-LMD Data Merger v1.0")
        self.setGeometry(100, 100, 1200, 800)

        # Create central widget
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        # Main layout
        main_layout = QHBoxLayout(central_widget)

        # Create splitter for resizable panels
        splitter = QSplitter(Qt.Orientation.Horizontal)
        main_layout.addWidget(splitter)

        # Left panel - Controls
        left_panel = self.create_control_panel()
        splitter.addWidget(left_panel)

        # Right panel - Data preview
        right_panel = self.create_preview_panel()
        splitter.addWidget(right_panel)

        # Set splitter proportions
        splitter.setSizes([400, 800])

        # Status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.status_bar.showMessage("Ready")

        # Progress bar (initially hidden)
        self.progress_bar = QProgressBar()
        self.status_bar.addPermanentWidget(self.progress_bar)
        self.progress_bar.setVisible(False)

    def create_control_panel(self):
        """Create the control panel with all settings."""
        panel = QWidget()
        layout = QVBoxLayout(panel)

        # Title
        title = QLabel("MSD-LMD Data Merger")
        title.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)

        # File selection group
        file_group = QGroupBox("Select Files")
        file_layout = QVBoxLayout(file_group)

        # MSD file selection
        msd_layout = QHBoxLayout()
        self.msd_label = QLabel("MSD File: Not selected")
        self.msd_label.setStyleSheet("border: 1px solid #ccc; padding: 5px;")
        msd_btn = QPushButton("Select MSD")
        msd_btn.clicked.connect(self.select_msd_file)
        msd_layout.addWidget(self.msd_label)
        msd_layout.addWidget(msd_btn)
        file_layout.addLayout(msd_layout)

        # LMD file selection
        lmd_layout = QHBoxLayout()
        self.lmd_label = QLabel("LMD File: Not selected")
        self.lmd_label.setStyleSheet("border: 1px solid #ccc; padding: 5px;")
        lmd_btn = QPushButton("Select LMD")
        lmd_btn.clicked.connect(self.select_lmd_file)
        lmd_layout.addWidget(self.lmd_label)
        lmd_layout.addWidget(lmd_btn)
        file_layout.addLayout(lmd_layout)

        # Output directory selection
        output_layout = QHBoxLayout()
        self.output_label = QLabel("Output Dir: Default")
        self.output_label.setStyleSheet("border: 1px solid #ccc; padding: 5px;")
        output_btn = QPushButton("Select Output")
        output_btn.clicked.connect(self.select_output_dir)
        output_layout.addWidget(self.output_label)
        output_layout.addWidget(output_btn)
        file_layout.addLayout(output_layout)

        layout.addWidget(file_group)

        # Parameters group
        param_group = QGroupBox("Matching Parameters")
        param_layout = QVBoxLayout(param_group)

        # Time difference
        time_layout = QHBoxLayout()
        time_layout.addWidget(QLabel("Max time difference (sec):"))
        self.time_spin = QSpinBox()
        self.time_spin.setRange(1, 300)
        self.time_spin.setValue(DEFAULT_MAX_TIME_DIFF_SEC)
        time_layout.addWidget(self.time_spin)
        param_layout.addLayout(time_layout)

        # Chainage difference
        chainage_layout = QHBoxLayout()
        chainage_layout.addWidget(QLabel("Max chainage difference (m):"))
        self.chainage_spin = QDoubleSpinBox()
        self.chainage_spin.setRange(0.1, 100.0)
        self.chainage_spin.setSingleStep(0.5)
        self.chainage_spin.setValue(DEFAULT_MAX_CHAINAGE_DIFF_M)
        chainage_layout.addWidget(self.chainage_spin)
        param_layout.addLayout(chainage_layout)

        # Spatial distance
        spatial_layout = QHBoxLayout()
        spatial_layout.addWidget(QLabel("Max spatial distance (m):"))
        self.spatial_spin = QSpinBox()
        self.spatial_spin.setRange(10, 1000)
        self.spatial_spin.setValue(DEFAULT_MAX_SPATIAL_DIST_M)
        spatial_layout.addWidget(self.spatial_spin)
        param_layout.addLayout(spatial_layout)

        layout.addWidget(param_group)

        # LMD Columns group
        columns_group = QGroupBox("LMD Output Columns")
        columns_layout = QVBoxLayout(columns_group)

        columns_layout.addWidget(QLabel("Select LMD columns to include in output (scrollable list):"))
        self.columns_list = QListWidget()
        self.columns_list.setMinimumHeight(200)  # Allow more height for scrolling
        columns_layout.addWidget(self.columns_list)

        columns_btn_layout = QHBoxLayout()
        clear_btn = QPushButton("Clear All")
        clear_btn.clicked.connect(self.clear_columns)
        default_btn = QPushButton("Default")
        default_btn.clicked.connect(self.load_default_columns)
        columns_btn_layout.addWidget(clear_btn)
        columns_btn_layout.addWidget(default_btn)
        columns_layout.addLayout(columns_btn_layout)

        layout.addWidget(columns_group)

        # Suffix configuration group
        suffix_group = QGroupBox("Column Suffix Settings")
        suffix_layout = QVBoxLayout(suffix_group)

        suffix_layout.addWidget(QLabel("Suffix for LMD columns (when conflict):"))
        self.suffix_edit = QLineEdit("_lmd")
        self.suffix_edit.setPlaceholderText("Enter suffix (e.g., _lmd, _LMD, _from_lmd)")
        suffix_layout.addWidget(self.suffix_edit)

        layout.addWidget(suffix_group)

        # Control buttons
        button_layout = QHBoxLayout()

        self.run_btn = QPushButton("🚀 Run Merge")
        self.run_btn.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        self.run_btn.setStyleSheet("background-color: #4CAF50; color: white; padding: 10px;")
        self.run_btn.clicked.connect(self.run_merge)
        self.run_btn.setEnabled(False)

        self.stop_btn = QPushButton("⏹️ Stop")
        self.stop_btn.setEnabled(False)
        self.stop_btn.clicked.connect(self.stop_merge)

        button_layout.addWidget(self.run_btn)
        button_layout.addWidget(self.stop_btn)
        layout.addLayout(button_layout)

        # Log output
        log_group = QGroupBox("Log")
        log_layout = QVBoxLayout(log_group)
        self.log_text = QTextEdit()
        self.log_text.setMaximumHeight(200)
        self.log_text.setReadOnly(True)
        log_layout.addWidget(self.log_text)
        layout.addWidget(log_group)

        # Add stretch to push everything up
        layout.addStretch()

        return panel

    def create_preview_panel(self):
        """Create the data preview panel."""
        panel = QWidget()
        layout = QVBoxLayout(panel)

        # Preview title
        preview_title = QLabel("Data Preview")
        preview_title.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        preview_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(preview_title)

        # File selector for preview
        preview_controls = QHBoxLayout()
        preview_controls.addWidget(QLabel("Preview:"))
        self.preview_combo = QComboBox()
        self.preview_combo.addItems(["MSD", "LMD"])
        self.preview_combo.currentTextChanged.connect(self.update_preview)
        preview_controls.addWidget(self.preview_combo)

        load_preview_btn = QPushButton("Load Preview")
        load_preview_btn.clicked.connect(self.load_preview)
        preview_controls.addWidget(load_preview_btn)

        preview_controls.addStretch()
        layout.addLayout(preview_controls)

        # Data table
        self.data_table = QTableWidget()
        self.data_table.setAlternatingRowColors(True)
        layout.addWidget(self.data_table)

        return panel

    def select_msd_file(self):
        """Select MSD file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select MSD file", "", "CSV files (*.csv);;All files (*.*)"
        )
        if file_path:
            self.msd_path = file_path
            self.msd_label.setText(f"MSD File: {os.path.basename(file_path)}")
            self.check_run_enabled()
            self.log_message(f"Selected MSD: {file_path}")

    def select_lmd_file(self):
        """Select LMD file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select LMD file", "", "CSV files (*.csv);;All files (*.*)"
        )
        if file_path:
            self.lmd_path = file_path
            self.lmd_label.setText(f"LMD File: {os.path.basename(file_path)}")
            self.check_run_enabled()
            self.log_message(f"Selected LMD: {file_path}")

            # Load LMD data for column selection
            try:
                self.lmd_df = pl.read_csv(file_path, infer_schema_length=0)
                self.load_default_columns()
            except Exception as e:
                QMessageBox.warning(self, "Error", f"Cannot read LMD file: {e}")

    def select_output_dir(self):
        """Select output directory."""
        dir_path = QFileDialog.getExistingDirectory(self, "Select output directory")
        if dir_path:
            self.output_dir = dir_path
            self.output_label.setText(f"Output Dir: {os.path.basename(dir_path)}")
            self.log_message(f"Selected output dir: {dir_path}")
        else:
            self.output_dir = ""
            self.output_label.setText("Output Dir: Default")

    def clear_columns(self):
        """Uncheck all selected columns."""
        for i in range(self.columns_list.count()):
            self.columns_list.item(i).setCheckState(Qt.CheckState.Unchecked)

    def load_default_columns(self):
        """Load all available LMD columns with checkboxes."""
        if self.lmd_df is not None:
            # Get available columns
            available_cols = [col for col in self.lmd_df.columns
                            if col not in ["lmd_idx", "Lat", "Lon", "Chain", "TestDateUTC_parsed"]]

            # Define essential columns that should be checked by default
            essential_cols = ["BinViewerVersion", "Filename", "tsdSlope3000", "tsdSlope2000",
                            "tsdSlope1000", "compositeModulus3000", "compositeModulus2000"]

            # Update columns list with all available columns
            self.columns_list.clear()
            for col in available_cols:
                item = QListWidgetItem(col)
                # Check essential columns by default
                if col in essential_cols:
                    item.setCheckState(Qt.CheckState.Checked)
                else:
                    item.setCheckState(Qt.CheckState.Unchecked)
                self.columns_list.addItem(item)

            self.log_message(f"Loaded {len(available_cols)} LMD columns")

    def update_columns_list(self, columns):
        """Update the columns list widget (legacy method - kept for compatibility)."""
        pass

    def check_run_enabled(self):
        """Check if run button should be enabled."""
        self.run_btn.setEnabled(bool(self.msd_path and self.lmd_path))

    def run_merge(self):
        """Run the merge process."""
        if not self.msd_path or not self.lmd_path:
            QMessageBox.warning(self, "Warning", "Please select both MSD and LMD files!")
            return

        # Get selected columns
        selected_columns = []
        for i in range(self.columns_list.count()):
            item = self.columns_list.item(i)
            if item.checkState() == Qt.CheckState.Checked:
                selected_columns.append(item.text())

        if not selected_columns:
            QMessageBox.warning(self, "Warning", "Please select at least one LMD column!")
            return

        # Prepare parameters
        params = {
            'msd_path': self.msd_path,
            'lmd_path': self.lmd_path,
            'output_dir': self.output_dir if self.output_dir else None,
            'max_time_diff': self.time_spin.value(),
            'max_chainage_diff': self.chainage_spin.value(),
            'max_spatial_dist': self.spatial_spin.value(),
            'lmd_output_cols': selected_columns,
            'lmd_suffix': self.suffix_edit.text().strip() or "_lmd"
        }

        # Disable controls
        self.run_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Indeterminate progress

        # Start worker thread
        self.worker = WorkerThread(params)
        self.worker.progress.connect(self.update_progress)
        self.worker.finished.connect(self.on_merge_finished)
        self.worker.error.connect(self.on_merge_error)
        self.worker.start()

        self.log_message("Starting merge process...")

    def stop_merge(self):
        """Stop the merge process."""
        if self.worker and self.worker.isRunning():
            self.worker.terminate()
            self.worker.wait()
            self.on_merge_finished(None)
            self.log_message("Merge process stopped")

    def update_progress(self, message):
        """Update progress message."""
        self.status_bar.showMessage(message)
        self.log_message(message)

    def on_merge_finished(self, result):
        """Handle merge completion."""
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)

        if result is not None:
            self.status_bar.showMessage("Completed!")
            QMessageBox.information(self, "Success", "Merge process completed successfully!")
        else:
            self.status_bar.showMessage("No matches found")
            QMessageBox.information(self, "Info", "No suitable matches found")

    def on_merge_error(self, error_msg):
        """Handle merge error."""
        self.progress_bar.setVisible(False)
        self.run_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)

        self.status_bar.showMessage("Error!")
        self.log_message(f"Error: {error_msg}")
        QMessageBox.critical(self, "Error", f"An error occurred:\n{error_msg}")

    def load_preview(self):
        """Load data preview."""
        file_path = self.msd_path if self.preview_combo.currentText() == "MSD" else self.lmd_path

        if not file_path:
            QMessageBox.warning(self, "Warning", f"Please select {self.preview_combo.currentText()} file first!")
            return

        try:
            df = pl.read_csv(file_path, infer_schema_length=0)
            self.display_dataframe(df.limit(100))  # Show first 100 rows
            self.log_message(f"Loaded preview for {self.preview_combo.currentText()}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Cannot read file: {e}")

    def update_preview(self):
        """Update preview when combo box changes."""
        self.load_preview()

    def display_dataframe(self, df):
        """Display dataframe in the table widget."""
        self.data_table.setRowCount(min(len(df), 100))
        self.data_table.setColumnCount(len(df.columns))

        # Set headers
        self.data_table.setHorizontalHeaderLabels(df.columns)

        # Fill data
        for row in range(min(len(df), 100)):
            for col in range(len(df.columns)):
                value = str(df[row, col])
                self.data_table.setItem(row, col, QTableWidgetItem(value))

        # Resize columns to content
        self.data_table.resizeColumnsToContents()

    def log_message(self, message):
        """Add message to log."""
        self.log_text.append(message)
        # Auto scroll to bottom
        cursor = self.log_text.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        self.log_text.setTextCursor(cursor)


def run_gui():
    """Run the GUI application."""
    app = QApplication(sys.argv)
    app.setStyle('Fusion')  # Modern style

    # Set application properties
    app.setApplicationName("MSD-LMD Merger")
    app.setApplicationVersion("1.0.0")

    window = MSDLMDMergerGUI()
    window.show()

    sys.exit(app.exec())


if __name__ == "__main__":
    run_gui()