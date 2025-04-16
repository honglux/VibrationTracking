# Vibration Data Analysis

This folder contains Python scripts for analyzing vibration data from the sensors.

## Scripts

1. **vibration_analyzer.py** - Core module with the `VibrationAnalyzer` class for data analysis and visualization
2. **run_analysis.py** - User-friendly script to select and analyze specific data files
3. **database_manager.py** - Database management module for storing and retrieving analysis results

## Requirements

The scripts require the following Python packages:
- pandas
- numpy
- matplotlib
- sqlite3 (built into Python)

You can install them with:
```
pip install pandas numpy matplotlib
```

Or using the provided requirements.txt file:
```
pip install -r Scripts/requirements.txt
```

## Usage

### Running the analysis

To analyze vibration data:

1. Ensure your data files are in the `VibData` folder
2. Navigate to the root directory
3. Run the script:

```
python Scripts/run_analysis.py
```

4. Select the file number you want to analyze from the displayed list
5. The script will:
   - Read and process the data
   - Group measurements by second
   - Calculate vibration metrics
   - Generate plots
   - Save results as CSV and PNG files in the `Results` folder
   - Store the analysis results in the SQLite database

### Using the Database

The analysis results are automatically stored in a SQLite database (`vibration_analysis.db`). You can:

1. List all analysis sessions:
```python
from database_manager import DatabaseManager
db = DatabaseManager()
sessions = db.list_sessions()
```

2. Retrieve specific session data:
```python
session_data = db.get_session_data(session_id)
```

3. Get summary statistics:
```python
summary = db.get_session_summary(session_id)
```

4. Delete a session:
```python
db.delete_session(session_id)
```

### Using the Analyzer Class

You can also use the `VibrationAnalyzer` class directly in your code:

```python
from vibration_analyzer import VibrationAnalyzer

# Create analyzer instance
analyzer = VibrationAnalyzer("path/to/data.txt")

# Read and analyze data
analyzer.read_data()
analyzer.analyze_data_by_second()

# Generate plots
analyzer.plot_vibration_levels()
analyzer.plot_displacement_data()
analyzer.plot_severity_score()

# Save results
analyzer.save_results()
```

## Output Files

The scripts generate the following output files in the `Results` folder:

1. `vibration_analysis_FILENAME.csv` - CSV file with processed data grouped by second
2. `vibration_analysis_FILENAME.png` - Plot of vibration levels over time
3. `displacement_analysis_FILENAME.png` - Plot of displacement data over time
4. `severity_analysis_FILENAME.png` - Plot of the vibration severity score over time

## Database Structure

The SQLite database contains three main tables:

1. `analysis_sessions` - Stores metadata about each analysis run:
   - File name
   - Analysis date
   - Total samples
   - Duration
   - Temperature statistics
   - Notes

2. `vibration_data` - Stores the processed data by second:
   - Vibration levels (mean, max, std)
   - Displacement data (X, Y, Z)
   - Temperature
   - Severity score

3. `analysis_results` - Stores plot paths and summary statistics:
   - Paths to generated plots
   - Summary statistics in JSON format

## Analysis Details

The vibration analysis includes:

- Grouping data by second
- Calculating overall vibration level (RMS of X, Y, Z speeds)
- Computing mean, max, and standard deviation of vibration levels
- Computing a vibration severity score that combines velocity and displacement metrics:

```
Severity Score = velocity_score * (1 + displacement_factor)
```

Where:
- `velocity_score = mean * (1 + peak_factor) * (1 + variability_factor)`
- `peak_factor = (max/mean - 1) * 0.5` (gives half weight to how much max exceeds mean)
- `variability_factor = std/mean` (gives full weight to relative standard deviation)
- `displacement_factor = (disp_x + disp_y + disp_z) / (3 * mean) * 0.5` (gives half weight to displacement)

This score provides a single measurement that considers:
- Base vibration level (mean)
- Peak intensity (max)
- Variability/instability (std)
- Displacement in all axes

Higher scores indicate potentially more problematic vibration conditions.

## Customization

You can modify the scripts to:
- Change the analysis parameters
- Add more metrics
- Customize the visualization
- Process multiple files at once
- Add new database queries
- Modify the severity score calculation 