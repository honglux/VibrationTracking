# Vibration Data Analysis

This folder contains Python scripts for analyzing vibration data from the sensors.

## Scripts

1. **vibration_analyzer.py** - Core module with functions to read, analyze, and visualize vibration data
2. **run_analysis.py** - User-friendly script to select and analyze a specific data file

## Requirements

The scripts require the following Python packages:
- pandas
- numpy
- matplotlib

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

## Output Files

The scripts generate the following output files in the `Results` folder:

1. `vibration_analysis_FILENAME.csv` - CSV file with processed data grouped by second
2. `vibration_analysis_FILENAME.png` - Plot of vibration levels over time
3. `displacement_analysis_FILENAME.png` - Plot of displacement data over time
4. `severity_analysis_FILENAME.png` - Plot of the vibration severity score over time

## Analysis Details

The vibration analysis includes:

- Grouping data by second
- Calculating overall vibration level (RMS of X, Y, Z speeds)
- Computing mean, max, and standard deviation of vibration levels
- Computing a vibration severity score that combines mean, max, and std
- Analyzing displacement in X, Y, and Z directions
- Temperature tracking

### Vibration Level Calculation

The vibration level is calculated as:
```
Vibration Level = sqrt(speed_x² + speed_y² + speed_z²)
```

### Severity Score

The vibration severity score is a comprehensive metric that combines mean, max, and standard deviation into a single value for each second:

```
Severity Score = mean * (1 + peak_factor) * (1 + variability_factor)
```

Where:
- `peak_factor = (max/mean - 1) * 0.5` (gives half weight to how much max exceeds mean)
- `variability_factor = std/mean` (gives full weight to relative standard deviation)

This score provides a single measurement that considers:
- Base vibration level (mean)
- Peak intensity (max)
- Variability/instability (std)

Higher scores indicate potentially more problematic vibration conditions.

## Customization

You can modify the scripts to:
- Change the analysis parameters
- Add more metrics
- Customize the visualization
- Process multiple files at once 