import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
import os
import math
import logging
import json

class VibrationAnalyzer:
    def __init__(self, data_file_path, debug=False, logger=None):
        """
        Initialize the VibrationAnalyzer with a data file path
        
        Parameters:
        - data_file_path: Path to the vibration data file
        - debug: Whether to enable detailed logging
        - logger: Optional existing logger to use
        """
        self.data_file_path = data_file_path
        self.data = None
        self.grouped_data = None
        self.file_name = os.path.basename(data_file_path).split('.')[0]
        self.debug = debug
        
        # Set up logging
        self.logger = logger if logger is not None else self._setup_logging()
        
        # Log initialization
        self.logger.info(f"Initializing VibrationAnalyzer for file: {self.data_file_path}")
        self.logger.info(f"Debug mode: {'Enabled' if self.debug else 'Disabled'}")
        
    def _setup_logging(self):
        """
        Set up logging configuration if no logger is provided
        """
        # Create logs directory if it doesn't exist
        logs_dir = os.path.join(os.path.dirname(os.path.dirname(self.data_file_path)), "Logs")
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        # Create a unique log file for this analysis
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(logs_dir, f"vibration_analysis_{self.file_name}_{timestamp}.log")
        
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG if self.debug else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        return logging.getLogger(f"VibrationAnalyzer_{self.file_name}")
    
    def read_data(self):
        """
        Read the vibration data file and return a DataFrame
        """
        self.logger.info("Starting data reading process")
        
        try:
            # Read the file with pandas, handling potential formatting issues
            self.logger.debug("Reading CSV file with pandas")
            self.data = pd.read_csv(self.data_file_path, delimiter='\t', skipinitialspace=True)
            self.logger.debug(f"Initial data shape: {self.data.shape}")
            self.logger.debug(f"Columns: {list(self.data.columns)}")
            
            # Clean up column names by removing extra spaces
            self.logger.debug("Cleaning up column names")
            self.data.columns = self.data.columns.str.strip()
            
            # Clean up any extra spaces in the data
            self.logger.debug("Cleaning up data spaces")
            for col in self.data.columns:
                if self.data[col].dtype == 'object':
                    self.data[col] = self.data[col].str.strip()
            
            # Convert time column to datetime
            self.logger.debug("Converting time column to datetime")
            self.data['time'] = pd.to_datetime(self.data['time'])
            
            # Ensure numeric columns are numeric
            self.logger.debug("Converting numeric columns")
            numeric_columns = ['SpeedX(mm/s)', 'SpeedY(mm/s)', 'SpeedZ(mm/s)', 
                             'DisplacementX(um)', 'DisplacementY(um)', 'DisplacementZ(um)',
                             'Temperature(°C)']
            for col in numeric_columns:
                self.data[col] = pd.to_numeric(self.data[col], errors='coerce')
                if self.debug:
                    self.logger.debug(f"Column {col} statistics:")
                    self.logger.debug(f"  NaN count: {self.data[col].isna().sum()}")
                    self.logger.debug(f"  Mean: {self.data[col].mean()}")
                    self.logger.debug(f"  Std: {self.data[col].std()}")
            
            self.logger.info("Data reading completed successfully")
            return self.data
            
        except Exception as e:
            self.logger.error(f"Error reading data: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    def calculate_vibration_level(speed_x, speed_y, speed_z):
        """
        Calculate the overall vibration level from the three components
        
        RMS = sqrt(vx² + vy² + vz²)
        """
        return np.sqrt(speed_x**2 + speed_y**2 + speed_z**2)
    
    @staticmethod
    def calculate_severity_score(mean, max_val, std, disp_x_mean, disp_y_mean, disp_z_mean):
        """
        Calculate a vibration severity score that combines velocity and displacement metrics
        
        This creates a single metric that considers:
        - Base vibration level (mean)
        - Peak intensity (max)
        - Variability/instability (std)
        - Displacement in all three axes (X, Y, Z)
        
        Formula: velocity_score * (1 + displacement_factor)
        
        Where:
        - velocity_score = mean * (1 + peak_factor) * (1 + variability_factor)
        - peak_factor = (max/mean - 1) * 0.5 (half weight to how much max exceeds mean)
        - variability_factor = std/mean (full weight to relative standard deviation)
        - displacement_factor = (disp_x + disp_y + disp_z) / (3 * mean) * 0.5 (half weight to displacement)
        
        Higher scores indicate more severe vibration conditions.
        """
        # Handle cases where mean is 0 to prevent division by zero
        if mean == 0:
            if max_val == 0 and std == 0:
                return 0
            else:
                return max_val + std
        
        # Calculate velocity factors
        peak_factor = (max_val / mean - 1) * 0.5 if max_val > mean else 0
        variability_factor = std / mean
        
        # Calculate velocity score
        velocity_score = mean * (1 + peak_factor) * (1 + variability_factor)
        
        # Calculate displacement factor
        # Using mean displacement across all axes, normalized by velocity mean
        # and weighted by 0.5 to give it less influence than velocity
        mean_displacement = (disp_x_mean + disp_y_mean + disp_z_mean) / 3
        displacement_factor = (mean_displacement / mean) * 0.5
        
        # Calculate final severity score
        severity = velocity_score * (1 + displacement_factor)
        
        return severity
    
    def analyze_data_by_second(self):
        """
        Group data by second and calculate vibration metrics
        """
        self.logger.info("Starting data analysis")
        
        try:
            if self.data is None:
                self.read_data()
            
            # Add a second column for grouping
            self.logger.debug("Adding second column for grouping")
            self.data['second'] = self.data['time'].dt.floor('s')
            
            # Calculate the vibration level for each sample
            self.logger.debug("Calculating vibration levels")
            self.data['vibration_level'] = self.calculate_vibration_level(
                self.data['SpeedX(mm/s)'], 
                self.data['SpeedY(mm/s)'], 
                self.data['SpeedZ(mm/s)']
            )
            
            if self.debug:
                self.logger.debug("Vibration level statistics:")
                self.logger.debug(f"  Mean: {self.data['vibration_level'].mean()}")
                self.logger.debug(f"  Max: {self.data['vibration_level'].max()}")
                self.logger.debug(f"  Std: {self.data['vibration_level'].std()}")
            
            # Group by second and calculate metrics
            self.logger.debug("Grouping data by second")
            grouped = self.data.groupby('second').agg({
                'vibration_level': ['mean', 'max', 'std'],
                'DisplacementX(um)': ['mean', 'max'],
                'DisplacementY(um)': ['mean', 'max'],
                'DisplacementZ(um)': ['mean', 'max'],
                'Temperature(°C)': 'mean'
            })
            
            # Flatten the column names
            self.logger.debug("Flattening column names")
            grouped.columns = ['_'.join(col).strip() for col in grouped.columns.values]
            grouped = grouped.reset_index()
            
            # Handle cases where there's only one data point in a second
            # For these cases, std will be NaN, so we'll set it to a small value
            # relative to the mean to avoid division by zero
            grouped['vibration_level_std'] = grouped.apply(
                lambda row: row['vibration_level_std'] if pd.notna(row['vibration_level_std']) 
                          else row['vibration_level_mean'] * 0.01,  # Use 1% of mean as std
                axis=1
            )
            
            if self.debug:
                self.logger.debug("Grouped data statistics:")
                self.logger.debug(f"  Number of seconds: {len(grouped)}")
                self.logger.debug(f"  Columns: {list(grouped.columns)}")
                self.logger.debug("Checking for single-point seconds:")
                single_point_seconds = grouped[grouped['vibration_level_std'] == grouped['vibration_level_mean'] * 0.01]
                if not single_point_seconds.empty:
                    self.logger.debug(f"  Found {len(single_point_seconds)} seconds with single data point")
                    self.logger.debug("  First few single-point seconds:")
                    self.logger.debug(single_point_seconds[['second', 'vibration_level_mean', 'vibration_level_std']].head())
            
            # Calculate velocity score for each second
            self.logger.debug("Calculating velocity scores")
            grouped['velocity_score'] = grouped.apply(
                lambda row: self.calculate_velocity_score(
                    row['vibration_level_mean'],
                    row['vibration_level_max'],
                    row['vibration_level_std']
                ), 
                axis=1
            )
            
            # Calculate mean displacement for each second
            self.logger.debug("Calculating mean displacements")
            grouped['mean_displacement'] = grouped.apply(
                lambda row: (row['DisplacementX(um)_mean'] + 
                           row['DisplacementY(um)_mean'] + 
                           row['DisplacementZ(um)_mean']) / 3,
                axis=1
            )
            
            # Calculate severity score for each second
            self.logger.debug("Calculating severity scores")
            grouped['vibration_severity_score'] = grouped.apply(
                lambda row: self.calculate_severity_score(
                    row['vibration_level_mean'],
                    row['vibration_level_max'],
                    row['vibration_level_std'],
                    row['DisplacementX(um)_mean'],
                    row['DisplacementY(um)_mean'],
                    row['DisplacementZ(um)_mean']
                ), 
                axis=1
            )
            
            self.grouped_data = grouped
            self.logger.info("Data analysis completed successfully")
            return grouped
            
        except Exception as e:
            self.logger.error(f"Error in data analysis: {str(e)}", exc_info=True)
            raise
    
    @staticmethod
    def calculate_velocity_score(mean, max_val, std):
        """
        Calculate the velocity score component of the severity score
        
        Formula: mean * (1 + peak_factor) * (1 + variability_factor)
        Where:
        - peak_factor = (max/mean - 1) * 0.5 (half weight to how much max exceeds mean)
        - variability_factor = std/mean (full weight to relative standard deviation)
        """
        # Handle cases where mean is 0 or NaN
        if pd.isna(mean) or mean == 0:
            if pd.isna(max_val) or pd.isna(std) or (max_val == 0 and std == 0):
                return 0.0
            else:
                return float(max_val + std)
        
        # Ensure all values are numeric
        mean = float(mean)
        max_val = float(max_val)
        std = float(std)
        
        # Calculate velocity factors
        peak_factor = (max_val / mean - 1) * 0.5 if max_val > mean else 0.0
        variability_factor = std / mean
        
        # Calculate velocity score
        velocity_score = mean * (1 + peak_factor) * (1 + variability_factor)
        
        # Ensure we return a float
        return float(velocity_score)
    
    @staticmethod
    def get_results_dir():
        """
        Get the path to the Results directory under the root project folder
        """
        script_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.dirname(script_dir)
        results_dir = os.path.join(root_dir, "Results")
        
        if not os.path.exists(results_dir):
            os.makedirs(results_dir)
            
        return results_dir
    
    def plot_vibration_levels(self, output_path=None):
        """
        Create a plot of vibration levels over time
        
        Parameters:
        - output_path: Custom path where to save the plot (if None, uses default path)
        """
        if self.grouped_data is None:
            self.analyze_data_by_second()
            
        plt.figure(figsize=(12, 8))
        
        # Plot mean vibration level
        plt.plot(self.grouped_data['second'], self.grouped_data['vibration_level_mean'], 
                 label='Mean Vibration Level', marker='o', linewidth=2)
        
        # Plot max vibration level
        plt.plot(self.grouped_data['second'], self.grouped_data['vibration_level_max'], 
                 label='Max Vibration Level', marker='x', linestyle='--', linewidth=1.5)
        
        # Plot severity score
        plt.plot(self.grouped_data['second'], self.grouped_data['vibration_severity_score'], 
                 label='Severity Score', marker='s', linestyle='-.', linewidth=2, color='red')
        
        # Add standard deviation as shaded area
        plt.fill_between(
            self.grouped_data['second'],
            self.grouped_data['vibration_level_mean'] - self.grouped_data['vibration_level_std'],
            self.grouped_data['vibration_level_mean'] + self.grouped_data['vibration_level_std'],
            alpha=0.2, label='Standard Deviation'
        )
        
        # Add labels and title
        plt.xlabel('Time')
        plt.ylabel('Vibration Level (mm/s)')
        plt.title(f'Vibration Level Analysis - {self.file_name}')
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        # Format x-axis
        plt.gcf().autofmt_xdate()
        
        # Save the figure
        if output_path is None:
            results_dir = self.get_results_dir()
            output_path = os.path.join(results_dir, f"vibration_analysis_{self.file_name}.png")
        
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        
        # Display the figure
        plt.tight_layout()
        plt.show()
    
    def plot_displacement_data(self, output_path=None):
        """
        Create a plot of displacement data over time
        
        Parameters:
        - output_path: Custom path where to save the plot (if None, uses default path)
        """
        if self.grouped_data is None:
            self.analyze_data_by_second()
            
        plt.figure(figsize=(12, 8))
        
        # Plot mean displacement for each axis
        plt.plot(self.grouped_data['second'], self.grouped_data['DisplacementX(um)_mean'], 
                 label='X-axis', marker='o', linewidth=2)
        plt.plot(self.grouped_data['second'], self.grouped_data['DisplacementY(um)_mean'], 
                 label='Y-axis', marker='s', linewidth=2)
        plt.plot(self.grouped_data['second'], self.grouped_data['DisplacementZ(um)_mean'], 
                 label='Z-axis', marker='^', linewidth=2)
        
        # Add labels and title
        plt.xlabel('Time')
        plt.ylabel('Displacement (µm)')
        plt.title(f'Mean Displacement Analysis - {self.file_name}')
        plt.grid(True, alpha=0.3)
        plt.legend()
        
        # Format x-axis
        plt.gcf().autofmt_xdate()
        
        # Save the figure
        if output_path is None:
            results_dir = self.get_results_dir()
            output_path = os.path.join(results_dir, f"displacement_analysis_{self.file_name}.png")
        
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        
        # Display the figure
        plt.tight_layout()
        plt.show()
    
    def plot_severity_score(self, output_path=None):
        """
        Create a dedicated plot of the vibration severity score over time
        
        Parameters:
        - output_path: Custom path where to save the plot (if None, uses default path)
        """
        if self.grouped_data is None:
            self.analyze_data_by_second()
            
        plt.figure(figsize=(12, 8))
        
        # Plot severity score with a color gradient based on value
        sc = plt.scatter(self.grouped_data['second'], self.grouped_data['vibration_severity_score'], 
                        c=self.grouped_data['vibration_severity_score'], cmap='viridis', 
                        s=80, alpha=0.7)
        
        # Connect points with a line
        plt.plot(self.grouped_data['second'], self.grouped_data['vibration_severity_score'], 
                 linestyle='-', linewidth=1.5, alpha=0.6, color='black')
        
        # Add color bar
        cbar = plt.colorbar(sc)
        cbar.set_label('Severity Score')
        
        # Add labels and title
        plt.xlabel('Time')
        plt.ylabel('Vibration Severity Score')
        plt.title(f'Vibration Severity Analysis - {self.file_name}')
        plt.grid(True, alpha=0.3)
        
        # Format x-axis
        plt.gcf().autofmt_xdate()
        
        # Save the figure
        if output_path is None:
            results_dir = self.get_results_dir()
            output_path = os.path.join(results_dir, f"severity_analysis_{self.file_name}.png")
        
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        
        # Display the figure
        plt.tight_layout()
        plt.show()
    
    def save_results(self, output_dir=None):
        """
        Save all analysis results to files
        
        Parameters:
        - output_dir: Directory to save results (if None, uses default Results directory)
        """
        if self.grouped_data is None:
            self.analyze_data_by_second()
            
        if output_dir is None:
            output_dir = self.get_results_dir()
            
        # Generate all plots
        self.plot_vibration_levels(os.path.join(output_dir, f"vibration_analysis_{self.file_name}.png"))
        self.plot_displacement_data(os.path.join(output_dir, f"displacement_analysis_{self.file_name}.png"))
        self.plot_severity_score(os.path.join(output_dir, f"severity_analysis_{self.file_name}.png"))
        
        # Save the processed data
        csv_path = os.path.join(output_dir, f"vibration_analysis_{self.file_name}.csv")
        self.grouped_data.to_csv(csv_path, index=False)
        
        return output_dir

def main():
    # Example usage of the VibrationAnalyzer class
    data_folder = "VibData"
    file_name = "20250323152752.txt"  # Using the smallest file for this example
    file_path = os.path.join(data_folder, file_name)
    
    print(f"Analyzing vibration data from: {file_path}")
    
    # Create analyzer instance with debug mode enabled
    analyzer = VibrationAnalyzer(file_path, debug=True)
    
    try:
        # Read and analyze data
        analyzer.read_data()
        analyzer.analyze_data_by_second()
        
        # Display first few rows of processed data
        print("\nProcessed data (first 5 rows):")
        print(analyzer.grouped_data.head(5))
        
        # Save all results
        output_dir = analyzer.save_results()
        print(f"\nResults saved to: {output_dir}")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        raise

if __name__ == "__main__":
    main() 