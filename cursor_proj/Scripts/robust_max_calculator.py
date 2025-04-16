import pandas as pd
import numpy as np
from database_manager import DatabaseManager
import logging
import os
from datetime import datetime
from typing import Optional, Tuple, Dict
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.io as pio
import sqlite3

class RobustMaxCalculator:
    def __init__(self, debug: bool = False):
        """
        Initialize the RobustMaxCalculator
        
        Parameters:
        - debug: Whether to enable detailed logging
        """
        self.debug = debug
        self.db = DatabaseManager()
        self.data: Optional[pd.DataFrame] = None
        self.cleaned_data: Optional[pd.DataFrame] = None
        self.statistics: Dict = {}
        self._setup_logging()
    
    def _setup_logging(self):
        """Set up logging configuration"""
        # Create logs directory if it doesn't exist
        logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Logs")
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        # Create a unique log file for this session
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(logs_dir, f"robust_max_calculation_{timestamp}.log")
        
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG if self.debug else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("RobustMaxCalculator")
    
    def load_data(self) -> None:
        """
        Load data from the database and perform initial analysis
        
        This method:
        1. Loads all analysis results from the database
        2. Calculates basic statistics
        3. Identifies the data distribution
        """
        self.logger.info("Loading data from database")
        
        # Get all analysis results
        with sqlite3.connect(self.db.db_path) as conn:
            query = '''
                SELECT ar.*, rd.timestamp
                FROM analysis_results ar
                JOIN raw_data rd ON ar.epoch_seconds = rd.epoch_seconds
                ORDER BY ar.epoch_seconds
            '''
            self.data = pd.read_sql_query(query, conn)
            
            # Convert timestamp to datetime
            self.data['timestamp'] = pd.to_datetime(self.data['timestamp'])
        
        # Calculate basic statistics
        self.statistics = {
            'total_points': len(self.data),
            'sampling_rate': self._calculate_sampling_rate(),
            'distribution': self._analyze_distribution(),
            'raw_max': self.data['severity_score'].max(),
            'raw_min': self.data['severity_score'].min(),
            'raw_mean': self.data['severity_score'].mean(),
            'raw_std': self.data['severity_score'].std()
        }
        
        self.logger.info(f"Loaded {self.statistics['total_points']} data points")
        self.logger.info(f"Sampling rate: {self.statistics['sampling_rate']:.2f} Hz")
        self.logger.info(f"Raw maximum: {self.statistics['raw_max']:.2f}")
        self.logger.info(f"Raw minimum: {self.statistics['raw_min']:.2f}")
        self.logger.info(f"Raw mean: {self.statistics['raw_mean']:.2f}")
        self.logger.info(f"Raw std: {self.statistics['raw_std']:.2f}")
    
    def _calculate_sampling_rate(self) -> float:
        """Calculate the average sampling rate in Hz"""
        if len(self.data) < 2:
            return 0.0
        
        time_diffs = self.data['timestamp'].diff().dt.total_seconds()
        return 1.0 / time_diffs.mean()
    
    def _analyze_distribution(self) -> Dict:
        """Analyze the distribution of severity scores"""
        scores = self.data['severity_score']
        
        # Calculate skewness and kurtosis
        skewness = scores.skew()
        kurtosis = scores.kurtosis()
        
        # Determine distribution type
        if abs(skewness) < 0.5 and abs(kurtosis) < 1:
            dist_type = "normal"
        elif skewness > 0:
            dist_type = "right_skewed"
        else:
            dist_type = "left_skewed"
        
        return {
            'type': dist_type,
            'skewness': skewness,
            'kurtosis': kurtosis
        }
    
    def detect_outliers(self, method: str = "IQR") -> pd.DataFrame:
        """
        Detect and handle outliers using specified method
        
        Parameters:
        - method: "IQR" or "Z-Score"
        
        Returns:
        - DataFrame with outliers marked
        """
        self.logger.info(f"Detecting outliers using {method} method")
        
        if self.data is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        
        scores = self.data['severity_score']
        
        if method == "IQR":
            # Calculate IQR-based bounds
            Q1 = scores.quantile(0.25)
            Q3 = scores.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            
            # Mark outliers
            self.data['is_outlier'] = (scores < lower_bound) | (scores > upper_bound)
            
            self.logger.info(f"IQR bounds: [{lower_bound:.2f}, {upper_bound:.2f}]")
            self.logger.info(f"Found {self.data['is_outlier'].sum()} outliers")
            
        elif method == "Z-Score":
            # Calculate Z-score based bounds
            z_scores = np.abs((scores - scores.mean()) / scores.std())
            threshold = 3  # Standard threshold for Z-score method
            
            # Mark outliers
            self.data['is_outlier'] = z_scores > threshold
            
            self.logger.info(f"Z-score threshold: {threshold}")
            self.logger.info(f"Found {self.data['is_outlier'].sum()} outliers")
        
        else:
            raise ValueError(f"Unknown method: {method}")
        
        # Store cleaned data
        self.cleaned_data = self.data[~self.data['is_outlier']].copy()
        
        return self.data
    
    def calculate_baseline(self, percentile: float = 99) -> float:
        """
        Calculate a robust baseline using percentile method
        
        Parameters:
        - percentile: The percentile to use (default: 99)
        
        Returns:
        - float: The calculated baseline
        """
        if self.cleaned_data is None:
            raise ValueError("No cleaned data available. Call detect_outliers() first.")
        
        baseline = self.cleaned_data['severity_score'].quantile(percentile / 100)
        
        self.logger.info(f"Calculated baseline (P{percentile}): {baseline:.2f}")
        return baseline
    
    def get_scientific_max(self, method: str = "percentile", percentile: float = 99) -> float:
        """
        Get the scientific maximum value using specified method
        
        Parameters:
        - method: "percentile" or "iqr"
        - percentile: The percentile to use (default: 99)
        
        Returns:
        - float: The scientific maximum value
        """
        if self.cleaned_data is None:
            raise ValueError("No cleaned data available. Call detect_outliers() first.")
        
        if method == "percentile":
            scientific_max = self.calculate_baseline(percentile)
        elif method == "iqr":
            Q1 = self.cleaned_data['severity_score'].quantile(0.25)
            Q3 = self.cleaned_data['severity_score'].quantile(0.75)
            IQR = Q3 - Q1
            scientific_max = Q3 + 1.5 * IQR
        else:
            raise ValueError(f"Unknown method: {method}")
        
        self.logger.info(f"Scientific maximum ({method}): {scientific_max:.2f}")
        return scientific_max
    
    def get_percentage_scores(self, scientific_max: float) -> pd.DataFrame:
        """
        Calculate percentage scores based on scientific maximum
        
        Parameters:
        - scientific_max: The scientific maximum value to use
        
        Returns:
        - DataFrame with percentage scores
        """
        if self.data is None:
            raise ValueError("Data not loaded. Call load_data() first.")
        
        # Calculate percentage scores
        self.data['percentage_score'] = (self.data['severity_score'] / scientific_max) * 100
        
        # Cap percentage scores at 100%
        self.data['percentage_score'] = self.data['percentage_score'].clip(0, 100)
        
        self.logger.info("Calculated percentage scores")
        return self.data

    def plot_percentage_scores(self, results: Dict, output_dir: Optional[str] = None) -> None:
        """
        Create interactive plots of percentage scores for all methods, separated by day
        
        Parameters:
        - results: Dictionary containing results from all methods
        - output_dir: Directory to save plots (if None, uses default Results directory)
        """
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Results")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
        
        # Get all unique dates from any method's data
        dates = pd.Series([], dtype='datetime64[ns]')
        for result in results.values():
            dates = pd.concat([dates, result['data']['timestamp'].dt.date])
        dates = dates.unique()
        
        # Define colors and styles for different methods
        method_styles = {
            'IQR_percentile': ('blue', 'IQR + P99'),
            'IQR_iqr': ('green', 'IQR + IQR'),
            'Z-Score_percentile': ('red', 'Z-Score + P99'),
            'Z-Score_iqr': ('orange', 'Z-Score + IQR')
        }
        
        # Create plots for each day
        for date in dates:
            self.logger.info(f"Creating interactive plot for date: {date}")
            
            # Create figure with secondary y-axis
            fig = go.Figure()
            
            # Plot data for each method
            for method_key, style in method_styles.items():
                color, label = style
                day_data = results[method_key]['data']
                day_data = day_data[day_data['timestamp'].dt.date == date]
                
                # Convert timestamps to seconds since midnight for x-axis
                seconds = (day_data['timestamp'] - pd.Timestamp(date)).dt.total_seconds()
                
                # Add trace for percentage scores
                fig.add_trace(go.Scatter(
                    x=seconds,
                    y=day_data['percentage_score'],
                    name=label,
                    line=dict(color=color),
                    mode='lines+markers',
                    hovertemplate="Time: %{x:.0f}s<br>Score: %{y:.1f}%<extra></extra>"
                ))
            
            # Add horizontal lines at 50% and 75%
            fig.add_hline(y=50, line_dash="dash", line_color="gray", opacity=0.3)
            fig.add_hline(y=75, line_dash="dash", line_color="gray", opacity=0.3)
            
            # Update layout
            fig.update_layout(
                title=f'Vibration Severity Percentage - {date}',
                xaxis_title='Time (seconds since midnight)',
                yaxis_title='Percentage Score (%)',
                hovermode='x unified',
                showlegend=True,
                legend=dict(
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01
                ),
                height=800,
                template='plotly_white'
            )
            
            # Add grid
            fig.update_xaxes(showgrid=True, gridwidth=1, gridcolor='LightGray')
            fig.update_yaxes(showgrid=True, gridwidth=1, gridcolor='LightGray')
            
            # Save as interactive HTML file
            output_file = os.path.join(output_dir, f'vibration_percentage_{date}.html')
            pio.write_html(fig, file=output_file, auto_open=True)
            
            self.logger.info(f"Saved interactive plot to: {output_file}")
            
            # Also save as static PNG for reference
            png_file = os.path.join(output_dir, f'vibration_percentage_{date}.png')
            pio.write_image(fig, png_file, scale=2)
            self.logger.info(f"Saved static plot to: {png_file}")

    def run_comparison_tests(self) -> Dict:
        """
        Run comparison tests between different methods
        
        Returns:
        - Dictionary containing test results and statistics
        """
        self.logger.info("Starting comparison tests")
        
        # Load data if not already loaded
        if self.data is None:
            self.load_data()
        
        results = {}
        
        # Test both outlier detection methods
        for outlier_method in ["IQR", "Z-Score"]:
            self.logger.info(f"\nTesting with {outlier_method} outlier detection")
            
            # Detect outliers
            self.detect_outliers(method=outlier_method)
            
            # Test both scientific max methods
            for max_method in ["percentile", "iqr"]:
                self.logger.info(f"Testing {max_method} method for scientific maximum")
                
                # Get scientific maximum
                scientific_max = self.get_scientific_max(method=max_method)
                
                # Calculate percentage scores
                results_df = self.get_percentage_scores(scientific_max)
                
                # Store results
                method_key = f"{outlier_method}_{max_method}"
                results[method_key] = {
                    'scientific_max': scientific_max,
                    'outliers_removed': self.data['is_outlier'].sum(),
                    'percentage_stats': results_df['percentage_score'].describe().to_dict(),
                    'data': results_df
                }
        
        return results
        
    def get_robust_max(self, outlier_method: str = "IQR", max_method: str = "percentile", 
                      percentile: float = 99) -> Tuple[float, pd.DataFrame]:
        """
        Get the robust maximum value in a single method call
        
        This method encapsulates the entire workflow:
        1. Load data from the database
        2. Detect and remove outliers
        3. Calculate the scientific maximum
        4. Calculate percentage scores
        
        Parameters:
        - outlier_method: Method to detect outliers ("IQR" or "Z-Score")
        - max_method: Method to calculate scientific maximum ("percentile" or "iqr")
        - percentile: The percentile to use (default: 99)
        
        Returns:
        - Tuple containing:
          - float: The calculated robust maximum value
          - DataFrame: The data with percentage scores
        """
        self.logger.info(f"Getting robust max using {outlier_method} for outliers and {max_method} for max calculation")
        
        # Load data if not already loaded
        if self.data is None:
            self.load_data()
        
        # Detect outliers
        self.detect_outliers(method=outlier_method)
        
        # Get scientific maximum
        scientific_max = self.get_scientific_max(method=max_method, percentile=percentile)
        
        # Calculate percentage scores
        results_df = self.get_percentage_scores(scientific_max)
        
        self.logger.info(f"Robust max calculation complete. Value: {scientific_max:.2f}")
        return scientific_max, results_df

def main():
    # Create calculator instance with debug mode enabled
    calculator = RobustMaxCalculator(debug=True)
    
    try:
        # Run comparison tests
        results = calculator.run_comparison_tests()
        
        # Print summary of results
        print("\nTest Results Summary:")
        print("=" * 50)
        
        for method_key, result in results.items():
            print(f"\nMethod: {method_key}")
            print(f"Scientific Maximum: {result['scientific_max']:.2f}")
            print(f"Outliers Removed: {result['outliers_removed']}")
            print("\nPercentage Score Statistics:")
            for stat, value in result['percentage_stats'].items():
                print(f"  {stat}: {value:.2f}")
        
        # Create plots comparing all methods
        print("\nCreating comparison plots")
        calculator.plot_percentage_scores(results)
        
        print("\nAll tests and plots completed successfully!")
        
    except Exception as e:
        print(f"Error during testing: {str(e)}")
        raise

if __name__ == "__main__":
    main() 