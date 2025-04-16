import pandas as pd
import matplotlib.pyplot as plt
from database_manager import DatabaseManager
import os
from datetime import datetime
import logging
import sqlite3

class DatabasePlotter:
    def __init__(self, debug=False):
        """
        Initialize the DatabasePlotter
        
        Parameters:
        - debug: Whether to enable detailed logging
        """
        self.debug = debug
        self.db = DatabaseManager()
        self._setup_logging()
        
    def _setup_logging(self):
        """Set up logging configuration"""
        # Create logs directory if it doesn't exist
        logs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Logs")
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        # Create a unique log file for this plotting session
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(logs_dir, f"database_plotting_{timestamp}.log")
        
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG if self.debug else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("DatabasePlotter")
    
    def get_all_analysis_results(self):
        """
        Retrieve all analysis results from the database
        
        Returns:
        - DataFrame containing all analysis results
        """
        self.logger.info("Retrieving all analysis results from database")
        
        with sqlite3.connect(self.db.db_path) as conn:
            # Get all analysis results with timestamps
            query = '''
                SELECT ar.*, rd.timestamp
                FROM analysis_results ar
                JOIN raw_data rd ON ar.epoch_seconds = rd.epoch_seconds
                ORDER BY ar.epoch_seconds
            '''
            results = pd.read_sql_query(query, conn)
            
            # Convert timestamp to datetime
            results['timestamp'] = pd.to_datetime(results['timestamp'])
            
            # Add date column for grouping
            results['date'] = results['timestamp'].dt.date
            
            self.logger.info(f"Retrieved {len(results)} analysis results")
            return results
    
    def plot_daily_results(self, output_dir=None):
        """
        Create separate plots for each day's analysis results
        
        Parameters:
        - output_dir: Directory to save plots (if None, uses default Results directory)
        """
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Results")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
        
        # Get all results
        results = self.get_all_analysis_results()
        
        # Group by date
        for date, day_data in results.groupby('date'):
            self.logger.info(f"Creating plot for date: {date}")
            
            # Create figure with three subplots
            fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(15, 12))
            fig.suptitle(f'Vibration Analysis Results - {date}', fontsize=16)
            
            # Convert timestamps to seconds since midnight for x-axis
            seconds = (day_data['timestamp'] - pd.Timestamp(date)).dt.total_seconds()
            
            # Plot 1: Velocity Score
            ax1.plot(seconds, day_data['velocity_score'], 'b-', label='Velocity Score')
            ax1.set_xlabel('Time (seconds since midnight)')
            ax1.set_ylabel('Velocity Score')
            ax1.set_title('Velocity Score Over Time')
            ax1.grid(True, alpha=0.3)
            ax1.legend()
            
            # Plot 2: Mean Displacement
            ax2.plot(seconds, day_data['mean_displacement'], 'g-', label='Mean Displacement')
            ax2.set_xlabel('Time (seconds since midnight)')
            ax2.set_ylabel('Mean Displacement (µm)')
            ax2.set_title('Mean Displacement Over Time')
            ax2.grid(True, alpha=0.3)
            ax2.legend()
            
            # Plot 3: Severity Score
            ax3.plot(seconds, day_data['severity_score'], 'r-', label='Severity Score')
            ax3.set_xlabel('Time (seconds since midnight)')
            ax3.set_ylabel('Severity Score')
            ax3.set_title('Severity Score Over Time')
            ax3.grid(True, alpha=0.3)
            ax3.legend()
            
            # Adjust layout and save
            plt.tight_layout()
            output_file = os.path.join(output_dir, f'vibration_analysis_{date}.png')
            plt.savefig(output_file, dpi=300, bbox_inches='tight')
            plt.close()
            
            self.logger.info(f"Saved plot to: {output_file}")

def main():
    # Create plotter instance with debug mode enabled
    plotter = DatabasePlotter(debug=True)
    
    try:
        # Create daily plots
        plotter.plot_daily_results()
        print("Plotting completed successfully!")
        
    except Exception as e:
        print(f"Error during plotting: {str(e)}")
        raise

if __name__ == "__main__":
    main() 