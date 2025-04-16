import os
import pandas as pd
from datetime import datetime
from vibration_analyzer import VibrationAnalyzer
from database_manager import DatabaseManager
import sqlite3
import logging

class BatchAnalysisRunner:
    def __init__(self, data_dir="VibData", debug=False):
        """
        Initialize the BatchAnalysisRunner
        
        Parameters:
        - data_dir: Directory containing vibration data files
        - debug: Whether to enable detailed logging
        """
        self.data_dir = data_dir
        self.db = DatabaseManager()
        self.debug = debug
        
        # Set up logging
        self._setup_logging()
    
    def _setup_logging(self):
        """
        Set up logging configuration for the batch analysis run
        """
        # Create logs directory if it doesn't exist
        logs_dir = os.path.join(os.path.dirname(os.path.dirname(self.data_dir)), "Logs")
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        # Create a unique log file for this batch run
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(logs_dir, f"batch_analysis_{timestamp}.log")
        
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG if self.debug else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("BatchAnalysis")
        
        # Log initialization
        self.logger.info("Initializing Batch Analysis")
        self.logger.info(f"Data directory: {self.data_dir}")
        self.logger.info(f"Debug mode: {'Enabled' if self.debug else 'Disabled'}")
    
    def list_data_files(self):
        """
        List all txt files in the data directory
        """
        files = [f for f in os.listdir(self.data_dir) if f.endswith('.txt')]
        self.logger.info(f"Found {len(files)} data files")
        return files
    
    def get_file_timestamps(self, file_path):
        """
        Get the first and last timestamps from a data file
        
        Parameters:
        - file_path: Path to the data file
        
        Returns:
        - Tuple of (first_timestamp, last_timestamp) or None if file is empty
        """
        try:
            # Read the file with pandas, keeping the header row
            df = pd.read_csv(file_path, delimiter='\t')
            
            if df.empty:
                raise ValueError("File is empty or has no data")
            
            # Get first and last timestamps
            first_time = pd.to_datetime(df.iloc[0]['time'])
            last_time = pd.to_datetime(df.iloc[-1]['time'])
            
            self.logger.debug(f"File {os.path.basename(file_path)} time range: {first_time} to {last_time}")
            return first_time, last_time
        except Exception as e:
            self.logger.error(f"Error reading timestamps from {file_path}: {e}")
            return None
    
    def is_file_analyzed(self, file_name, first_time, last_time):
        """
        Check if a file has already been analyzed by checking its timestamps in the database
        
        Parameters:
        - file_name: Name of the data file
        - first_time: First timestamp in the file
        - last_time: Last timestamp in the file
        
        Returns:
        - True if file is already analyzed, False otherwise
        """
        self.logger.debug(f"\nChecking if file {file_name} is already analyzed")
        self.logger.debug(f"Input timestamps - First: {first_time}, Last: {last_time}")
        
        with sqlite3.connect(self.db.db_path) as conn:
            cursor = conn.cursor()
            
            # Get the first and last epoch seconds for this file
            self.logger.debug("Querying database for existing timestamps")
            cursor.execute('''
                SELECT MIN(epoch_seconds), MAX(epoch_seconds)
                FROM raw_data
                WHERE file_name = ?
            ''', (file_name,))
            
            result = cursor.fetchone()
            self.logger.debug(f"Database query result: {result}")
            
            if result is None or result[0] is None:
                self.logger.debug("No existing data found in database for this file")
                return False
            
            db_first_epoch = result[0]
            db_last_epoch = result[1]
            self.logger.debug(f"Database timestamps - First: {datetime.fromtimestamp(db_first_epoch)}, Last: {datetime.fromtimestamp(db_last_epoch)}")
            
            # Convert input timestamps to epoch seconds
            first_epoch = int(first_time.timestamp())
            last_epoch = int(last_time.timestamp())
            self.logger.debug(f"Converted input timestamps to epoch seconds - First: {first_epoch}, Last: {last_epoch}")
            
            # Check if the timestamps match (within 1 second tolerance)
            first_diff = abs(db_first_epoch - first_epoch)
            last_diff = abs(db_last_epoch - last_epoch)
            self.logger.debug(f"Time differences - First: {first_diff} seconds, Last: {last_diff} seconds")
            
            is_analyzed = (first_diff <= 1 and last_diff <= 1)
            
            if is_analyzed:
                self.logger.debug(f"File {file_name} already analyzed (timestamps match within 1 second tolerance)")
            else:
                self.logger.debug(f"File {file_name} needs analysis (timestamps do not match)")
                if first_diff > 1:
                    self.logger.debug(f"First timestamp mismatch: {first_diff} seconds difference")
                if last_diff > 1:
                    self.logger.debug(f"Last timestamp mismatch: {last_diff} seconds difference")
            
            return is_analyzed
    
    def analyze_file(self, file_path):
        """
        Analyze a single data file and save results to database
        
        Parameters:
        - file_path: Path to the data file
        
        Returns:
        - True if analysis was successful, False otherwise
        """
        try:
            self.logger.info(f"\nAnalyzing: {os.path.basename(file_path)}")
            
            # Create analyzer instance and run analysis
            analyzer = VibrationAnalyzer(file_path, debug=self.debug, logger=self.logger)
            analyzer.read_data()
            analyzer.analyze_data_by_second()
            
            # Get the complete file name with extension
            file_name = os.path.basename(file_path)  # This includes the .txt extension
            
            # Save each data point and its corresponding analysis result
            for _, row in analyzer.data.iterrows():
                try:
                    # Format timestamp as string
                    timestamp_str = row['time'].strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Save raw data point
                    data_point = {
                        'timestamp': timestamp_str,
                        'file_name': file_name,  # Using complete file name with extension
                        'speed_x': float(row['SpeedX(mm/s)']),
                        'speed_y': float(row['SpeedY(mm/s)']),
                        'speed_z': float(row['SpeedZ(mm/s)']),
                        'displacement_x': float(row['DisplacementX(um)']),
                        'displacement_y': float(row['DisplacementY(um)']),
                        'displacement_z': float(row['DisplacementZ(um)']),
                        'temperature': float(row['Temperature(°C)']) if pd.notna(row['Temperature(°C)']) else None
                    }
                    epoch_seconds = self.db.save_data_point(data_point)
                    
                    # Find corresponding analysis result
                    analysis_row = analyzer.grouped_data[analyzer.grouped_data['second'] == row['time'].floor('s')].iloc[0]
                    
                    # Save analysis result
                    result = {
                        'epoch_seconds': int(epoch_seconds),
                        'file_name': file_name,  # Using complete file name with extension
                        'velocity_score': float(analysis_row['velocity_score']),
                        'mean_displacement': float(analysis_row['mean_displacement']),
                        'severity_score': float(analysis_row['vibration_severity_score'])
                    }
                    self.db.save_analysis_result(result)
                    
                except Exception as e:
                    self.logger.error(f"Error processing data point at {row['time']}: {e}")
                    continue
            
            self.logger.info(f"Analysis complete. Data saved to database.")
            return True
            
        except Exception as e:
            self.logger.error(f"Error analyzing {file_path}: {e}")
            return False
    
    def run_batch_analysis(self):
        """
        Run batch analysis on all data files
        """
        # Get available data files
        data_files = self.list_data_files()
        
        if not data_files:
            self.logger.warning("No data files found in the VibData directory")
            return
        
        self.logger.info(f"Found {len(data_files)} data files")
        
        # Process each file
        processed = 0
        skipped = 0
        failed = 0
        
        for file_name in data_files:
            file_path = os.path.join(self.data_dir, file_name)
            
            # Get file timestamps
            timestamps = self.get_file_timestamps(file_path)
            if timestamps is None:
                self.logger.warning(f"Skipping {file_name}: Could not read timestamps")
                failed += 1
                continue
            
            first_time, last_time = timestamps
            
            # Check if file is already analyzed
            if self.is_file_analyzed(file_name, first_time, last_time):
                self.logger.info(f"Skipping {file_name}: Already analyzed")
                skipped += 1
                continue
            
            # Analyze file
            if self.analyze_file(file_path):
                processed += 1
            else:
                failed += 1
        
        # Print summary
        self.logger.info("\nBatch Analysis Summary:")
        self.logger.info(f"Total files: {len(data_files)}")
        self.logger.info(f"Processed: {processed}")
        self.logger.info(f"Skipped: {skipped}")
        self.logger.info(f"Failed: {failed}")

def main():
    # Create runner instance and execute batch analysis
    runner = BatchAnalysisRunner(debug=True)
    runner.run_batch_analysis()

if __name__ == "__main__":
    main() 