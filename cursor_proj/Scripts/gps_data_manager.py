import os
import xml.etree.ElementTree as ET
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from typing import Dict, List, Optional
from database_manager import DatabaseManager
import sqlite3

class GPSDataManager:
    def __init__(self, debug: bool = False):
        """
        Initialize the GPS Data Manager
        
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
        
        # Create a unique log file for this session
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(logs_dir, f"gps_data_processing_{timestamp}.log")
        
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG if self.debug else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("GPSDataManager")
    
    def read_gpx_file(self, file_path: str) -> pd.DataFrame:
        """
        Read a GPX file and extract track points
        
        Parameters:
        - file_path: Path to the GPX file
        
        Returns:
        - DataFrame containing track points
        """
        self.logger.info(f"Reading GPX file: {file_path}")
        
        # Parse GPX file
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        # Define namespace
        ns = {'gpx': 'http://www.topografix.com/GPX/1/1',
              'mytracks': 'http://mytracks.stichling.info/myTracksGPX/1/0'}
        
        # Extract track points
        track_points = []
        for trkpt in root.findall('.//gpx:trkpt', ns):
            # Get basic point data
            point = {
                'latitude': float(trkpt.get('lat')),
                'longitude': float(trkpt.get('lon')),
                'elevation': float(trkpt.find('gpx:ele', ns).text),
                'timestamp': trkpt.find('gpx:time', ns).text
            }
            
            # Get extensions data if available
            extensions = trkpt.find('gpx:extensions', ns)
            if extensions is not None:
                point['speed'] = float(extensions.find('mytracks:speed', ns).text) if extensions.find('mytracks:speed', ns) is not None else None
                point['gradient'] = float(extensions.find('mytracks:gradient', ns).text) if extensions.find('mytracks:gradient', ns) is not None else None
                point['length'] = float(extensions.find('mytracks:length', ns).text) if extensions.find('mytracks:length', ns) is not None else None
            
            track_points.append(point)
        
        # Convert to DataFrame
        df = pd.DataFrame(track_points)
        
        # Convert timestamp to datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        self.logger.info(f"Extracted {len(df)} track points")
        return df
    
    def save_gps_data(self, df: pd.DataFrame, file_name: str) -> None:
        """
        Save GPS data to database
        
        Parameters:
        - df: DataFrame containing GPS data
        - file_name: Name of the GPX file
        """
        self.logger.info(f"Saving GPS data for file: {file_name}")
        
        # Convert timezone from UTC-4 to UTC+8 (add 12 hours)
        df['timestamp'] = df['timestamp'] + pd.Timedelta(hours=8)
        
        for _, row in df.iterrows():
            data_point = {
                'timestamp': row['timestamp'].strftime('%Y-%m-%d %H:%M:%S'),  # Convert to string format
                'file_name': file_name,
                'latitude': row['latitude'],
                'longitude': row['longitude'],
                'elevation': row['elevation'],
                'speed': row.get('speed'),
                'gradient': row.get('gradient'),
                'length': row.get('length')
            }
            
            self.db.save_gps_point(data_point)
        
        self.logger.info("GPS data saved successfully")
    
    def is_file_processed(self, file_name: str) -> bool:
        """
        Check if a file has already been processed and recorded in the database
        
        Parameters:
        - file_name: Name of the GPX file
        
        Returns:
        - True if the file has already been processed, False otherwise
        """
        self.logger.info(f"Checking if file {file_name} has already been processed")
        
        # Query the database to check if the file exists
        with sqlite3.connect(self.db.db_path, timeout=30) as conn:
            # Check if any records exist for this file
            query = '''
                SELECT COUNT(*) 
                FROM gps_data 
                WHERE file_name = ?
            '''
            cursor = conn.cursor()
            cursor.execute(query, (file_name,))
            count = cursor.fetchone()[0]
            
            if count > 0:
                self.logger.info(f"File {file_name} has already been processed ({count} records found)")
                return True
            else:
                self.logger.info(f"File {file_name} has not been processed yet")
                return False
    
    def process_gpx_file(self, file_path: str) -> None:
        """
        Process a GPX file: read it and save to database
        
        Parameters:
        - file_path: Path to the GPX file
        """
        try:
            # Get file name
            file_name = os.path.basename(file_path)
            
            # Check if the file has already been processed
            if self.is_file_processed(file_name):
                self.logger.info(f"Skipping already processed file: {file_name}")
                return
            
            # Read GPX file
            df = self.read_gpx_file(file_path)
            
            # Save to database
            self.save_gps_data(df, file_name)
            
            self.logger.info(f"Successfully processed GPX file: {file_name}")
            
        except Exception as e:
            self.logger.error(f"Error processing GPX file {file_path}: {str(e)}")
            raise
    
    def process_directory(self, directory_path: str) -> None:
        """
        Process all GPX files in a directory
        
        Parameters:
        - directory_path: Path to directory containing GPX files
        """
        self.logger.info(f"Processing GPX files in directory: {directory_path}")
        
        # Get all GPX files
        gpx_files = [f for f in os.listdir(directory_path) if f.endswith('.gpx')]
        
        if not gpx_files:
            self.logger.warning(f"No GPX files found in directory: {directory_path}")
            return
        
        # Process each file
        for file_name in gpx_files:
            file_path = os.path.join(directory_path, file_name)
            self.process_gpx_file(file_path)
        
        self.logger.info(f"Processed {len(gpx_files)} GPX files")
    
    def get_gps_data(self) -> pd.DataFrame:
        """
        Get all GPS data from the database
        
        Returns:
        - DataFrame containing GPS data with epoch_seconds, timestamp, latitude, and longitude
        """
        self.logger.info("Retrieving GPS data from database")
        
        # Get all GPS data
        gps_data = self.db.get_all_gps_data()
        
        # Select only the needed columns
        gps_data = gps_data[['epoch_seconds', 'timestamp', 'latitude', 'longitude']]
        
        # Sort by epoch_seconds
        gps_data = gps_data.sort_values('epoch_seconds')
        
        self.logger.info(f"Retrieved {len(gps_data)} GPS data points")
        return gps_data
    
    def fill_missing_data(self, gps_data: pd.DataFrame) -> pd.DataFrame:
        """
        Fill missing data points with simple averaging
        
        Parameters:
        - gps_data: DataFrame containing GPS data
        
        Returns:
        - DataFrame with continuous data points (one second intervals)
        """
        self.logger.info("Filling missing data points")
        
        # Define the maximum gap threshold (3 minutes = 180 seconds)
        max_gap_threshold = 60
        
        # Sort the data by epoch_seconds
        sorted_data = gps_data.sort_values('epoch_seconds').reset_index(drop=True)
        
        # Initialize result list to store all data points
        result = []
        
        # Process data sequentially
        for i in range(len(sorted_data)):
            # Add the current point to the result
            current_point = sorted_data.iloc[i].to_dict()
            result.append(current_point)
            
            # If this is not the last point, check for gaps
            if i < len(sorted_data) - 1:
                current_epoch = sorted_data.iloc[i]['epoch_seconds']
                next_epoch = sorted_data.iloc[i+1]['epoch_seconds']
                gap_size = next_epoch - current_epoch - 1
                
                # If there's a gap and it's less than the threshold, fill it
                if 0 < gap_size <= max_gap_threshold:
                    # Get the values at the start and end of the gap
                    start_lat = sorted_data.iloc[i]['latitude']
                    end_lat = sorted_data.iloc[i+1]['latitude']
                    start_lon = sorted_data.iloc[i]['longitude']
                    end_lon = sorted_data.iloc[i+1]['longitude']
                    
                    # Calculate the step size for interpolation
                    lat_step = (end_lat - start_lat) / (gap_size + 1)
                    lon_step = (end_lon - start_lon) / (gap_size + 1)
                    
                    # Get the start timestamp
                    start_time = sorted_data.iloc[i]['timestamp']
                    if pd.isna(start_time):
                        # If start time is missing, use epoch seconds
                        start_time = pd.to_datetime(sorted_data.iloc[i]['epoch_seconds'], unit='s')
                    
                    # Fill the gap
                    for j in range(1, gap_size + 1):
                        # Calculate the epoch seconds for this point
                        epoch = current_epoch + j
                        
                        # Calculate interpolated values
                        lat = start_lat + lat_step * j
                        lon = start_lon + lon_step * j
                        
                        # Calculate the timestamp for this point
                        time_diff = epoch - current_epoch
                        timestamp = pd.to_datetime(start_time) + pd.Timedelta(seconds=time_diff)
                        
                        # Create a new data point
                        new_point = {
                            'epoch_seconds': epoch,
                            'timestamp': timestamp,
                            'latitude': lat,
                            'longitude': lon
                        }
                        
                        # Add to result
                        result.append(new_point)
        
        # Convert result to DataFrame
        result_df = pd.DataFrame(result)
        
        # Log the number of points added
        original_count = len(sorted_data)
        filled_count = len(result_df)
        added_count = filled_count - original_count
        
        self.logger.info(f"Added {added_count} interpolated points to fill gaps less than {max_gap_threshold} seconds")
        
        return result_df
    
    def fill_missing_data_complex(self, gps_data: pd.DataFrame) -> pd.DataFrame:
        """
        Advanced method to fill missing data points with track segmentation
        
        Parameters:
        - gps_data: DataFrame containing GPS data
        
        Returns:
        - DataFrame with continuous data points (one second intervals)
        """
        self.logger.info("Using complex method to fill missing data points")
        
        # Get the min and max epoch seconds
        min_epoch = gps_data['epoch_seconds'].min()
        max_epoch = gps_data['epoch_seconds'].max()
        
        # Define the maximum gap threshold (3 minutes = 180 seconds)
        max_gap_threshold = 180
        
        # Create a complete range of epoch seconds
        complete_range = pd.DataFrame({'epoch_seconds': range(min_epoch, max_epoch + 1)})
        
        # Merge with original data
        merged_data = pd.merge(complete_range, gps_data, on='epoch_seconds', how='left')
        
        # Count missing values
        missing_count = merged_data['latitude'].isna().sum()
        self.logger.info(f"Found {missing_count} missing data points")
        
        if missing_count > 0:
            # Identify gaps larger than the threshold
            # First, find the indices of non-missing values
            non_missing_indices = merged_data['latitude'].notna().nonzero()[0]
            
            # If we have at least 2 non-missing values
            if len(non_missing_indices) >= 2:
                # Calculate gaps between consecutive non-missing values
                gaps = np.diff(non_missing_indices)
                
                # Find indices where gaps exceed the threshold
                large_gap_indices = np.where(gaps > max_gap_threshold)[0]
                
                # Create a list of track segments
                track_segments = []
                start_idx = 0
                
                for gap_idx in large_gap_indices:
                    # End index of current segment
                    end_idx = non_missing_indices[gap_idx]
                    # Start index of next segment
                    next_start_idx = non_missing_indices[gap_idx + 1]
                    
                    # Add the current segment
                    track_segments.append((start_idx, end_idx))
                    # Update start index for next segment
                    start_idx = next_start_idx
                
                # Add the last segment
                track_segments.append((start_idx, len(merged_data)))
                
                # Fill missing values within each segment
                for start, end in track_segments:
                    segment = merged_data.iloc[start:end]
                    if len(segment) > 1:
                        # Fill missing values with interpolation
                        merged_data.loc[segment.index, 'latitude'] = segment['latitude'].interpolate(method='linear')
                        merged_data.loc[segment.index, 'longitude'] = segment['longitude'].interpolate(method='linear')
                        
                        # Fill missing timestamps
                        first_timestamp = merged_data.loc[segment.index[0], 'timestamp']
                        if pd.isna(first_timestamp):
                            # If the first timestamp is missing, find the first non-missing one
                            non_missing_timestamps = merged_data.loc[segment.index, 'timestamp'].dropna()
                            if not non_missing_timestamps.empty:
                                first_timestamp = non_missing_timestamps.iloc[0]
                            else:
                                # If all timestamps are missing, use the epoch seconds
                                first_timestamp = pd.to_datetime(merged_data.loc[segment.index[0], 'epoch_seconds'], unit='s')
                        
                        # Calculate timestamps for the segment
                        merged_data.loc[segment.index, 'timestamp'] = pd.to_datetime(first_timestamp) + \
                                                                     pd.to_timedelta(merged_data.loc[segment.index, 'epoch_seconds'] - \
                                                                                    merged_data.loc[segment.index[0], 'epoch_seconds'], unit='s')
                
                self.logger.info(f"Filled missing data points with interpolation, treating gaps > {max_gap_threshold} seconds as separate tracks")
                self.logger.info(f"Identified {len(track_segments)} separate track segments")
            else:
                # If we have less than 2 non-missing values, just interpolate everything
                merged_data['latitude'] = merged_data['latitude'].interpolate(method='linear')
                merged_data['longitude'] = merged_data['longitude'].interpolate(method='linear')
                
                # Fill missing timestamps
                merged_data['timestamp'] = pd.to_datetime(merged_data['timestamp'].iloc[0]) + \
                                          pd.to_timedelta(merged_data['epoch_seconds'] - merged_data['epoch_seconds'].iloc[0], unit='s')
                
                self.logger.info("Filled missing data points with interpolation (insufficient data for gap detection)")
        
        return merged_data
    
    def calculate_velocities(self, gps_data: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate velocity magnitude and direction for each data point
        
        Parameters:
        - gps_data: DataFrame containing GPS data
        
        Returns:
        - DataFrame with added velocity magnitude and direction columns
        """
        self.logger.info("Calculating velocities")
        
        # Create a copy to avoid modifying the original
        result = gps_data.copy()
        
        # Calculate velocity magnitude and direction
        # For the first point, use the same values as the second point
        result['velocity_magnitude'] = 0.0
        result['velocity_direction'] = 0.0
        
        # Calculate for all points except the first one
        for i in range(1, len(result)):
            # Get current and previous points
            current = result.iloc[i]
            previous = result.iloc[i-1]
            
            # Calculate time difference in seconds
            time_diff = current['epoch_seconds'] - previous['epoch_seconds']
            
            if time_diff > 0:
                # Calculate distance using Haversine formula
                lat1, lon1 = previous['latitude'], previous['longitude']
                lat2, lon2 = current['latitude'], current['longitude']
                
                # Convert to radians
                lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
                
                # Haversine formula
                dlon = lon2 - lon1
                dlat = lat2 - lat1
                a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
                c = 2 * np.arcsin(np.sqrt(a))
                r = 6371  # Radius of earth in kilometers
                distance = c * r * 1000  # Convert to meters
                
                # Calculate velocity magnitude (m/s)
                velocity_magnitude = distance / time_diff
                
                # Calculate velocity direction (degrees, 0 = North, 90 = East)
                velocity_direction = np.degrees(np.arctan2(dlon, dlat))
                # Normalize to 0-360
                velocity_direction = (velocity_direction + 360) % 360
                
                # Store results
                result.at[result.index[i], 'velocity_magnitude'] = velocity_magnitude
                result.at[result.index[i], 'velocity_direction'] = velocity_direction
        
        # For the first point, use the same values as the second point
        if len(result) > 1:
            result.at[result.index[0], 'velocity_magnitude'] = result.iloc[1]['velocity_magnitude']
            result.at[result.index[0], 'velocity_direction'] = result.iloc[1]['velocity_direction']
        
        self.logger.info("Velocity calculations completed")
        return result
    
    def save_gps_results(self, gps_data: pd.DataFrame) -> None:
        """
        Save processed GPS data to the gps_results table
        
        Parameters:
        - gps_data: DataFrame containing processed GPS data
        """
        self.logger.info("Saving processed GPS data to database")
        
        # Clear existing results
        self.db.clear_gps_results()
        
        # Save each data point
        for _, row in gps_data.iterrows():
            # Convert timestamp to string format if it's a datetime object
            timestamp = row['timestamp']
            if isinstance(timestamp, pd.Timestamp):
                timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
            
            data_point = {
                'epoch_seconds': int(row['epoch_seconds']),  # Ensure it's an integer
                'timestamp': timestamp,
                'latitude': float(row['latitude']),  # Ensure it's a float
                'longitude': float(row['longitude']),  # Ensure it's a float
                'velocity_magnitude': float(row['velocity_magnitude']),  # Ensure it's a float
                'velocity_direction': float(row['velocity_direction'])  # Ensure it's a float
            }
            
            self.db.save_gps_result(data_point)
        
        self.logger.info(f"Saved {len(gps_data)} processed GPS data points to database")
    
    def process_gps_data(self) -> None:
        """
        Process GPS data: retrieve, fill missing data, calculate velocities, and save results
        """
        self.logger.info("Starting GPS data processing")
        
        try:
            # Get GPS data
            gps_data = self.get_gps_data()
            
            if len(gps_data) == 0:
                self.logger.warning("No GPS data found in database")
                return
            
            # Fill missing data
            filled_data = self.fill_missing_data(gps_data)
            
            # Calculate velocities
            processed_data = self.calculate_velocities(filled_data)
            
            # Save results
            self.save_gps_results(processed_data)
            
            self.logger.info("GPS data processing completed successfully")
            
        except Exception as e:
            self.logger.error(f"Error processing GPS data: {str(e)}")
            raise


def main():
    # Example usage
    gps_manager = GPSDataManager(debug=True)
    
    # Process GPS data
    gps_manager.process_gps_data()
    
    # Or process all files in a directory
    # directory_path = "TracksData"
    # gps_manager.process_directory(directory_path)

if __name__ == "__main__":
    main() 