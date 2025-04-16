import sqlite3
import os
from datetime import datetime
import pandas as pd
import json
import time

class DatabaseManager:
    def __init__(self, db_folder="Database"):
        """
        Initialize the database manager
        
        Parameters:
        - db_folder: Folder to store database files
        """
        # Create database folder if it doesn't exist
        if not os.path.exists(db_folder):
            os.makedirs(db_folder)
        
        self.db_folder = db_folder
        self.db_path = os.path.join(db_folder, "vibration_analysis.db")
        self._create_tables()
    
    def _create_tables(self):
        """
        Create the necessary database tables if they don't exist
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create raw_data table for storing the original data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS raw_data (
                    epoch_seconds INTEGER PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    speed_x REAL NOT NULL,
                    speed_y REAL NOT NULL,
                    speed_z REAL NOT NULL,
                    displacement_x REAL NOT NULL,
                    displacement_y REAL NOT NULL,
                    displacement_z REAL NOT NULL,
                    temperature REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create analysis_results table for storing calculated metrics
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS analysis_results (
                    epoch_seconds INTEGER PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    velocity_score REAL NOT NULL,
                    mean_displacement REAL NOT NULL,
                    severity_score REAL NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (epoch_seconds) REFERENCES raw_data (epoch_seconds)
                )
            ''')
            
            # Create gps_data table for storing GPS track points
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS gps_data (
                    epoch_seconds INTEGER PRIMARY KEY,
                    file_name TEXT NOT NULL,
                    timestamp DATETIME NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    elevation REAL NOT NULL,
                    speed REAL,
                    gradient REAL,
                    length REAL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Create gps_results table for storing processed GPS data
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS gps_results (
                    epoch_seconds INTEGER PRIMARY KEY,
                    timestamp DATETIME NOT NULL,
                    latitude REAL NOT NULL,
                    longitude REAL NOT NULL,
                    velocity_magnitude REAL NOT NULL,
                    velocity_direction REAL NOT NULL,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (epoch_seconds) REFERENCES gps_data (epoch_seconds)
                )
            ''')
            
            conn.commit()
    
    def save_data_point(self, data_point):
        """
        Save or update a single data point
        
        Parameters:
        - data_point: Dictionary containing the data point information
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Convert timestamp to epoch seconds
            timestamp = pd.to_datetime(data_point['timestamp'])
            epoch_seconds = int(timestamp.timestamp())
            
            # Use REPLACE INTO to handle conflicts (update if exists)
            cursor.execute('''
                REPLACE INTO raw_data 
                (epoch_seconds, file_name, timestamp, speed_x, speed_y, speed_z,
                 displacement_x, displacement_y, displacement_z, temperature)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                epoch_seconds,
                data_point['file_name'],
                data_point['timestamp'],
                data_point['speed_x'],
                data_point['speed_y'],
                data_point['speed_z'],
                data_point['displacement_x'],
                data_point['displacement_y'],
                data_point['displacement_z'],
                data_point['temperature']
            ))
            
            conn.commit()
            return epoch_seconds
    
    def save_analysis_result(self, result):
        """
        Save or update a single analysis result
        
        Parameters:
        - result: Dictionary containing the analysis result
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Use REPLACE INTO to handle conflicts (update if exists)
            cursor.execute('''
                REPLACE INTO analysis_results 
                (epoch_seconds, file_name, velocity_score, mean_displacement, severity_score)
                VALUES (?, ?, ?, ?, ?)
            ''', (
                result['epoch_seconds'],
                result['file_name'],
                result['velocity_score'],
                result['mean_displacement'],
                result['severity_score']
            ))
            
            conn.commit()
    
    def get_data_point(self, epoch_seconds):
        """
        Retrieve a single data point by epoch seconds
        
        Parameters:
        - epoch_seconds: The epoch seconds to look up
        
        Returns:
        - Dictionary containing the data point information
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM raw_data WHERE epoch_seconds = ?
            ''', (epoch_seconds,))
            
            row = cursor.fetchone()
            if row is None:
                return None
            
            # Convert row to dictionary
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, row))
    
    def get_analysis_result(self, epoch_seconds):
        """
        Retrieve a single analysis result by epoch seconds
        
        Parameters:
        - epoch_seconds: The epoch seconds to look up
        
        Returns:
        - Dictionary containing the analysis result
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM analysis_results WHERE epoch_seconds = ?
            ''', (epoch_seconds,))
            
            row = cursor.fetchone()
            if row is None:
                return None
            
            # Convert row to dictionary
            columns = [description[0] for description in cursor.description]
            return dict(zip(columns, row))
    
    def get_file_data(self, file_name):
        """
        Retrieve all data points and analysis results for a specific file
        
        Parameters:
        - file_name: Name of the file to retrieve data for
        
        Returns:
        - Dictionary containing raw data and analysis results
        """
        with sqlite3.connect(self.db_path) as conn:
            # Get raw data
            raw_data = pd.read_sql_query('''
                SELECT * FROM raw_data 
                WHERE file_name = ?
                ORDER BY epoch_seconds
            ''', conn, params=(file_name,))
            
            # Get analysis results
            analysis_results = pd.read_sql_query('''
                SELECT * FROM analysis_results 
                WHERE file_name = ?
                ORDER BY epoch_seconds
            ''', conn, params=(file_name,))
            
            return {
                'raw_data': raw_data,
                'analysis_results': analysis_results
            }
    
    def delete_file_data(self, file_name):
        """
        Delete all data points and analysis results for a specific file
        
        Parameters:
        - file_name: Name of the file to delete data for
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Delete in correct order to maintain referential integrity
            cursor.execute('DELETE FROM analysis_results WHERE file_name = ?', (file_name,))
            cursor.execute('DELETE FROM raw_data WHERE file_name = ?', (file_name,))
            
            conn.commit()
    
    def save_gps_point(self, data_point):
        """
        Save or update a single GPS data point
        
        Parameters:
        - data_point: Dictionary containing the GPS data point information
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Convert timestamp string to epoch seconds
            timestamp = pd.to_datetime(data_point['timestamp'])
            epoch_seconds = int(timestamp.timestamp())
            
            # Use REPLACE INTO to handle conflicts (update if exists)
            cursor.execute('''
                REPLACE INTO gps_data 
                (epoch_seconds, file_name, timestamp, latitude, longitude, elevation,
                 speed, gradient, length)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                epoch_seconds,
                data_point['file_name'],
                data_point['timestamp'],  # Use the string format timestamp
                data_point['latitude'],
                data_point['longitude'],
                data_point['elevation'],
                data_point.get('speed'),
                data_point.get('gradient'),
                data_point.get('length')
            ))
            
            conn.commit()
            return epoch_seconds
    
    def get_gps_data(self, file_name):
        """
        Retrieve all GPS data points for a specific file
        
        Parameters:
        - file_name: Name of the file to retrieve data for
        
        Returns:
        - DataFrame containing GPS data points
        """
        with sqlite3.connect(self.db_path) as conn:
            # Get GPS data
            gps_data = pd.read_sql_query('''
                SELECT * FROM gps_data 
                WHERE file_name = ?
                ORDER BY epoch_seconds
            ''', conn, params=(file_name,))
            
            return gps_data
        
    def get_all_gps_data(self):
        """
        Retrieve all GPS data points from the database
        
        Returns:
        - DataFrame containing all GPS data points
        """
        with sqlite3.connect(self.db_path) as conn:
            # Get all GPS data
            gps_data = pd.read_sql_query('''
                SELECT * FROM gps_data
                ORDER BY epoch_seconds
            ''', conn)
            return gps_data
    
    def get_gps_data_by_time_range(self, start_time, end_time):
        """
        Retrieve GPS data points within a time range
        
        Parameters:
        - start_time: Start timestamp (datetime or epoch seconds)
        - end_time: End timestamp (datetime or epoch seconds)
        
        Returns:
        - DataFrame containing GPS data points
        """
        with sqlite3.connect(self.db_path) as conn:
            # Convert timestamps to epoch seconds if they're datetime objects
            if isinstance(start_time, pd.Timestamp):
                start_epoch = int(start_time.timestamp())
            else:
                start_epoch = start_time
                
            if isinstance(end_time, pd.Timestamp):
                end_epoch = int(end_time.timestamp())
            else:
                end_epoch = end_time
            
            # Get GPS data
            gps_data = pd.read_sql_query('''
                SELECT * FROM gps_data 
                WHERE epoch_seconds BETWEEN ? AND ?
                ORDER BY epoch_seconds
            ''', conn, params=(start_epoch, end_epoch))
            
            return gps_data
    
    def clear_gps_data(self):
        """
        Clear all GPS data from the database
        
        This method will delete all records from the gps_data table
        without affecting other tables.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM gps_data')
            conn.commit()
            print(f"Cleared {cursor.rowcount} GPS data points from the database")
    
    def save_gps_result(self, data_point):
        """
        Save or update a single processed GPS data point
        
        Parameters:
        - data_point: Dictionary containing the processed GPS data point information
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Convert timestamp string to epoch seconds if needed
            if isinstance(data_point['timestamp'], str):
                timestamp = pd.to_datetime(data_point['timestamp'])
                epoch_seconds = int(timestamp.timestamp())
            else:
                epoch_seconds = data_point['epoch_seconds']
            
            # Use REPLACE INTO to handle conflicts (update if exists)
            cursor.execute('''
                REPLACE INTO gps_results 
                (epoch_seconds, timestamp, latitude, longitude, velocity_magnitude, velocity_direction)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                epoch_seconds,
                data_point['timestamp'],
                data_point['latitude'],
                data_point['longitude'],
                data_point['velocity_magnitude'],
                data_point['velocity_direction']
            ))
            
            conn.commit()
            return epoch_seconds
    
    def get_gps_results(self, start_time=None, end_time=None):
        """
        Retrieve processed GPS data points within a time range
        
        Parameters:
        - start_time: Start timestamp (datetime or epoch seconds)
        - end_time: End timestamp (datetime or epoch seconds)
        
        Returns:
        - DataFrame containing processed GPS data points
        """
        with sqlite3.connect(self.db_path) as conn:
            if start_time is not None and end_time is not None:
                # Convert timestamps to epoch seconds if they're datetime objects
                if isinstance(start_time, pd.Timestamp):
                    start_epoch = int(start_time.timestamp())
                else:
                    start_epoch = start_time
                    
                if isinstance(end_time, pd.Timestamp):
                    end_epoch = int(end_time.timestamp())
                else:
                    end_epoch = end_time
                
                # Get GPS results within time range
                gps_results = pd.read_sql_query('''
                    SELECT * FROM gps_results 
                    WHERE epoch_seconds BETWEEN ? AND ?
                    ORDER BY epoch_seconds
                ''', conn, params=(start_epoch, end_epoch))
            else:
                # Get all GPS results
                gps_results = pd.read_sql_query('''
                    SELECT * FROM gps_results 
                    ORDER BY epoch_seconds
                ''', conn)
            
            return gps_results
    
    def clear_gps_results(self):
        """
        Clear all processed GPS data from the database
        
        This method will delete all records from the gps_results table
        without affecting other tables.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM gps_results')
            conn.commit()
            print(f"Cleared {cursor.rowcount} processed GPS data points from the database")

def main():
    # Example usage
    from vibration_analyzer import VibrationAnalyzer
    
    # Initialize database manager
    db = DatabaseManager()
    
    # Example: Clear GPS data
    db.clear_gps_data()
    
    # Example: Analyze a file and save to database
    data_folder = "VibData"
    file_name = "20250323152752.txt"
    file_path = os.path.join(data_folder, file_name)
    
    # Create and run analyzer
    analyzer = VibrationAnalyzer(file_path)
    analyzer.read_data()
    analyzer.analyze_data_by_second()
    
    # Example of saving data points and analysis results
    for _, row in analyzer.data.iterrows():
        # Save raw data point
        data_point = {
            'timestamp': row['time'],
            'file_name': file_name,
            'speed_x': row['SpeedX(mm/s)'],
            'speed_y': row['SpeedY(mm/s)'],
            'speed_z': row['SpeedZ(mm/s)'],
            'displacement_x': row['DisplacementX(um)'],
            'displacement_y': row['DisplacementY(um)'],
            'displacement_z': row['DisplacementZ(um)'],
            'temperature': row['Temperature(°C)']
        }
        epoch_seconds = db.save_data_point(data_point)
        
        # Find corresponding analysis result
        analysis_row = analyzer.grouped_data[analyzer.grouped_data['second'] == row['time'].floor('s')].iloc[0]
        
        # Save analysis result
        result = {
            'epoch_seconds': epoch_seconds,
            'file_name': file_name,
            'velocity_score': analysis_row['velocity_score'],
            'mean_displacement': analysis_row['mean_displacement'],
            'severity_score': analysis_row['vibration_severity_score']
        }
        db.save_analysis_result(result)
    
    # Example of retrieving data
    file_data = db.get_file_data(file_name)
    print("\nRaw data:")
    print(file_data['raw_data'].head())
    print("\nAnalysis results:")
    print(file_data['analysis_results'].head())

if __name__ == "__main__":
    main() 