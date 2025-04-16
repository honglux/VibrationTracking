import pandas as pd
import numpy as np
from database_manager import DatabaseManager
import plotly.graph_objects as go
import plotly.io as pio
import os
from datetime import datetime
import logging
import sqlite3

class MapVisualizer:
    def __init__(self, debug: bool = False):
        """
        Initialize the Map Visualizer
        
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
        log_file = os.path.join(logs_dir, f"map_visualization_{timestamp}.log")
        
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG if self.debug else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("MapVisualizer")
    
    def get_combined_data(self) -> pd.DataFrame:
        """
        Get GPS data and severity scores combined
        
        Returns:
        - DataFrame containing combined GPS and severity data
        """
        self.logger.info("Fetching GPS data and severity scores")
        
        # Get all GPS data with timeout
        with sqlite3.connect(self.db.db_path, timeout=30) as conn:
            # Get GPS results data instead of raw GPS data
            gps_data = pd.read_sql_query('''
                SELECT * FROM gps_results 
                ORDER BY epoch_seconds
            ''', conn)
            
            # Get all analysis results
            analysis_results = pd.read_sql_query('''
                SELECT epoch_seconds, severity_score 
                FROM analysis_results
                ORDER BY epoch_seconds
            ''', conn)
        
        # Find maximum severity score
        max_severity = analysis_results['severity_score'].max()
        self.logger.info(f"Maximum severity score: {max_severity:.2f}")
        
        # Merge GPS data with analysis results
        combined_data = pd.merge(
            gps_data,
            analysis_results,
            on='epoch_seconds',
            how='inner'
        )
        
        # Calculate percentage scores
        combined_data['percentage_score'] = (combined_data['severity_score'] / max_severity) * 100
        combined_data['percentage_score'] = combined_data['percentage_score'].clip(0, 100)
        
        # Convert timestamp to readable format
        combined_data['timestamp'] = pd.to_datetime(combined_data['timestamp'])
        combined_data['time_str'] = combined_data['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
        
        self.logger.info(f"Combined {len(combined_data)} data points")
        return combined_data
    
    def calculate_map_bounds(self, data: pd.DataFrame) -> dict:
        """
        Calculate map bounds and zoom level based on data points
        
        Parameters:
        - data: DataFrame containing GPS data
        
        Returns:
        - Dictionary containing map bounds and zoom settings
        """
        # Calculate the bounding box
        lat_min, lat_max = data['latitude'].min(), data['latitude'].max()
        lon_min, lon_max = data['longitude'].min(), data['longitude'].max()
        
        # Add padding to the bounds (20% on each side)
        lat_padding = (lat_max - lat_min) * 0.2
        lon_padding = (lon_max - lon_min) * 0.2
        
        # Apply padding to bounds
        lat_min_padded = lat_min - lat_padding
        lat_max_padded = lat_max + lat_padding
        lon_min_padded = lon_min - lon_padding
        lon_max_padded = lon_max + lon_padding
        
        # Calculate center point
        lat_center = (lat_min + lat_max) / 2
        lon_center = (lon_min + lon_max) / 2
        
        # Calculate the span
        lat_span = lat_max_padded - lat_min_padded
        lon_span = lon_max_padded - lon_min_padded
        
        # Calculate zoom level based on the larger span
        # Use a more conservative zoom level to show more area
        max_span = max(lat_span, lon_span)
        if max_span < 0.01:  # Very small area
            zoom = 13  # Reduced from 15
        elif max_span < 0.05:  # Small area
            zoom = 11  # Reduced from 13
        elif max_span < 0.1:  # Medium area
            zoom = 9   # Reduced from 11
        else:  # Large area
            zoom = 7   # Reduced from 9
        
        return {
            'center': {'lat': lat_center, 'lon': lon_center},
            'zoom': zoom,
            'bounds': {
                'north': lat_max_padded,
                'south': lat_min_padded,
                'east': lon_max_padded,
                'west': lon_min_padded
            }
        }
    
    def create_map(self, data: pd.DataFrame, output_dir: str = None) -> None:
        """
        Create an interactive map visualization
        
        Parameters:
        - data: DataFrame containing combined GPS and severity data
        - output_dir: Directory to save the output (if None, uses Results directory)
        """
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Results")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
        
        self.logger.info("Creating map visualization")
        
        # Calculate map bounds and zoom
        self.logger.info("Calculating map bounds...")
        map_settings = self.calculate_map_bounds(data)
        self.logger.info(f"Map settings calculated: zoom={map_settings['zoom']}, center={map_settings['center']}")
        
        # Create figure
        self.logger.info("Creating figure...")
        fig = go.Figure()
        
        # Calculate opacity based on severity percentage
        # Higher severity = higher opacity
        min_opacity = 0.3  # Minimum opacity for low severity points
        max_opacity = 1.0  # Maximum opacity for high severity points
        
        # Calculate opacity for each point
        opacity = min_opacity + (data['percentage_score'] / 100) * (max_opacity - min_opacity)
        
        # Add scatter plot for GPS points using scattermap
        self.logger.info("Adding GPS points to map...")
        fig.add_trace(go.Scattermap(
            lat=data['latitude'],
            lon=data['longitude'],
            mode='markers',
            marker=dict(
                size=8,  # Slightly smaller points for better visibility
                color=data['percentage_score'],
                colorscale=[
                    [0, 'white'],      # 0% = white
                    [0.3, '#ffcccc'],  # 30% = light red
                    [0.6, '#ff9999'],  # 60% = medium red
                    [0.8, '#ff6666'],  # 80% = darker red
                    [1.0, '#ff0000']   # 100% = bright red
                ],
                colorbar=dict(
                    title='Severity Score (%)',
                    x=1.1,  # Position the colorbar slightly to the right
                    xanchor='left'
                ),
                showscale=True,
                opacity=opacity  # Use calculated opacity
            ),
            text=data['percentage_score'].round(1).astype(str) + '%',
            hovertemplate="<b>Time:</b> %{customdata[0]}<br>" +
                         "<b>Severity Score:</b> %{text}<br>" +
                         "<b>Latitude:</b> %{lat:.6f}<br>" +
                         "<b>Longitude:</b> %{lon:.6f}<br>" +
                         "<b>Velocity:</b> %{customdata[1]:.1f} m/s<extra></extra>",
            customdata=data[['time_str', 'velocity_magnitude']].values
        ))
        
        # Update layout with calculated bounds and improved interaction settings
        self.logger.info("Updating layout...")
        fig.update_layout(
            title='Vibration Severity Map',
            map=dict(
                style='white-bg',
                zoom=map_settings['zoom'],
                center=map_settings['center'],
                bearing=0,  # Ensure north is up
                pitch=0,    # No tilt
                bounds=map_settings['bounds']
            ),
            margin={'r': 50, 't': 30, 'l': 0, 'b': 0},
            height=800,
            showlegend=False,
            # Add improved interaction settings
            dragmode='pan',  # Default to pan mode for easier navigation
            hovermode='closest',  # Show hover info for the closest point
            # Add buttons for zoom and pan
            updatemenus=[
                dict(
                    type="buttons",
                    direction="right",
                    active=0,
                    x=0.1,
                    y=1.1,
                    buttons=list([
                        dict(
                            label="Pan",
                            method="relayout",
                            args=[{"dragmode": "pan"}]
                        ),
                        dict(
                            label="Zoom",
                            method="relayout",
                            args=[{"dragmode": "zoom"}]
                        ),
                        dict(
                            label="Reset",
                            method="relayout",
                            args=[{"map.zoom": map_settings['zoom'], 
                                  "map.center": map_settings['center']}]
                        )
                    ])
                )
            ]
        )

        # Save as interactive HTML file first
        self.logger.info("Saving interactive HTML...")
        output_file = os.path.join(output_dir, 'vibration_severity_map.html')
        pio.write_html(fig, file=output_file, auto_open=True)  # Auto-open the file
        self.logger.info(f"Saved interactive map to: {output_file}")
        
        # # Try to save as static PNG with error handling
        # self.logger.info("Attempting to save static PNG...")
        # png_file = os.path.join(output_dir, 'vibration_severity_map.png')
        # try:
        #     # First try with kaleido engine
        #     pio.write_image(fig, png_file, scale=2, engine='kaleido')
        #     self.logger.info(f"Saved static map to: {png_file}")
        # except Exception as e:
        #     self.logger.warning(f"Failed to save PNG with kaleido engine: {str(e)}")
        #     try:
        #         # Fallback to default engine
        #         pio.write_image(fig, png_file, scale=2)
        #         self.logger.info(f"Saved static map using default engine to: {png_file}")
        #     except Exception as e:
        #         self.logger.error(f"Failed to save PNG with default engine: {str(e)}")
        #         self.logger.info("Continuing without PNG export...")
        
        # Log completion
        self.logger.info("Map visualization completed successfully")


def main():
    # Create visualizer instance with debug mode enabled
    visualizer = MapVisualizer(debug=True)
    
    try:
        # Get combined data
        data = visualizer.get_combined_data()
        
        # Create map visualization
        visualizer.create_map(data, output_dir="Results")
        
        print("\nMap visualization completed successfully!")
        print("Check the Results directory for the output files.")
        
    except Exception as e:
        print(f"Error during visualization: {str(e)}")
        raise

if __name__ == "__main__":
    main() 