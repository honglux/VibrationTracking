import os
import pandas as pd
import json
from datetime import datetime
import logging
import sqlite3
from database_manager import DatabaseManager
import math
from robust_max_calculator import RobustMaxCalculator
from typing import Tuple, Optional

class LeafletMapVisualizer:
    """
    Class for generating interactive Leaflet.js maps with OpenStreetMap
    """
    def __init__(self, debug: bool = False):
        """
        Initialize the Leaflet Map Visualizer
        
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
        log_file = os.path.join(logs_dir, f"leaflet_map_visualization_{timestamp}.log")
        
        # Configure logging
        logging.basicConfig(
            level=logging.DEBUG if self.debug else logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger("LeafletMapVisualizer")
    
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
        # max_severity = analysis_results['severity_score'].max()
        scaler = RobustMaxCalculator()
        robust_max, _ = scaler.get_robust_max()
        self.logger.info(f"Maximum severity score: {robust_max:.2f}")
        
        # Merge GPS data with analysis results
        combined_data = pd.merge(
            gps_data,
            analysis_results,
            on='epoch_seconds',
            how='inner'
        )
        
        # Calculate percentage scores
        combined_data['percentage_score'] = (combined_data['severity_score'] / robust_max) * 100
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
    
    # Deprecated
    def create_map(self, data: pd.DataFrame, output_dir: str = None) -> None:
        """
        Create an interactive Leaflet map visualization
        
        Parameters:
        - data: DataFrame containing combined GPS and severity data
        - output_dir: Directory to save the output (if None, uses Results directory)
        """
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Results")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
        
        self.logger.info("Creating Leaflet map visualization")
        
        # Calculate map bounds and zoom
        self.logger.info("Calculating map bounds...")
        map_settings = self.calculate_map_bounds(data)
        self.logger.info(f"Map settings calculated: zoom={map_settings['zoom']}, center={map_settings['center']}")
        
        # Convert data to GeoJSON format
        self.logger.info("Converting data to GeoJSON format...")
        features = []
        
        for _, row in data.iterrows():
            # Calculate color based on severity percentage
            percentage = row['percentage_score']
            if percentage < 30:
                color = '#a0d8ef'  # Light blue
            elif percentage < 60:
                color = '#ffcccc'  # Light red
            elif percentage < 80:
                color = '#ff9999'  # Medium red
            else:
                color = '#ff0000'  # Bright red
            
            # Calculate opacity based on severity percentage
            # Use a higher minimum opacity to ensure all points are visible
            opacity = 0.5 + (percentage / 100) * 0.5  # Range from 0.5 to 1.0
            
            # Create feature
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [row['longitude'], row['latitude']]
                },
                'properties': {
                    'time': row['time_str'],
                    'severity_score': f"{percentage:.1f}%",
                    'velocity': f"{row['velocity_magnitude']:.1f} m/s",
                    'color': color,
                    'opacity': opacity,
                    'radius': 5  # Marker radius in pixels
                }
            }
            features.append(feature)
        
        # Create GeoJSON collection
        geojson_data = {
            'type': 'FeatureCollection',
            'features': features
        }
        
        # Create HTML file with Leaflet map
        self.logger.info("Generating HTML file with Leaflet map...")
        html_content = self._generate_html_content(geojson_data, map_settings)
        
        # Save HTML file
        output_file = os.path.join(output_dir, 'vibration_severity_map_leaflet.html')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.info(f"Saved Leaflet map to: {output_file}")
        self.logger.info("Map visualization completed successfully")
    
    # Deprecated
    def create_line_map(self, data: pd.DataFrame, output_dir: str = None) -> None:
        """
        Create an interactive Leaflet map visualization with lines between sequential points
        
        Parameters:
        - data: DataFrame containing combined GPS and severity data
        - output_dir: Directory to save the output (if None, uses Results directory)
        """
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Results")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
        
        self.logger.info("Creating Leaflet line map visualization")
        
        # Calculate map bounds and zoom
        self.logger.info("Calculating map bounds...")
        map_settings = self.calculate_map_bounds(data)
        self.logger.info(f"Map settings calculated: zoom={map_settings['zoom']}, center={map_settings['center']}")
        
        # Convert data to GeoJSON format with lines
        self.logger.info("Converting data to GeoJSON format with lines...")
        point_features = []
        line_features = []
        
        # Sort data by epoch_seconds to ensure correct line order
        data = data.sort_values('epoch_seconds')
        
        # Create point features
        for _, row in data.iterrows():
            # Calculate color based on severity percentage using gradient
            percentage = row['percentage_score']
            color = self._get_gradient_color(percentage)
            
            # Create point feature
            point_feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [row['longitude'], row['latitude']]
                },
                'properties': {
                    'time': row['time_str'],
                    'severity_score': f"{percentage:.1f}%",
                    'velocity': f"{row['velocity_magnitude']:.1f} m/s",
                    'color': color,
                    'radius': 3  # Smaller radius for points
                }
            }
            point_features.append(point_feature)
        
        # Create line features between sequential points
        for i in range(len(data) - 1):
            current_row = data.iloc[i]
            next_row = data.iloc[i + 1]
            
            # Calculate time difference in seconds
            time_diff = next_row['epoch_seconds'] - current_row['epoch_seconds']
            
            # Only draw line if time difference is less than 60 seconds
            if time_diff < 60:
                # Use the starting point's percentage for the line color
                starting_percentage = current_row['percentage_score']
                color = self._get_gradient_color(starting_percentage)
                
                # Create line feature
                line_feature = {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'LineString',
                        'coordinates': [
                            [current_row['longitude'], current_row['latitude']],
                            [next_row['longitude'], next_row['latitude']]
                        ]
                    },
                    'properties': {
                        'start_time': current_row['time_str'],
                        'end_time': next_row['time_str'],
                        'severity_score': f"{starting_percentage:.1f}%",
                        'color': color,
                        'weight': 7  # Line weight in pixels
                    }
                }
                line_features.append(line_feature)
        
        # Create GeoJSON collections
        points_geojson = {
            'type': 'FeatureCollection',
            'features': point_features
        }
        
        lines_geojson = {
            'type': 'FeatureCollection',
            'features': line_features
        }
        
        # Create HTML file with Leaflet map
        self.logger.info("Generating HTML file with Leaflet line map...")
        html_content = self._generate_line_html_content(points_geojson, lines_geojson, map_settings)
        
        # Save HTML file
        output_file = os.path.join(output_dir, 'vibration_severity_line_map_leaflet.html')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.info(f"Saved Leaflet line map to: {output_file}")
        self.logger.info("Line map visualization completed successfully")
    
    def _get_gradient_color(self, percentage: float) -> str:
        """
        Get a color based on a percentage value using a gradient from green to yellow to red
        
        Parameters:
        - percentage: The percentage value (0-100)
        
        Returns:
        - Hex color code
        """
        # Ensure percentage is within 0-100 range
        percentage = max(0, min(100, percentage))
        
        # Define color stops
        if percentage < 50:
            # Green to Yellow gradient (0-50%)
            # Green: #2e8b57 (Sea Green)
            # Yellow: #daa520 (Goldenrod)
            r = int(46 + (percentage / 50) * (218 - 46))
            g = int(139 + (percentage / 50) * (165 - 139))
            b = int(87 + (percentage / 50) * (32 - 87))
        else:
            # Yellow to Red gradient (50-100%)
            # Yellow: #daa520 (Goldenrod)
            # Red: #8b0000 (Dark Red)
            r = int(218 + ((percentage - 50) / 50) * (139 - 218))
            g = int(165 + ((percentage - 50) / 50) * (0 - 165))
            b = int(32 + ((percentage - 50) / 50) * (0 - 32))
        
        # Convert to hex
        return f"#{r:02x}{g:02x}{b:02x}"
    
    def _generate_html_content(self, geojson_data: dict, map_settings: dict) -> str:
        """
        Generate HTML content with Leaflet map
        
        Parameters:
        - geojson_data: GeoJSON data for visualization
        - map_settings: Map settings (center, zoom, bounds)
        
        Returns:
        - HTML content as string
        """
        # Convert GeoJSON data to JSON string
        geojson_str = json.dumps(geojson_data)
        
        # Generate HTML content
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Vibration Severity Map</title>
    
    <!-- Leaflet CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
          integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
          crossorigin=""/>
    
    <!-- Leaflet MarkerCluster CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.Default.css" />
    
    <!-- Leaflet JavaScript -->
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
            crossorigin=""></script>
    
    <!-- Leaflet MarkerCluster JavaScript -->
    <script src="https://unpkg.com/leaflet.markercluster@1.4.1/dist/leaflet.markercluster.js"></script>
    
    <style>
        body {{
            margin: 0;
            padding: 0;
            font-family: Arial, sans-serif;
        }}
        #map {{
            width: 100%;
            height: 100vh;
        }}
        .info {{
            padding: 6px 8px;
            font: 14px/16px Arial, Helvetica, sans-serif;
            background: white;
            background: rgba(255, 255, 255, 0.8);
            box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
            border-radius: 5px;
        }}
        .legend {{
            line-height: 18px;
            color: #555;
        }}
        .legend i {{
            width: 18px;
            height: 18px;
            float: left;
            margin-right: 8px;
            opacity: 0.7;
        }}
        .controls {{
            position: absolute;
            top: 10px;
            right: 10px;
            z-index: 1000;
            background: white;
            padding: 5px;
            border-radius: 5px;
            box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
        }}
        .controls button {{
            margin: 2px;
            padding: 5px 10px;
            border: none;
            background: #4CAF50;
            color: white;
            cursor: pointer;
            border-radius: 3px;
        }}
        .controls button:hover {{
            background: #45a049;
        }}
    </style>
</head>
<body>
    <div id="map"></div>
    <div class="controls">
        <button onclick="resetMap()">Reset View</button>
        <button onclick="toggleClusters()">Toggle Clusters</button>
    </div>
    
    <script>
        // Map data
        const mapData = {geojson_str};
        const mapSettings = {{
            center: [{map_settings['center']['lat']}, {map_settings['center']['lon']}],
            zoom: {map_settings['zoom']},
            bounds: [
                [{map_settings['bounds']['south']}, {map_settings['bounds']['west']}],
                [{map_settings['bounds']['north']}, {map_settings['bounds']['east']}]
            ]
        }};
        
        // Initialize map
        const map = L.map('map').setView(mapSettings.center, mapSettings.zoom);
        
        // Add OpenStreetMap tile layer
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }}).addTo(map);
        
        // Add Chinese tile layer (optional)
        const chineseTiles = L.tileLayer('https://webrd0{{s}}.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=8&x={{x}}&y={{y}}&z={{z}}', {{
            attribution: '&copy; <a href="https://amap.com">AMap</a>'
        }});
        
        // Add layer control
        const baseMaps = {{
            "OpenStreetMap": L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png'),
            "Chinese Map": chineseTiles
        }};
        L.control.layers(baseMaps).addTo(map);
        
        // Create marker cluster group
        const markers = L.markerClusterGroup();
        
        // Add GeoJSON data to map
        const geojsonLayer = L.geoJSON(mapData, {{
            pointToLayer: function(feature, latlng) {{
                const props = feature.properties;
                return L.circleMarker(latlng, {{
                    radius: props.radius,
                    fillColor: props.color,
                    color: 'transparent', // Remove the black circle
                    weight: 0, // Set weight to 0 to remove the border
                    opacity: 0, // Set opacity to 0 to make the border invisible
                    fillOpacity: props.opacity
                }});
            }},
            onEachFeature: function(feature, layer) {{
                const props = feature.properties;
                layer.bindPopup(`
                    <b>Time:</b> ${{props.time}}<br>
                    <b>Severity Score:</b> ${{props.severity_score}}<br>
                    <b>Velocity:</b> ${{props.velocity}}
                `);
            }}
        }});
        
        // Add GeoJSON layer to marker cluster
        markers.addLayer(geojsonLayer);
        
        // Add marker cluster to map
        map.addLayer(markers);
        
        // Fit map to bounds
        map.fitBounds(mapSettings.bounds);
        
        // Add legend
        const legend = L.control({{position: 'bottomright'}});
        legend.onAdd = function(map) {{
            const div = L.DomUtil.create('div', 'info legend');
            div.innerHTML = `
                <h4>Severity Score</h4>
                <i style="background: #a0d8ef"></i> 0-30%<br>
                <i style="background: #ffcccc"></i> 30-60%<br>
                <i style="background: #ff9999"></i> 60-80%<br>
                <i style="background: #ff0000"></i> 80-100%
            `;
            return div;
        }};
        legend.addTo(map);
        
        // Add scale control
        L.control.scale().addTo(map);
        
        // Function to reset map view
        function resetMap() {{
            map.fitBounds(mapSettings.bounds);
        }}
        
        // Function to toggle clusters
        let clustersEnabled = true;
        function toggleClusters() {{
            if (clustersEnabled) {{
                map.removeLayer(markers);
                geojsonLayer.addTo(map);
                clustersEnabled = false;
            }} else {{
                map.removeLayer(geojsonLayer);
                map.addLayer(markers);
                clustersEnabled = true;
            }}
        }}
    </script>
</body>
</html>
"""
        return html
    
    def _generate_line_html_content(self, points_geojson: dict, lines_geojson: dict, map_settings: dict) -> str:
        """
        Generate HTML content with Leaflet map showing lines between points
        
        Parameters:
        - points_geojson: GeoJSON data for points
        - lines_geojson: GeoJSON data for lines
        - map_settings: Map settings (center, zoom, bounds)
        
        Returns:
        - HTML content as string
        """
        # Convert GeoJSON data to JSON string
        points_geojson_str = json.dumps(points_geojson)
        lines_geojson_str = json.dumps(lines_geojson)
        
        # Generate HTML content
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Vibration Severity Line Map</title>
    
    <!-- Leaflet CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css"
          integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY="
          crossorigin=""/>
    
    <!-- Leaflet MarkerCluster CSS -->
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.4.1/dist/MarkerCluster.Default.css" />
    
    <!-- Leaflet JavaScript -->
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"
            integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo="
            crossorigin=""></script>
    
    <!-- Leaflet MarkerCluster JavaScript -->
    <script src="https://unpkg.com/leaflet.markercluster@1.4.1/dist/leaflet.markercluster.js"></script>
    
    <style>
        body {{
            margin: 0;
            padding: 0;
            font-family: Arial, sans-serif;
        }}
        #map {{
            width: 100%;
            height: 100vh;
        }}
        .info {{
            padding: 6px 8px;
            font: 14px/16px Arial, Helvetica, sans-serif;
            background: white;
            background: rgba(255, 255, 255, 0.8);
            box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
            border-radius: 5px;
        }}
        .legend {{
            line-height: 18px;
            color: #555;
        }}
        .legend i {{
            width: 18px;
            height: 18px;
            float: left;
            margin-right: 8px;
            opacity: 0.7;
        }}
        .controls {{
            position: absolute;
            top: 10px;
            right: 10px;
            z-index: 1000;
            background: white;
            padding: 5px;
            border-radius: 5px;
            box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
        }}
        .controls button {{
            margin: 2px;
            padding: 5px 10px;
            border: none;
            background: #4CAF50;
            color: white;
            cursor: pointer;
            border-radius: 3px;
        }}
        .controls button:hover {{
            background: #45a049;
        }}
    </style>
</head>
<body>
    <div id="map"></div>
    <div class="controls">
        <button onclick="resetMap()">Reset View</button>
        <button onclick="togglePoints()">Toggle Points</button>
    </div>
    
    <script>
        // Map data
        const pointsData = {points_geojson_str};
        const linesData = {lines_geojson_str};
        const mapSettings = {{
            center: [{map_settings['center']['lat']}, {map_settings['center']['lon']}],
            zoom: {map_settings['zoom']},
            bounds: [
                [{map_settings['bounds']['south']}, {map_settings['bounds']['west']}],
                [{map_settings['bounds']['north']}, {map_settings['bounds']['east']}]
            ]
        }};
        
        // Initialize map
        const map = L.map('map').setView(mapSettings.center, mapSettings.zoom);
        
        // Add OpenStreetMap tile layer
        L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }}).addTo(map);
        
        // Add Chinese tile layer (optional)
        const chineseTiles = L.tileLayer('https://webrd0{{s}}.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=8&x={{x}}&y={{y}}&z={{z}}', {{
            attribution: '&copy; <a href="https://amap.com">AMap</a>'
        }});
        
        // Add layer control
        const baseMaps = {{
            "OpenStreetMap": L.tileLayer('https://{{s}}.tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png'),
            "Chinese Map": chineseTiles
        }};
        L.control.layers(baseMaps).addTo(map);
        
        // Create marker cluster group for points
        const markers = L.markerClusterGroup();
        
        // Add points GeoJSON data to map
        const pointsLayer = L.geoJSON(pointsData, {{
            pointToLayer: function(feature, latlng) {{
                const props = feature.properties;
                return L.circleMarker(latlng, {{
                    radius: props.radius,
                    fillColor: props.color,
                    color: 'transparent',
                    weight: 0,
                    opacity: 0,
                    fillOpacity: 1
                }});
            }},
            onEachFeature: function(feature, layer) {{
                const props = feature.properties;
                layer.bindPopup(`
                    <b>Time:</b> ${{props.time}}<br>
                    <b>Severity Score:</b> ${{props.severity_score}}<br>
                    <b>Velocity:</b> ${{props.velocity}}
                `);
            }}
        }});
        
        // Add lines GeoJSON data to map
        const linesLayer = L.geoJSON(linesData, {{
            style: function(feature) {{
                const props = feature.properties;
                return {{
                    color: props.color,
                    weight: props.weight,
                    opacity: 1
                }};
            }},
            onEachFeature: function(feature, layer) {{
                const props = feature.properties;
                layer.bindPopup(`
                    <b>Start Time:</b> ${{props.start_time}}<br>
                    <b>End Time:</b> ${{props.end_time}}<br>
                    <b>Severity Score:</b> ${{props.severity_score}}
                `);
            }}
        }});
        
        // Add GeoJSON layers to marker cluster
        markers.addLayer(pointsLayer);
        
        // Add layers to map
        map.addLayer(linesLayer);
        map.addLayer(markers);
        
        // Fit map to bounds
        map.fitBounds(mapSettings.bounds);
        
        // Add legend
        const legend = L.control({{position: 'bottomright'}});
        legend.onAdd = function(map) {{
            const div = L.DomUtil.create('div', 'info legend');
            div.innerHTML = `
                <h4>Severity Score</h4>
                <i style="background: #2e8b57"></i> 0%<br>
                <i style="background: #daa520"></i> 50%<br>
                <i style="background: #8b0000"></i> 100%
            `;
            return div;
        }};
        legend.addTo(map);
        
        // Add scale control
        L.control.scale().addTo(map);
        
        // Function to reset map view
        function resetMap() {{
            map.fitBounds(mapSettings.bounds);
        }}
        
        // Function to toggle points
        let pointsVisible = true;
        function togglePoints() {{
            if (pointsVisible) {{
                map.removeLayer(markers);
                pointsVisible = false;
            }} else {{
                map.addLayer(markers);
                pointsVisible = true;
            }}
        }}
    </script>
</body>
</html>
"""
        return html
    
    def _transform_coordinates(self, lng: float, lat: float) -> tuple:
        """
        Transform WGS-84 coordinates to GCJ-02 coordinates (used by Gaode Maps)
        
        Parameters:
        - lng: Longitude in WGS-84
        - lat: Latitude in WGS-84
        
        Returns:
        - Tuple of (longitude, latitude) in GCJ-02
        """
        # Constants
        a = 6378245.0  # Semi-major axis
        ee = 0.00669342162296594323  # Eccentricity squared
        
        # Check if the coordinates are in China
        if self._is_out_of_china(lng, lat):
            return lng, lat
        
        # Transform
        dlat = self._transform_lat(lng - 105.0, lat - 35.0)
        dlng = self._transform_lng(lng - 105.0, lat - 35.0)
        
        radlat = lat / 180.0 * 3.141592653589793
        magic = math.sin(radlat)
        magic = 1 - ee * magic * magic
        sqrtmagic = math.sqrt(magic)
        
        dlat = (dlat * 180.0) / ((a * (1 - ee)) / (magic * sqrtmagic) * 3.141592653589793)
        dlng = (dlng * 180.0) / (a / sqrtmagic * math.cos(radlat) * 3.141592653589793)
        
        mglat = lat + dlat
        mglng = lng + dlng
        
        return mglng, mglat
    
    def _is_out_of_china(self, lng: float, lat: float) -> bool:
        """
        Check if the coordinates are outside of China
        
        Parameters:
        - lng: Longitude
        - lat: Latitude
        
        Returns:
        - True if outside China, False otherwise
        """
        return not (73.66 < lng < 135.05 and 3.86 < lat < 53.55)
    
    def _transform_lat(self, lng: float, lat: float) -> float:
        """
        Transform latitude
        
        Parameters:
        - lng: Longitude
        - lat: Latitude
        
        Returns:
        - Transformed latitude
        """
        ret = -100.0 + 2.0 * lng + 3.0 * lat + 0.2 * lat * lat + \
              0.1 * lng * lat + 0.2 * math.sqrt(abs(lng))
        ret += (20.0 * math.sin(6.0 * lng * 3.141592653589793) + 20.0 *
                math.sin(2.0 * lng * 3.141592653589793)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lat * 3.141592653589793) + 40.0 *
                math.sin(lat / 3.0 * 3.141592653589793)) * 2.0 / 3.0
        ret += (160.0 * math.sin(lat / 12.0 * 3.141592653589793) + 320 *
                math.sin(lat * 3.141592653589793 / 30.0)) * 2.0 / 3.0
        return ret
    
    def _transform_lng(self, lng: float, lat: float) -> float:
        """
        Transform longitude
        
        Parameters:
        - lng: Longitude
        - lat: Latitude
        
        Returns:
        - Transformed longitude
        """
        ret = 300.0 + lng + 2.0 * lat + 0.1 * lng * lng + \
              0.1 * lng * lat + 0.1 * math.sqrt(abs(lng))
        ret += (20.0 * math.sin(6.0 * lng * 3.141592653589793) + 20.0 *
                math.sin(2.0 * lng * 3.141592653589793)) * 2.0 / 3.0
        ret += (20.0 * math.sin(lng * 3.141592653589793) + 40.0 *
                math.sin(lng / 3.0 * 3.141592653589793)) * 2.0 / 3.0
        ret += (150.0 * math.sin(lng / 12.0 * 3.141592653589793) + 300.0 *
                math.sin(lng / 30.0 * 3.141592653589793)) * 2.0 / 3.0
        return ret
    
    def _detect_nearby_points(self, data: pd.DataFrame, distance_threshold: float = 3.0, velocity_threshold: float = 0.5) -> pd.DataFrame:
        """
        Detect points that are too close to each other and mark them for filtering.
        For points within the threshold distance, keep only the one with the highest velocity magnitude.
        Points with velocity below the threshold are considered static and not rendered.
        Skip distance comparison with the last three points to avoid filtering consecutive points.
        
        Parameters:
        - data: DataFrame containing GPS data with columns: latitude, longitude, velocity_magnitude
        - distance_threshold: Maximum distance in meters to consider points as "nearby"
        - velocity_threshold: Minimum velocity in m/s to consider a point valid
        
        Returns:
        - DataFrame with an additional 'should_render' column indicating whether to render the point
        """
        self.logger.info("Detecting nearby points")
        
        # Add a column to track whether to render each point
        data['should_render'] = True
        
        # Convert distance threshold from meters to degrees (approximate)
        # 1 degree ≈ 111,320 meters at the equator
        threshold_degrees = distance_threshold / 111320.0
        
        # Create a list to store visited points
        visited_points = []
        
        for i in range(len(data)):
            current_point = data.iloc[i]
            current_lat = current_point['latitude']
            current_lon = current_point['longitude']
            current_vel = current_point['velocity_magnitude']
            
            # Skip points with velocity below threshold
            if current_vel < velocity_threshold:
                # self.logger.info(f"data {str(i)} has been filtered due to low velocity {current_vel}")
                data.at[i, 'should_render'] = False
                continue
            
            # Check against previously visited points, skipping the last three
            for j, (visited_lat, visited_lon, visited_vel, visited_idx) in enumerate(visited_points[:-5]):
                # Calculate distance using simple Euclidean distance (approximate for small distances)
                lat_diff = current_lat - visited_lat
                lon_diff = current_lon - visited_lon
                distance = math.sqrt(lat_diff**2 + lon_diff**2)
                
                if distance < threshold_degrees:
                    # Points are too close, keep the one with higher velocity
                    if current_vel > visited_vel:
                        # Current point has higher velocity, mark the previous one to not render
                        data.at[visited_idx, 'should_render'] = False
                        # Update the visited point with current point's data
                        visited_points[j] = (current_lat, current_lon, current_vel, i)
                    else:
                        # Previous point has higher velocity, mark current point to not render
                        data.at[i, 'should_render'] = False
                    break
            else:
                # No nearby points found, add current point to visited points
                visited_points.append((current_lat, current_lon, current_vel, i))
        
        static_points = len(data) - data['should_render'].sum()
        self.logger.info(f"Filtered {static_points} points due to low velocity or proximity")
        return data

    def _generate_gaode_map_data(self, data: pd.DataFrame, output_dir: str) -> Tuple[str, dict]:
        """
        Generate GeoJSON data for Gaode Maps visualization and save to file
        
        Parameters:
        - data: DataFrame containing GPS and severity data
        - output_dir: Directory to save the data file
        
        Returns:
        - Tuple containing:
          - str: Path to the data file
          - dict: Map settings (center, zoom, bounds)
        """
        self.logger.info("Generating Gaode Maps data")
        
        # Detect and filter nearby points
        data = self._detect_nearby_points(data)
        
        # Calculate map bounds and zoom settings
        map_settings = self.calculate_map_bounds(data)
        
        # Convert data to GeoJSON format
        features = []
        for _, row in data.iterrows():
            # Skip points marked as not to render
            if not row['should_render']:
                continue
                
            # Transform coordinates for Gaode Maps
            lng, lat = self._transform_coordinates(row['longitude'], row['latitude'])
            
            # Calculate color based on severity percentage
            color = self._get_gradient_color(row['percentage_score'])
            
            # Convert timestamp to string
            timestamp_str = row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            # Create point feature
            feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [lng, lat]
                },
                'properties': {
                    'severity_score': row['severity_score'],
                    'percentage_score': row['percentage_score'],
                    'timestamp': timestamp_str,
                    'color': color
                }
            }
            features.append(feature)
        
        # Create GeoJSON collection
        geojson_data = {
            'type': 'FeatureCollection',
            'features': features
        }
        
        # Save data to file
        data_file = os.path.join(output_dir, "vibration_severity_map_gaode_data.json")
        with open(data_file, 'w', encoding='utf-8') as f:
            json.dump(geojson_data, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Saved Gaode Maps data to: {data_file}")
        return data_file, map_settings

    def _generate_gaode_line_map_data(self, data: pd.DataFrame, output_dir: str) -> Tuple[str, str, dict]:
        """
        Generate GeoJSON data for Gaode Maps line visualization and save to files
        
        Parameters:
        - data: DataFrame containing GPS and severity data
        - output_dir: Directory to save the data files
        
        Returns:
        - Tuple containing:
          - str: Path to the points data file
          - str: Path to the lines data file
          - dict: Map settings (center, zoom, bounds)
        """
        self.logger.info("Generating Gaode Maps line data")
        
        # Detect and filter nearby points
        data = self._detect_nearby_points(data)
        
        # Calculate map bounds and zoom settings
        map_settings = self.calculate_map_bounds(data)
        
        # Convert data to GeoJSON format
        point_features = []
        line_features = []
        
        for i in range(len(data) - 1):
            current_row = data.iloc[i]
            next_row = data.iloc[i + 1]
            
            # Skip points marked as not to render
            if not current_row['should_render']:
                continue
            
            # Transform coordinates for Gaode Maps
            current_lng, current_lat = self._transform_coordinates(
                current_row['longitude'], 
                current_row['latitude']
            )
            next_lng, next_lat = self._transform_coordinates(
                next_row['longitude'], 
                next_row['latitude']
            )
            
            # Calculate color based on severity percentage
            color = self._get_gradient_color(current_row['percentage_score'])
            
            # Convert timestamp to string
            timestamp_str = current_row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            
            # Create point feature
            point_feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [current_lng, current_lat]
                },
                'properties': {
                    'severity_score': current_row['severity_score'],
                    'percentage_score': current_row['percentage_score'],
                    'timestamp': timestamp_str,
                    'color': color
                }
            }
            point_features.append(point_feature)
            
            # Create line feature if time gap is less than 60 seconds
            time_diff = next_row['epoch_seconds'] - current_row['epoch_seconds']
            if time_diff < 60 and next_row['should_render']:
                line_feature = {
                    'type': 'Feature',
                    'geometry': {
                        'type': 'LineString',
                        'coordinates': [
                            [current_lng, current_lat],
                            [next_lng, next_lat]
                        ]
                    },
                    'properties': {
                        'severity_score': (current_row['severity_score'] + next_row['severity_score']) / 2,
                        'percentage_score': (current_row['percentage_score'] + next_row['percentage_score']) / 2,
                        'color': color
                    }
                }
                line_features.append(line_feature)
        
        # Add last point if it should be rendered
        last_row = data.iloc[-1]
        if last_row['should_render']:
            last_lng, last_lat = self._transform_coordinates(
                last_row['longitude'], 
                last_row['latitude']
            )
            last_color = self._get_gradient_color(last_row['percentage_score'])
            last_timestamp_str = last_row['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
            last_point_feature = {
                'type': 'Feature',
                'geometry': {
                    'type': 'Point',
                    'coordinates': [last_lng, last_lat]
                },
                'properties': {
                    'severity_score': last_row['severity_score'],
                    'percentage_score': last_row['percentage_score'],
                    'timestamp': last_timestamp_str,
                    'color': last_color
                }
            }
            point_features.append(last_point_feature)
        
        # Create GeoJSON collections
        points_geojson = {
            'type': 'FeatureCollection',
            'features': point_features
        }
        
        lines_geojson = {
            'type': 'FeatureCollection',
            'features': line_features
        }
        
        # Save data to files
        points_data_file = os.path.join(output_dir, "vibration_severity_line_map_gaode_points.json")
        lines_data_file = os.path.join(output_dir, "vibration_severity_line_map_gaode_lines.json")
        
        with open(points_data_file, 'w', encoding='utf-8') as f:
            json.dump(points_geojson, f, ensure_ascii=False, indent=2)
        
        with open(lines_data_file, 'w', encoding='utf-8') as f:
            json.dump(lines_geojson, f, ensure_ascii=False, indent=2)
        
        self.logger.info(f"Saved Gaode Maps line data to: {points_data_file} and {lines_data_file}")
        return points_data_file, lines_data_file, map_settings

    def _create_gaode_map_html(self, data_file: str, map_settings: dict, output_file: str) -> None:
        """
        Create HTML file for Gaode Maps visualization
        
        Parameters:
        - data_file: Path to the GeoJSON data file
        - map_settings: Map settings (center, zoom, bounds)
        - output_file: Path to save the HTML file
        """
        self.logger.info(f"Creating Gaode Maps HTML file: {output_file}")
        
        # Transform center coordinates for Gaode Maps
        center_lng, center_lat = self._transform_coordinates(
            map_settings['center']['lon'], 
            map_settings['center']['lat']
        )
        
        # Transform bounds coordinates for Gaode Maps
        south_west_lng, south_west_lat = self._transform_coordinates(
            map_settings['bounds']['west'], 
            map_settings['bounds']['south']
        )
        north_east_lng, north_east_lat = self._transform_coordinates(
            map_settings['bounds']['east'], 
            map_settings['bounds']['north']
        )
        
        # Generate HTML content
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Vibration Severity Map - Gaode</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <style>
        #map {{ height: 100vh; }}
        .legend {{ 
            padding: 6px 8px;
            font: 14px Arial, Helvetica, sans-serif;
            background: white;
            background: rgba(255, 255, 255, 0.8);
            box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
            border-radius: 5px;
            line-height: 24px;
            color: #555;
        }}
        .legend i {{ 
            width: 18px;
            height: 18px;
            float: left;
            margin-right: 8px;
            opacity: 0.7;
        }}
        .leaflet-control-button {{
            background-color: white;
            border: 2px solid rgba(0,0,0,0.2);
            border-radius: 4px;
            padding: 5px 10px;
            cursor: pointer;
            font-size: 14px;
            margin: 5px;
        }}
        .leaflet-control-button:hover {{
            background-color: #f4f4f4;
        }}
    </style>
</head>
<body>
    <div id="map"></div>
    <script>
        // Initialize map
        var map = L.map('map').setView([{center_lat}, {center_lng}], {map_settings['zoom']});
        
        // Add Gaode Maps tile layer
        L.tileLayer('https://webrd0{{s}}.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=8&x={{x}}&y={{y}}&z={{z}}', {{
            subdomains: ['1', '2', '3', '4'],
            attribution: '&copy; <a href="https://amap.com">高德地图</a>'
        }}).addTo(map);
        
        // Set map bounds
        var southWest = L.latLng({south_west_lat}, {south_west_lng});
        var northEast = L.latLng({north_east_lat}, {north_east_lng});
        var bounds = L.latLngBounds(southWest, northEast);
        map.fitBounds(bounds);
        
        // Load GeoJSON data
        fetch('{os.path.basename(data_file)}')
            .then(response => response.json())
            .then(geojsonData => {{
                // Create points layer
                var pointsLayer = L.geoJSON(geojsonData, {{
                    pointToLayer: function(feature, latlng) {{
                        return L.circleMarker(latlng, {{
                            radius: 6,
                            fillColor: feature.properties.color,
                            color: '#000',
                            weight: 1,
                            opacity: 0.1,
                            fillOpacity: 0.8
                        }});
                    }},
                    onEachFeature: function(feature, layer) {{
                        if (feature.properties) {{
                            var popupContent = '<b>Severity Score:</b> ' + feature.properties.severity_score.toFixed(2) + '<br>' +
                                             '<b>Percentage:</b> ' + feature.properties.percentage_score.toFixed(1) + '%<br>' +
                                             '<b>Time:</b> ' + feature.properties.timestamp;
                            layer.bindPopup(popupContent);
                        }}
                    }}
                }});
                
                // Create lines layer
                var linesLayer = L.geoJSON(geojsonData, {{
                    style: function(feature) {{
                        return {{
                            color: feature.properties.color,
                            weight: 4,
                            opacity: 1
                        }};
                    }},
                    onEachFeature: function(feature, layer) {{
                        if (feature.properties) {{
                            var popupContent = '<b>Severity Score:</b> ' + feature.properties.severity_score.toFixed(2) + '<br>' +
                                             '<b>Percentage:</b> ' + feature.properties.percentage_score.toFixed(1) + '%<br>' +
                                             '<b>Time:</b> ' + feature.properties.timestamp;
                            layer.bindPopup(popupContent);
                        }}
                    }}
                }});
                
                // Add layers to map
                linesLayer.addTo(map);
                pointsLayer.addTo(map);
                
                // Add toggle button
                var toggleButton = L.Control.extend({{
                    options: {{
                        position: 'topleft'
                    }},
                    onAdd: function(map) {{
                        var container = L.DomUtil.create('div', 'leaflet-bar leaflet-control');
                        var button = L.DomUtil.create('a', 'leaflet-control-button', container);
                        button.innerHTML = 'Toggle Points';
                        button.href = '#';
                        button.title = 'Toggle Points';
                        
                        L.DomEvent.on(button, 'click', function(e) {{
                            L.DomEvent.stopPropagation(e);
                            L.DomEvent.preventDefault(e);
                            if (map.hasLayer(pointsLayer)) {{
                                map.removeLayer(pointsLayer);
                            }} else {{
                                map.addLayer(pointsLayer);
                            }}
                        }});
                        
                        return container;
                    }}
                }});
                map.addControl(new toggleButton());
            }})
            .catch(error => console.error('Error loading data:', error));
        
        // Add legend
        var legend = L.control({{position: 'bottomright'}});
        legend.onAdd = function(map) {{
            var div = L.DomUtil.create('div', 'legend');
            var grades = [0, 50, 100];
            var colors = ['#00ff00', '#ffff00', '#ff0000'];
            
            div.innerHTML = '<b>Severity Percentage</b><br>';
            for (var i = 0; i < grades.length; i++) {{
                div.innerHTML +=
                    '<i style="background:' + colors[i] + '"></i> ' +
                    grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '%<br>' : '%+');
            }}
            return div;
        }};
        legend.addTo(map);
    </script>
</body>
</html>"""
        
        # Save HTML file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        self.logger.info(f"Gaode Maps HTML file created successfully: {output_file}")

    def _create_gaode_line_map_html(self, points_data_file: str, lines_data_file: str, 
                                  map_settings: dict, output_file: str) -> None:
        """
        Create HTML file for Gaode Maps line visualization
        
        Parameters:
        - points_data_file: Path to the points GeoJSON data file
        - lines_data_file: Path to the lines GeoJSON data file
        - map_settings: Map settings (center, zoom, bounds)
        - output_file: Path to save the HTML file
        """
        self.logger.info(f"Creating Gaode Maps line HTML file: {output_file}")
        
        # Transform center coordinates for Gaode Maps
        center_lng, center_lat = self._transform_coordinates(
            map_settings['center']['lon'], 
            map_settings['center']['lat']
        )
        
        # Transform bounds coordinates for Gaode Maps
        south_west_lng, south_west_lat = self._transform_coordinates(
            map_settings['bounds']['west'], 
            map_settings['bounds']['south']
        )
        north_east_lng, north_east_lat = self._transform_coordinates(
            map_settings['bounds']['east'], 
            map_settings['bounds']['north']
        )
        
        # Generate HTML content
        html = f"""<!DOCTYPE html>
<html>
<head>
    <title>Vibration Severity Line Map - Gaode</title>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.7.1/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.7.1/dist/leaflet.js"></script>
    <style>
        #map {{ height: 100vh; }}
        .legend {{ 
            padding: 6px 8px;
            font: 14px Arial, Helvetica, sans-serif;
            background: white;
            background: rgba(255, 255, 255, 0.8);
            box-shadow: 0 0 15px rgba(0, 0, 0, 0.2);
            border-radius: 5px;
            line-height: 24px;
            color: #555;
        }}
        .legend i {{ 
            width: 18px;
            height: 18px;
            float: left;
            margin-right: 8px;
            opacity: 0.7;
        }}
    </style>
</head>
<body>
    <div id="map"></div>
    <script>
        // Initialize map
        var map = L.map('map').setView([{center_lat}, {center_lng}], {map_settings['zoom']});
        
        // Add Gaode Maps tile layer
        L.tileLayer('https://webrd0{{s}}.is.autonavi.com/appmaptile?lang=zh_cn&size=1&scale=1&style=8&x={{x}}&y={{y}}&z={{z}}', {{
            subdomains: ['1', '2', '3', '4'],
            attribution: '&copy; <a href="https://amap.com">高德地图</a>'
        }}).addTo(map);
        
        // Set map bounds
        var southWest = L.latLng({south_west_lat}, {south_west_lng});
        var northEast = L.latLng({north_east_lat}, {north_east_lng});
        var bounds = L.latLngBounds(southWest, northEast);
        map.fitBounds(bounds);
        
        // Load GeoJSON data
        fetch('{os.path.basename(lines_data_file)}')
            .then(response => response.json())
            .then(linesData => {{
                // Create lines layer
                var linesLayer = L.geoJSON(linesData, {{
                    style: function(feature) {{
                        return {{
                            color: feature.properties.color,
                            weight: 7,
                            opacity: 1
                        }};
                    }},
                    onEachFeature: function(feature, layer) {{
                        if (feature.properties) {{
                            var popupContent = '<b>Average Severity Score:</b> ' + feature.properties.severity_score.toFixed(2) + '<br>' +
                                             '<b>Average Percentage:</b> ' + feature.properties.percentage_score.toFixed(1) + '%';
                            layer.bindPopup(popupContent);
                        }}
                    }}
                }}).addTo(map);
            }})
            .catch(error => console.error('Error loading data:', error));
        
        // Add legend
        var legend = L.control({{position: 'bottomright'}});
        legend.onAdd = function(map) {{
            var div = L.DomUtil.create('div', 'legend');
            var grades = [0, 50, 100];
            var colors = ['#00ff00', '#ffff00', '#ff0000'];
            
            div.innerHTML = '<b>Severity Percentage</b><br>';
            for (var i = 0; i < grades.length; i++) {{
                div.innerHTML +=
                    '<i style="background:' + colors[i] + '"></i> ' +
                    grades[i] + (grades[i + 1] ? '&ndash;' + grades[i + 1] + '%<br>' : '%+');
            }}
            return div;
        }};
        legend.addTo(map);
    </script>
</body>
</html>"""
        
        # Save HTML file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html)
        
        self.logger.info(f"Gaode Maps line HTML file created successfully: {output_file}")

    def create_gaode_map(self, data: pd.DataFrame, output_dir: Optional[str] = None) -> None:
        """
        Create an interactive map visualization using Gaode Maps
        
        Parameters:
        - data: DataFrame containing GPS and severity data
        - output_dir: Directory to save the output (if None, uses default Results directory)
        """
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Results")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
        
        # Generate map data and save to file
        data_file, map_settings = self._generate_gaode_map_data(data, output_dir)
        
        # Create HTML file
        output_file = os.path.join(output_dir, "vibration_severity_map_gaode.html")
        self._create_gaode_map_html(data_file, map_settings, output_file)
        
        self.logger.info(f"Gaode Maps visualization created successfully: {output_file}")

    def create_gaode_line_map(self, data: pd.DataFrame, output_dir: Optional[str] = None) -> None:
        """
        Create an interactive line map visualization using Gaode Maps
        
        Parameters:
        - data: DataFrame containing GPS and severity data
        - output_dir: Directory to save the output (if None, uses default Results directory)
        """
        if output_dir is None:
            output_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "Results")
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
        
        # Generate map data and save to files
        points_data_file, lines_data_file, map_settings = self._generate_gaode_line_map_data(data, output_dir)
        
        # Create HTML file
        output_file = os.path.join(output_dir, "vibration_severity_line_map_gaode.html")
        self._create_gaode_line_map_html(points_data_file, lines_data_file, map_settings, output_file)
        
        self.logger.info(f"Gaode Maps line visualization created successfully: {output_file}")

def main():
    # Create visualizer instance with debug mode enabled
    visualizer = LeafletMapVisualizer(debug=True)
    
    try:
        # Get combined data
        data = visualizer.get_combined_data()
        
        # Create map visualizations (Deprecated)
        # visualizer.create_map(data, output_dir="Results")
        # visualizer.create_line_map(data, output_dir="Results")
        
        # Create Gaode Maps visualizations
        visualizer.create_gaode_map(data, output_dir="Results")
        visualizer.create_gaode_line_map(data, output_dir="Results")
        
        print("\nMap visualization completed successfully!")
        print("Check the Results directory for the output files:")
        # print("1. vibration_severity_map_leaflet.html (points map with OpenStreetMap)")
        # print("2. vibration_severity_line_map_leaflet.html (line map with OpenStreetMap)")
        print("3. vibration_severity_map_gaode.html (points map with Gaode Maps)")
        print("4. vibration_severity_line_map_gaode.html (line map with Gaode Maps)")
        print("Open these files in a web browser to view the interactive maps.")
        
    except Exception as e:
        print(f"Error during visualization: {str(e)}")
        raise

if __name__ == "__main__":
    main() 