import sqlite3
import os
from datetime import datetime
import pandas as pd
import json
import time
from database_manager import DatabaseManager
from gps_data_manager import GPSDataManager
from batch_analysis import BatchAnalysisRunner

def combineDataTest():
    """
    Get GPS data and severity scores combined
    
    Returns:
    - DataFrame containing combined GPS and severity data
    """
    db = DatabaseManager()
    # Get all GPS data with timeout
    with sqlite3.connect(db.db_path, timeout=30) as conn:
        gps_data = pd.read_sql_query('''
            SELECT * FROM gps_data 
            ORDER BY epoch_seconds
        ''', conn)
        
        # Get all analysis results
        analysis_results = pd.read_sql_query('''
            SELECT epoch_seconds, severity_score 
            FROM analysis_results
            ORDER BY epoch_seconds
        ''', conn)
    
    print(f"gps_data length: {len(gps_data.keys())}")
    print(f"analysis_results length: {len(analysis_results.keys())}")
    print(f"gps_data: {gps_data}")
    print(f"analysis_results: {analysis_results}")

    # Find maximum severity score
    max_severity = analysis_results['severity_score'].max()
    
    combined_data = {}
    # Merge GPS data with analysis results
    gps_epoch_set = set(gps_data['epoch_seconds'])
    print(f"len gps_epoch_set: {len(gps_epoch_set)}")
    # print(f"gps_epoch_set: {gps_epoch_set}")
    analysis_epoch_set = set(analysis_results['epoch_seconds'])
    print(f"len analysis_epoch_set: {len(analysis_epoch_set)}")
    in_set = set()
    out_set = set()
    for gps_epochsecond in gps_epoch_set:
        if gps_epochsecond in analysis_epoch_set:
            in_set.add(gps_epochsecond)
        else:
            out_set.add(gps_epochsecond)
    print(f"len in_set: {len(in_set)}")
    print(f"len out_set: {len(out_set)}")
    print(f"out_set: {out_set}")
    # print(f"analysis_epoch_set: {analysis_epoch_set}")
    for ana_epochsecond in analysis_results['epoch_seconds']:
        if ana_epochsecond in gps_epoch_set:
            combined_data[ana_epochsecond] = {
                'latitude': gps_data.loc[gps_data['epoch_seconds'] == ana_epochsecond, 'latitude'].values[0],
                'longitude': gps_data.loc[gps_data['epoch_seconds'] == ana_epochsecond, 'longitude'].values[0],
                'severity_score': analysis_results.loc[analysis_results['epoch_seconds'] == ana_epochsecond, 'severity_score'].values[0]
            }
    print(f"combined_data length: {len(combined_data.keys())}")
    print(f"combined_data: {combined_data}")


    
    # # Calculate percentage scores
    # combined_data['percentage_score'] = (combined_data['severity_score'] / max_severity) * 100
    # combined_data['percentage_score'] = combined_data['percentage_score'].clip(0, 100)
    
    return combined_data

def batchAnalysisTest():
    runner = BatchAnalysisRunner(data_dir="VibData", debug=True)
    runner.run_batch_analysis()

def deleteGpsData():
    db = DatabaseManager()
    db.clear_gps_data()

def processGpsData():
    gps_manager = GPSDataManager(debug=True)
    gps_manager.process_gps_data()

def deleteGpsResults():
    db = DatabaseManager()
    db.clear_gps_results()

def recordGpsData():
    gps_manager = GPSDataManager(debug=True)
    gps_manager.process_directory("TracksData")

def main():
    batchAnalysisTest()
    recordGpsData()
    processGpsData()
    # deleteGpsData()
    # deleteGpsResults()
    pass

if __name__ == "__main__":
    main() 