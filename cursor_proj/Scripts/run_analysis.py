import os
import sys
from vibration_analyzer import VibrationAnalyzer

class VibrationAnalysisRunner:
    def __init__(self, data_dir="VibData"):
        """
        Initialize the VibrationAnalysisRunner
        
        Parameters:
        - data_dir: Directory containing vibration data files
        """
        self.data_dir = data_dir
        
    def list_data_files(self):
        """
        List all txt files in the data directory
        """
        files = [f for f in os.listdir(self.data_dir) if f.endswith('.txt')]
        return files
    
    def run_analysis(self):
        """
        Run the vibration analysis workflow
        """
        # Get available data files
        data_files = self.list_data_files()
        
        if not data_files:
            print("No data files found in the VibData directory")
            return
        
        print("Available vibration data files:")
        for i, file in enumerate(data_files, 1):
            file_path = os.path.join(self.data_dir, file)
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert to MB
            print(f"{i}. {file} ({file_size:.2f} MB)")
        
        # Let user select a file
        try:
            choice = int(input("\nSelect a file number to analyze (or 0 to exit): "))
            if choice == 0:
                print("Exiting...")
                return
            
            if choice < 1 or choice > len(data_files):
                print(f"Invalid choice. Please select a number between 1 and {len(data_files)}")
                return
            
            selected_file = data_files[choice - 1]
            file_path = os.path.join(self.data_dir, selected_file)
            
            print(f"\nAnalyzing: {selected_file}")
            
            # Create analyzer instance and run analysis
            analyzer = VibrationAnalyzer(file_path)
            analyzer.read_data()
            analyzer.analyze_data_by_second()
            
            # Display first few rows of processed data
            print("\nProcessed data (first 5 rows):")
            print(analyzer.grouped_data.head(5))
            
            # Save all results
            output_dir = analyzer.save_results()
            print(f"\nResults saved to: {output_dir}")
            
        except ValueError:
            print("Invalid input. Please enter a number.")
        except Exception as e:
            print(f"An error occurred: {e}")

def main():
    # Create runner instance and execute analysis
    runner = VibrationAnalysisRunner()
    runner.run_analysis()

if __name__ == "__main__":
    main() 