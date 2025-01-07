from consensus_economics.paths import Paths
import os


def main():
    # Get the processed xlsx path
    processed_xlsx_path = Paths().processed / "xlsx"
    
    # Iterate through all files in the directory
    for root, _, files in os.walk(processed_xlsx_path):
        for filename in files:
            if filename.endswith('.xlsx'):
                # Check if filename contains any " N" where N is an integer
                for i in range(1, 10):  # Checking numbers 1-9
                    if f" {i}" in filename:
                        old_path = os.path.join(root, filename)
                        # Create new filename by removing " N"
                        new_filename = filename.replace(f" {i}", "")
                        new_path = os.path.join(root, new_filename)
                        
                        # If the target file already exists, remove it
                        if os.path.exists(new_path):
                            os.remove(new_path)
                        
                        # Rename the file
                        os.rename(old_path, new_path)
                        break  # Break after first match since we've renamed the file

if __name__ == "__main__":
    main()
