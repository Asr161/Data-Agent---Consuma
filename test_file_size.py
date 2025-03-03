import os

def get_file_size(file_path):
    """
    Returns the size of the given file in MB.
    
    Args:
        file_path (str): Path to the JSON file.
    
    Returns:
        float: File size in MB.
    """
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return None

    file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert bytes to MB
    return round(file_size, 2)

# Example Usage:
file_path = "sample_data_50K.json"  # Change to your file name
file_size = get_file_size(file_path)

if file_size is not None:
    print(f"File size of '{file_path}': {file_size} MB")
