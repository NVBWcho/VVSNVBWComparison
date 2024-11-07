import os

def getRootDirectory():
    
    current_file_path = os.path.abspath(__file__)

    # Get the directory of the current file
    current_directory = os.path.dirname(current_file_path)

    # Get the parent directory
    print(current_directory)
    return current_directory


