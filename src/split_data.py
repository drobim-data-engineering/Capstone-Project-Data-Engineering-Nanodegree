import glob
from pathlib import Path
import os
from fsplit.filesplit import FileSplit

def collect_files(root_path):

    file_path_list = []

    # Collect each filepath
    for root, dirs, files in os.walk(root_path):
        for file in files:
            if file.endswith(".csv"):
                file_path_list.extend(glob.glob(os.path.join(root, file)))

    return file_path_list

def create_dir(file_path, output_path):

    # Extract File Path As List
    path_parts = Path(file_path).parts
    path_position = len(path_parts) - 2

    # Extract Subdirectory based on File Path
    export_path = output_path + path_parts[path_position]

    # Create Output Directory
    Path(export_path).mkdir(exist_ok=True)

    return export_path

def split_file_stats(f, s, c):
    print(f"File Name: {f}, Size: {s}, Row Count: {c}")

def split_file(root_path, split_size_mb, output_path):

    # Converts Mb to Bytes
    split_size_mb = split_size_mb * 1048576

    # Collect files from root path
    file_path = collect_files(root_path)

    for file in file_path:

        # Create Output Directory
        export_path = create_dir(file, output_path)

        # Reads File
        fs = FileSplit(file=file, splitsize=split_size_mb, output_dir=export_path)

        # Splits file
        fs.split(include_header=True,callback=split_file_stats)

def main():

    split_file('../data/raw/', 128, '../data/split/')

if __name__ == '__main__':
    main()