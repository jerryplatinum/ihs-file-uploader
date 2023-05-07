# File Comparison and Validation

This Python script is designed to validate and compare data files (CSV, XLS, and XLSX) against template files in a central repository. The purpose is to ensure that the headers and sheet names match before the files are used in a downstream application.

## How it works

1. The script searches for all CSV, XLS, and XLSX files in a specified source folder.
2. It then finds the corresponding template file in the template folder.
3. The contents of the data file and template file are compared, checking for header and sheet name mismatches.
4. If there are any mismatches, the file is moved to a quarantined folder, and an error log is generated.
5. For successful files, they are copied to a production folder and an archive folder with a timestamp appended to the file name.
6. A summary report of all scanned files, their status, and relevant metadata is generated in both TXT and CSV formats.

## Requirements

* Python 3.6 or higher
* openpyxl library (for processing Excel files)

## Usage

1. Set the paths for the following directories in the `__main__` block:
    - Source folder
    - Template folder
    - Log folder
    - Quarantined folder
    - Production folder
    - Archive folder
2. Run the script with `python scan_folders.py`.

## Notes

* The script will retry twice with a 5-minute wait time if there are any network issues or other failures.
* The code is well-documented with comments.
* The script uses only one external library, openpyxl, for processing Excel files.
