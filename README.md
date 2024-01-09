# iSAMS-OA Analyzer Script

## Introduction
Welcome to the iSAMS-OA Analyzer Script! This utility is designed to streamline your data analysis tasks by efficiently comparing and analyzing data from iSAMS and OA (OpenApply) systems.

**Latest Version: 1.2 (Release Date TBD)**

## Features

### Version 1.0 (Released Dec 25, 2023)
- **Performance**: Utilizes a vectorized approach for faster, more accurate results.
- **Visual Enhancements**: Includes color coding for iSAMS and OA data.
- **Additional Data Sheets**: Sheets added to highlight rows with discrepancies.

### Version 1.1 (Released Jan 8, 2024)
- **Improved Performance**: Enhanced logic in matching for faster and more flexible operations.
- **Parent/Guardian Dynamicity**: Ability to handle varying numbers of parents/guardians.
- **OA's Priority**: Added functionality to prioritize matching with OA's enrolled students.

### Version 1.2 (Release Date TBD)
- **Enhanced Parents Comparison**: Improved algorithm for comparing the parents of the students.
- **Updated Execution Method**: Transitioned to using Poetry and Click for easier distribution and execution.

## Getting Started

### Prerequisites
- Ensure your computer is set up for development. For MacOS users, see [Setting Up Your MacOS Development Environment: A Guide](https://fariaedu.atlassian.net/browse/DT-10313).
- Python installed on your system. (Python 3 recommended)
- Required Python libraries: pandas, openpyxl, jinja.

### Installation for Version 1.2
For the latest version (1.2), we've transitioned to using Poetry and Click for a smoother setup and execution process:
1. Install `pipx` as described here: [Pipx Installation Guide](https://github.com/pypa/pipx#readme).
2. Execute the following command in the terminal to install the script:
   ```pipx install git+https://github.com/chrele/iSAMS-OASync.git```

### Usage
For versions 1.0 and 1.1:
1. Place all the script and data files in the same folder.
2. Open Terminal or Command Prompt.
3. Navigate to the script's folder.
4. Run the script using the command:
   ```python iSAMS-OA.py <school_name>```
   Example:
   ```python iSAMS-OA.py 'Cologne International School'```

_Note: Use `python3`, `py`, or `py3` if your system is configured differently._

For version 1.2:
1. After installation, simply run the script from any location (make sure that all necessary files are in the same directory as your terminal execution) in the terminal:
   ```weather school --name='<school_name>'```
   Example:
   ```weather school --name='Cologne International School'```

Check the results in the generated output file named `isams_oa_analysis_<school_name>_<datetime>.xlsx`, which includes multiple sheets for different types of data comparison.

## Important Notes
- **File Location**: Keep all script, .xlsx, and .csv files in the same directory.
- **Grade-Year Mapping Format**: Ensure the grade-year mapping file follows the specified header format.
- **File Naming Convention**:
- iSAMS data: `iSAMS (school_name).xlsx`
- OA data: `OA (school_name).xlsx`
- Grade mapping: `grade_year_mapping (school_name).csv`
- **Python and Libraries**: Install required libraries using `pip install pandas openpyxl jinja`. Use `pip3` if `pip` isn't recognized.

## Troubleshooting Common Errors

### Handling 'Module Not Found' Errors
If you encounter a `ModuleNotFoundError`, it indicates a missing required library. To resolve:
1. Open Terminal or Command Prompt.
2. Install the missing library:
   ```pip install <missing_module>```
   Example:
   ```pip install jinja2```
3. Rerun the script after installation.

For further assistance, please contact [Christopher Andrew](mailto:christopher.andrew@fariaedu.com).

