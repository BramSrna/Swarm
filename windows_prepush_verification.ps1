# Go to the home directory of the project (location where this script is)
Set-Location -Path $PSScriptRoot

# Run the linters
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
flake8 . --count --exit-zero --max-complexity=10 --max-line-length=127 --statistics

# Run the unittests
pytest