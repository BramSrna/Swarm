# Go to the home directory of the project (location where this script is)
Set-Location -Path $PSScriptRoot

# Run the linters
flake8 --ignore=E501 .

# Run the unittests
pytest