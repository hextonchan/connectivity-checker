Write-Output "Initalize Python venv..."
python -m venv ./venv
Write-Output "Python venv created."

Write-Output "Load requirements.txt if exists."
./venv/Scripts/Activate.ps1
try{
    pip install --ignore-installed -r requirements.txt
}
catch{
    Write-Output "requirement.txt not found, skipped."
}
deactivate