# Create directories
@(5000,5001,5002) | ForEach-Object { 
    New-Item -ItemType Directory -Name "uploads_$_" -Force
    New-Item -ItemType Directory -Name "processed_$_" -Force  
    New-Item -ItemType Directory -Name "index_$_" -Force
}

# Start Node 1
Start-Process powershell -ArgumentList "-NoExit -Command `"`$env:UPLOAD_FOLDER='uploads_5000'; `$env:PROCESSED_FOLDER='processed_5000'; `$env:INDEX_FOLDER='index_5000'; python main.py`""
Start-Sleep 3

# Start Node 2  
Start-Process powershell -ArgumentList "-NoExit -Command `"`$env:UPLOAD_FOLDER='uploads_5001'; `$env:PROCESSED_FOLDER='processed_5001'; `$env:INDEX_FOLDER='index_5001'; python main.py`""
Start-Sleep 3

# Start Node 3
Start-Process powershell -ArgumentList "-NoExit -Command `"`$env:UPLOAD_FOLDER='uploads_5002'; `$env:PROCESSED_FOLDER='processed_5002'; `$env:INDEX_FOLDER='index_5002'; python main.py`""

Write-Host "🎉 Nodes started! Access: http://localhost:5000, 5001, 5002" -ForegroundColor Green