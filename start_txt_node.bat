REM start_txt_node.bat
@echo off
echo ========================================
echo Starting TXT-ONLY Node (Port 5000)
echo ========================================
set NODE_TYPE=txt
set FLASK_RUN_PORT=5000
python start_txt_node.py
pause

REM ========================================

REM start_pdf_node.bat
@echo off
echo ========================================
echo Starting PDF-ONLY Node (Port 5001)
echo ========================================
set NODE_TYPE=pdf
set FLASK_RUN_PORT=5001
python start_pdf_node.py
pause

REM ========================================

REM start_image_node.bat
@echo off
echo ========================================
echo Starting IMAGE-ONLY Node (Port 5002)
echo ========================================
set NODE_TYPE=image
set FLASK_RUN_PORT=5002
python start_image_node.py
pause

REM ========================================

REM start_document_node.bat
@echo off
echo ========================================
echo Starting DOCUMENT-ONLY Node (Port 5003)
echo ========================================
set NODE_TYPE=document
set FLASK_RUN_PORT=5003
python start_document_node.py
pause

REM ========================================

REM start_multi_node.bat
@echo off
echo ========================================
echo Starting MULTI-PURPOSE Node (Port 5004)
echo ========================================
set NODE_TYPE=auto
set FLASK_RUN_PORT=5004
python start_multi_node.py
pause

REM ========================================

REM start_specialized_cluster.bat
@echo off
echo ========================================
echo Starting Specialized Node Cluster
echo ========================================
echo.
echo Starting TXT Node on port 5000...
start cmd /k "set NODE_TYPE=txt && set FLASK_RUN_PORT=5000 && python start_txt_node.py"

timeout /t 3

echo Starting PDF Node on port 5001...
start cmd /k "set NODE_TYPE=pdf && set FLASK_RUN_PORT=5001 && python start_pdf_node.py"

echo.
echo ========================================
echo Cluster started!
echo TXT Node: http://localhost:5000
echo PDF Node: http://localhost:5001
echo ========================================
pause