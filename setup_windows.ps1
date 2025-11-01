# setup_windows.ps1
Write-Host "Setting up Decentralized Document Processor for Windows..." -ForegroundColor Cyan

# Check Python version
$pythonVersion = python --version
if (-not $pythonVersion.Contains("3.")) {
    Write-Host "Python 3.x required. Please install Python 3.8+ first." -ForegroundColor Red
    exit 1
}

# Create virtual environment
Write-Host "Creating virtual environment..." -ForegroundColor Yellow
python -m venv venv

# Activate virtual environment
Write-Host "Activating virtual environment..." -ForegroundColor Yellow
.\venv\Scripts\Activate.ps1

# Install dependencies
Write-Host "Installing Python dependencies..." -ForegroundColor Yellow
pip install flask flask-socketio python-socketio eventlet PyPDF2 python-docx pytesseract pillow spacy nltk whoosh requests

# Download spaCy model
Write-Host "Downloading spaCy model..." -ForegroundColor Yellow
python -m spacy download en_core_web_sm

# Create necessary directories
Write-Host "Creating directories..." -ForegroundColor Yellow
@("uploads", "processed", "search_index") | ForEach-Object { New-Item -ItemType Directory -Path $_ -Force }

Write-Host "Setup completed successfully!" -ForegroundColor Green
Write-Host "To run the application:" -ForegroundColor Cyan
Write-Host "   .\venv\Scripts\Activate.ps1" -ForegroundColor White
Write-Host "   python main.py" -ForegroundColor White
Write-Host "Then open: http://localhost:5000" -ForegroundColor White