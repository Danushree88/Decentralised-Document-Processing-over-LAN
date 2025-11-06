@echo off
echo ========================================
echo Installing OCR Dependencies
echo ========================================

echo.
echo Step 1: Installing Python packages...
pip install pytesseract
pip install pillow
pip install pdf2image

echo.
echo Step 2: Tesseract OCR needs to be installed manually
echo.
echo Please download and install Tesseract from:
echo https://github.com/UB-Mannheim/tesseract/wiki
echo.
echo Default installation path: C:\Program Files\Tesseract-OCR
echo.
pause

echo.
echo ========================================
echo Installation complete!
echo Please restart your nodes.
echo ========================================
pause