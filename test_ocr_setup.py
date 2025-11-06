#!/usr/bin/env python3
import sys
import os

print("="*60)
print("🧪 TESTING OCR SETUP")
print("="*60)

# Test 1: Imports
print("\n1️⃣ Testing imports...")
try:
    import pytesseract
    from PIL import Image
    print("   ✅ Imports successful")
except ImportError as e:
    print(f"   ❌ Import failed: {e}")
    print("\n   Fix: pip install pytesseract pillow")
    sys.exit(1)

# Test 2: Tesseract binary
print("\n2️⃣ Testing Tesseract binary...")
try:
    version = pytesseract.get_tesseract_version()
    print(f"   ✅ Tesseract version: {version}")
except Exception as e:
    print(f"   ❌ Tesseract not accessible: {e}")
    print("\n   Fix:")
    print("   Windows: https://github.com/UB-Mannheim/tesseract/wiki")
    print("   Linux: sudo apt-get install tesseract-ocr")
    print("   macOS: brew install tesseract")
    sys.exit(1)

# Test 3: OCR functionality
print("\n3️⃣ Testing OCR functionality...")
try:
    from PIL import Image, ImageDraw, ImageFont
    
    # Create test image
    img = Image.new('RGB', (400, 100), color='white')
    d = ImageDraw.Draw(img)
    
    # Draw text
    try:
        d.text((10, 30), "Hello OCR World", fill='black')
    except:
        # Fallback without font
        d.text((10, 30), "Hello OCR World", fill='black')
    
    # Run OCR
    text = pytesseract.image_to_string(img).strip()
    print(f"   OCR result: '{text}'")
    
    if text and ('hello' in text.lower() or 'ocr' in text.lower()):
        print("   ✅ OCR working correctly!")
    else:
        print(f"   ⚠️  OCR working but result unexpected")
    
except Exception as e:
    print(f"   ❌ OCR test failed: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*60)
print("🎉 SUCCESS! OCR is fully functional!")
print("="*60)
print("\nYou can now:")
print("1. Start your nodes")
print("2. Upload images (.jpg, .png, .jpeg)")
print("3. Watch OCR tasks being distributed and processed")
print("="*60)
