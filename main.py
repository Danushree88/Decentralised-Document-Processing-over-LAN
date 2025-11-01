#!/usr/bin/env python3
"""
Decentralized Document Processing & Indexing System
Main entry point
"""

import logging
import sys
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('doc_processor.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def main():
    """Main entry point"""
    print("🚀 Decentralized Document Processing System")
    print("=" * 50)
    
    # Check dependencies
    try:
        import flask
        import spacy
        import pytesseract
        import PyPDF2
        print("✅ All dependencies found")
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Please run: pip install -r requirements.txt")
        return
    
    # Start the dashboard
    from dashboard import app, socketio
    
    print("\n📊 Starting web dashboard...")
    print("💡 The system will automatically discover other nodes on the LAN")
    print("🌐 Dashboard will be available at: http://localhost:5000")
    
    try:
        socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    except Exception as e:
        print(f"❌ Error starting server: {e}")

if __name__ == "__main__":
    main()