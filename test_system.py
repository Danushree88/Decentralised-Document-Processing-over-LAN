import sys
import os

print("=== SYSTEM DIAGNOSTICS ===")

# Test basic imports
print("\n1. Testing Basic Imports:")
try:
    from flask import Flask
    print("✅ Flask")
except ImportError as e:
    print(f"❌ Flask: {e}")

try:
    from flask_socketio import SocketIO
    print("✅ Flask-SocketIO")
except ImportError as e:
    print(f"❌ Flask-SocketIO: {e}")

try:
    import PyPDF2
    print("✅ PyPDF2")
except ImportError as e:
    print(f"❌ PyPDF2: {e}")

try:
    import docx
    print("✅ python-docx")
except ImportError as e:
    print(f"❌ python-docx: {e}")

try:
    import spacy
    print("✅ spaCy")
except ImportError as e:
    print(f"❌ spaCy: {e}")

try:
    import nltk
    print("✅ NLTK")
except ImportError as e:
    print(f"❌ NLTK: {e}")

try:
    import whoosh
    print("✅ Whoosh")
except ImportError as e:
    print(f"❌ Whoosh: {e}")

# Test our modules
print("\n2. Testing Our Modules:")
try:
    from config import Config, get_local_ip
    print("✅ Config")
    print(f"   Local IP: {get_local_ip()}")
except Exception as e:
    print(f"❌ Config: {e}")

try:
    from document_processor import DocumentProcessor
    processor = DocumentProcessor()
    print("✅ DocumentProcessor")
except Exception as e:
    print(f"❌ DocumentProcessor: {e}")

try:
    from search_index import SearchIndex
    search_idx = SearchIndex()
    print("✅ SearchIndex")
except Exception as e:
    print(f"❌ SearchIndex: {e}")

try:
    from peer_node import PeerNode
    peer = PeerNode()
    print("✅ PeerNode")
    peer.stop()
except Exception as e:
    print(f"❌ PeerNode: {e}")

try:
    from task_manager import TaskManager
    print("✅ TaskManager")
except Exception as e:
    print(f"❌ TaskManager: {e}")

print("\n3. Directory Structure:")
directories = ['uploads', 'processed', 'search_index', 'templates', 'static']
for dir_name in directories:
    if os.path.exists(dir_name):
        print(f"✅ {dir_name}/")
    else:
        print(f"❌ {dir_name}/ (MISSING)")

print("\n=== DIAGNOSTICS COMPLETE ===")