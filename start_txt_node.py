# start_txt_node.py
"""
Start a TXT-only processing node on port 5000
"""
import os
import sys

os.environ['NODE_TYPE'] = 'txt'
os.environ['FLASK_RUN_PORT'] = '5000'

# Import and run the main app
from app import app, socketio, peer_node, local_ip, DEFAULT_PORT

if __name__ == '__main__':
    print("\n" + "="*60)
    print(" TXT-ONLY NODE")
    print("="*60)
    print(f" Node Type: TXT")
    print(f" Port: 5000")
    print(f" Supported: .txt files only")
    print(f" URL: http://{local_ip}:5000")
    print("="*60 + "\n")
    
    try:
        socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
    except Exception as e:
        print(f"Error: {e}")


# start_pdf_node.py
"""
Start a PDF-only processing node on port 5001
"""
import os
import sys

os.environ['NODE_TYPE'] = 'pdf'
os.environ['FLASK_RUN_PORT'] = '5001'

# Import and run the main app
from app import app, socketio, peer_node, local_ip, DEFAULT_PORT

if __name__ == '__main__':
    print("\n" + "="*60)
    print(" PDF-ONLY NODE")
    print("="*60)
    print(f" Node Type: PDF")
    print(f" Port: 5001")
    print(f" Supported: .pdf files only")
    print(f" URL: http://{local_ip}:5001")
    print("="*60 + "\n")
    
    try:
        socketio.run(app, host='0.0.0.0', port=5001, debug=False, allow_unsafe_werkzeug=True)
    except Exception as e:
        print(f"Error: {e}")


# start_image_node.py
"""
Start an IMAGE-only processing node on port 5002
"""
import os
import sys

os.environ['NODE_TYPE'] = 'image'
os.environ['FLASK_RUN_PORT'] = '5002'

# Import and run the main app
from app import app, socketio, peer_node, local_ip, DEFAULT_PORT

if __name__ == '__main__':
    print("\n" + "="*60)
    print(" IMAGE-ONLY NODE (OCR)")
    print("="*60)
    print(f" Node Type: IMAGE")
    print(f" Port: 5002")
    print(f" Supported: .jpg, .png, .jpeg, .bmp, .tiff")
    print(f" URL: http://{local_ip}:5002")
    print("="*60 + "\n")
    
    try:
        socketio.run(app, host='0.0.0.0', port=5002, debug=False, allow_unsafe_werkzeug=True)
    except Exception as e:
        print(f"Error: {e}")


# start_document_node.py
"""
Start a DOCUMENT-only processing node on port 5003
"""
import os
import sys

os.environ['NODE_TYPE'] = 'document'
os.environ['FLASK_RUN_PORT'] = '5003'

# Import and run the main app
from app import app, socketio, peer_node, local_ip, DEFAULT_PORT

if __name__ == '__main__':
    print("\n" + "="*60)
    print(" DOCUMENT-ONLY NODE")
    print("="*60)
    print(f" Node Type: DOCUMENT")
    print(f" Port: 5003")
    print(f" Supported: .doc, .docx files only")
    print(f" URL: http://{local_ip}:5003")
    print("="*60 + "\n")
    
    try:
        socketio.run(app, host='0.0.0.0', port=5003, debug=False, allow_unsafe_werkzeug=True)
    except Exception as e:
        print(f"Error: {e}")


# start_multi_node.py
"""
Start an AUTO node that handles all file types on port 5004
"""
import os
import sys

os.environ['NODE_TYPE'] = 'auto'
os.environ['FLASK_RUN_PORT'] = '5004'

# Import and run the main app
from app import app, socketio, peer_node, local_ip, DEFAULT_PORT

if __name__ == '__main__':
    print("\n" + "="*60)
    print(" MULTI-PURPOSE NODE (AUTO)")
    print("="*60)
    print(f" Node Type: AUTO")
    print(f" Port: 5004")
    print(f" Supported: All file types")
    print(f" URL: http://{local_ip}:5004")
    print("="*60 + "\n")
    
    try:
        socketio.run(app, host='0.0.0.0', port=5004, debug=False, allow_unsafe_werkzeug=True)
    except Exception as e:
        print(f"Error: {e}")