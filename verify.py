# start_ocr_node.py
import os
import subprocess
import time
import requests

def start_ocr_node_and_verify():
    """Start OCR node and verify it's working"""
    print("🚀 STARTING OCR SPECIALIST NODE...")
    print("=" * 50)
    
    # Set environment variable and start node
    env = os.environ.copy()
    env['FLASK_RUN_PORT'] = '5001'
    
    try:
        # Start the OCR node
        process = subprocess.Popen(
            ['python', 'main.py'],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        print("✅ OCR node started on port 5001")
        print("   Waiting for node to initialize...")
        time.sleep(5)
        
        # Check if node is running
        try:
            response = requests.get('http://localhost:5001/api/stats', timeout=5)
            if response.status_code == 200:
                print("✅ OCR node is responding on port 5001")
            else:
                print("⚠️  OCR node started but not responding properly")
        except:
            print("⚠️  OCR node starting (may take a few more seconds)")
        
        print("\n🎯 Now check the main terminal - you should see:")
        print("   '✅ Discovered peer: ... Type: ocr_specialist'")
        print("\n📝 You can now upload image files for OCR processing!")
        
        # Keep the process running
        print("\n⏳ OCR node is running... Press Ctrl+C to stop it")
        try:
            process.wait()
        except KeyboardInterrupt:
            print("\n🛑 Stopping OCR node...")
            process.terminate()
            
    except Exception as e:
        print(f"❌ Failed to start OCR node: {e}")

if __name__ == "__main__":
    start_ocr_node_and_verify()