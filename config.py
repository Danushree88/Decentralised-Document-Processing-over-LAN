import socket
import uuid

def get_local_ip():
    """Get local IP address - Windows compatible without netifaces"""
    try:
        # Method 1: Connect to external server to get local IP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            return ip
    except:
        try:
            # Method 2: Get by hostname
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            if ip and ip != '127.0.0.1':
                return ip
        except:
            pass
        
        # Fallback to localhost
        return '127.0.0.1'

class Config:
    # Network Configuration
    BROADCAST_PORT = 8888
    TASK_PORT = 8889
    FILE_PORT = 8890
    BROADCAST_ADDR = "255.255.255.255"
    HEARTBEAT_INTERVAL = 5  # seconds
    TASK_TIMEOUT = 30  # seconds
    
    # Node Capabilities
    CAPABILITIES = {
        'ocr': False,  # Set to True if Tesseract is installed and working
        'text_extraction': True,
        'nlp': True,
        'file_processing': True
    }
    
    # File Paths
    UPLOAD_FOLDER = 'uploads'
    PROCESSED_FOLDER = 'processed'
    INDEX_FOLDER = 'search_index'
    
    # Maximum file size (10MB)
    MAX_FILE_SIZE = 10 * 1024 * 1024

# Add get_local_ip as a static method to Config for backward compatibility
Config.get_local_ip = staticmethod(get_local_ip)

# Test the configuration
if __name__ == "__main__":
    print("=== Configuration Test ===")
    print(f"Local IP: {get_local_ip()}")
    print(f"Config Local IP: {Config.get_local_ip()}")
    print(f"Node ID would be: {get_local_ip()}_{uuid.uuid4().hex[:8]}")
    print("Configuration loaded successfully!")