import socket
import platform

def get_local_ip():
    """Get local IP address - Enhanced version"""
    try:
        # Method 1: Connect to external service
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            return ip
    except:
        try:
            # Method 2: Get hostname
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            if ip.startswith('127.'):
                raise Exception("Localhost IP")
            return ip
        except:
            # Method 3: Try all interfaces
            try:
                return socket.gethostbyname(socket.gethostname() + ".local")
            except:
                return '127.0.0.1'

class Config:
    # Network Configuration
    BROADCAST_PORT = 8888
    TASK_PORT = 8889
    FILE_PORT = 8890
    BROADCAST_ADDR = "255.255.255.255"
    HEARTBEAT_INTERVAL = 5  # seconds
    TASK_TIMEOUT = 30  # seconds
    
    # File Processing
    UPLOAD_FOLDER = 'uploads'
    PROCESSED_FOLDER = 'processed'
    INDEX_FOLDER = 'search_index'
    MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
    
    # Node Capabilities
    CAPABILITIES = {
        'text_extraction': True,
        'file_processing': True,
        'ocr': True,  # Enable OCR if available
        'nlp': True,  # Enable NLP if available
        'storage': 1000,  # MB available
        'cpu_cores': 4,  # Estimated CPU cores
        'memory_mb': 4096  # Estimated memory
    }