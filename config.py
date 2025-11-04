import socket
import platform

def get_local_ip():
    """Get the local IP address of the machine - Windows compatible version"""
    try:
        # Method 1: Connect to external address
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        try:
            # Method 2: Get hostname
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            if ip and not ip.startswith('127.'):
                return ip
        except:
            pass
        return '127.0.0.1'

def get_broadcast_addr():
    """Get broadcast address for the local network"""
    local_ip = get_local_ip()
    if local_ip == '127.0.0.1':
        return "255.255.255.255"
    
    # Simple method: replace last part with 255
    parts = local_ip.split('.')
    if len(parts) == 4:
        parts[3] = '255'
        return '.'.join(parts)
    return "255.255.255.255"

# Configuration
class Config:
    # Network
    BROADCAST_PORT = 8888
    TASK_PORT = 8889
    FILE_PORT = 8890
    BROADCAST_ADDR = get_broadcast_addr()  # Use function instead of hardcoded
    HEARTBEAT_INTERVAL = 5  # seconds
    TASK_TIMEOUT = 30  # seconds
    
    # Node capabilities
    CAPABILITIES = {
        'ocr': False,  # Set to True if Tesseract is installed
        'text_extraction': True,
        'nlp': True,
        'file_processing': True
    }
    
    # Paths
    UPLOAD_FOLDER = 'uploads'
    PROCESSED_FOLDER = 'processed'
    INDEX_FOLDER = 'search_index'
    
    # Maximum file size (10MB)
    MAX_FILE_SIZE = 10 * 1024 * 1024

# Test the configuration
if __name__ == "__main__":
    print(f"Local IP: {get_local_ip()}")
    print(f"Broadcast Address: {Config.BROADCAST_ADDR}")
    print(f"Running on: {platform.system()}")