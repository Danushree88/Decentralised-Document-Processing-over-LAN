import socket

def get_local_ip():
    """Get local IP address"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except:
        return '127.0.0.1'

# config.py
class Config:
    # Folders
    UPLOAD_FOLDER = 'uploads'
    PROCESSED_FOLDER = 'processed'
    INDEX_FOLDER = 'search_index'
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
    
    # Network Configuration
    BROADCAST_PORT = 8888
    TASK_PORT = 8889
    FILE_PORT = 8890
    BROADCAST_ADDR = "255.255.255.255"
    HEARTBEAT_INTERVAL = 5
    TASK_TIMEOUT = 60
    
    # Node Type - Set this for each node (only one capability per node)
    # Choose ONE from: 'ocr', 'text_extraction', 'keyword_extraction'
    NODE_TYPE = 'text_extraction'  # Change this per node
    
    # Single capability based on node type
    CAPABILITIES = {
        NODE_TYPE: True
    }
    
    # File type to task mapping
    FILE_TYPE_TASKS = {
        '.pdf': ['text_extraction', 'keyword_extraction'],
        '.doc': ['text_extraction', 'keyword_extraction'],
        '.docx': ['text_extraction', 'keyword_extraction'],
        '.txt': ['text_extraction', 'keyword_extraction'],
        '.jpg': ['ocr', 'keyword_extraction'],
        '.png': ['ocr', 'keyword_extraction'],
        '.jpeg': ['ocr', 'keyword_extraction']
    }
    
    # Required packages for each node type
    REQUIRED_PACKAGES = {
        'ocr': ['pytesseract', 'PIL'],
        'text_extraction': ['PyPDF2', 'docx'],
        'keyword_extraction': []  # Built-in capability
    }
    
    @classmethod
    def validate_node_type(cls):
        """Validate that this node can perform its assigned task"""
        valid_types = ['ocr', 'text_extraction', 'keyword_extraction']
        
        if cls.NODE_TYPE not in valid_types:
            raise ValueError(f"Invalid NODE_TYPE: {cls.NODE_TYPE}. Must be one of {valid_types}")
        
        print(f"\nNode Configuration:")
        print(f"   Type: {cls.NODE_TYPE}")
        print(f"   IP: {get_local_ip()}")
        print(f"   Capabilities: {cls.CAPABILITIES}")
        
        # Check for required packages
        required = cls.REQUIRED_PACKAGES.get(cls.NODE_TYPE, [])
        missing_packages = []
        
        for package in required:
            try:
                if package == 'pytesseract':
                    import pytesseract
                elif package == 'PIL':
                    from PIL import Image
                elif package == 'PyPDF2':
                    import PyPDF2
                elif package == 'docx':
                    import docx
                print(f"   [✓] {package} available")
            except ImportError:
                missing_packages.append(package)
                print(f"   [✗] {package} missing")
        
        if missing_packages:
            print(f"\nWARNING: Missing packages for {cls.NODE_TYPE}: {missing_packages}")
            print("Some tasks may fail!")
        
        return cls.CAPABILITIES

# VALIDATE NODE ON IMPORT
print("\n" + "="*50)
print("NODE INITIALIZATION")
print("="*50)
Config.validate_node_type()
print("="*50 + "\n")