import socket

def get_local_ip():
    """Get local IP address"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except:
        try:
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            if not ip.startswith('127.'):
                return ip
        except:
            pass
        return '127.0.0.1'

class Config:
    # Folders
    UPLOAD_FOLDER = 'uploads'
    PROCESSED_FOLDER = 'processed'
    INDEX_FOLDER = 'search_index'
    
    # Network Configuration
    BROADCAST_PORT = 8888
    TASK_PORT = 8889
    FILE_PORT = 8890
    BROADCAST_ADDR = "255.255.255.255"
    HEARTBEAT_INTERVAL = 5
    TASK_TIMEOUT = 60  # Increased from 30 to 60 seconds
    
    # File settings
    MAX_FILE_SIZE = 10 * 1024 * 1024  # 10MB
    
    # Node Capabilities
    CAPABILITIES = {
        'ocr': False,
        'text_extraction': True,
        'nlp': False,
        'summarization': False,
        'keyword_extraction': True,
        'entity_recognition': False,
        'topic_modeling': False,
        'file_processing': True
    }
    
    # File type mappings for task segmentation
    FILE_TYPE_TASKS = {
        '.pdf': ['text_extraction', 'keyword_extraction'],
        '.doc': ['text_extraction', 'keyword_extraction'],
        '.docx': ['text_extraction', 'keyword_extraction'],
        '.txt': ['text_extraction', 'keyword_extraction'],
        '.jpg': ['ocr', 'keyword_extraction'],
        '.png': ['ocr', 'keyword_extraction'],
        '.jpeg': ['ocr', 'keyword_extraction']
    }
    
    @classmethod
    def detect_capabilities(cls):
        """Detect what this node can do"""
        capabilities = cls.CAPABILITIES.copy()
        
        # Detect OCR
        try:
            import pytesseract
            capabilities['ocr'] = True
            print("OCR capability detected")
        except ImportError:
            capabilities['ocr'] = False
            print("OCR not available")
        
        # Detect NLP
        try:
            import spacy
            capabilities['nlp'] = True
            capabilities['entity_recognition'] = True
            print("NLP capabilities detected")
        except ImportError:
            capabilities['nlp'] = False
            capabilities['entity_recognition'] = False
            print("NLP not available")
        
        try:
            import nltk
            capabilities['summarization'] = True
            print("Summarization capability detected")
        except ImportError:
            capabilities['summarization'] = False
        
        # Always available
        capabilities['text_extraction'] = True
        capabilities['keyword_extraction'] = True
        capabilities['file_processing'] = True
        
        cls.CAPABILITIES = capabilities
        print(f"Detected capabilities: {[k for k, v in capabilities.items() if v]}")
        return capabilities