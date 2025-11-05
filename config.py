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

# config.py
class Config:
    # Network Configuration
    BROADCAST_PORT = 8888
    TASK_PORT = 8889
    FILE_PORT = 8890
    BROADCAST_ADDR = "255.255.255.255"
    HEARTBEAT_INTERVAL = 5
    
    # Node Capabilities (Dynamically detected)
    CAPABILITIES = {
        'ocr': False,
        'text_extraction': False,
        'nlp': False,
        'summarization': False,
        'keyword_extraction': False,
        'entity_recognition': False,
        'topic_modeling': False,
        'file_processing': False
    }
    
    # File type mappings
    FILE_TYPE_TASKS = {
        '.pdf': ['ocr', 'text_extraction', 'keyword_extraction'],
        '.doc': ['text_extraction', 'keyword_extraction'],
        '.docx': ['text_extraction', 'keyword_extraction'],
        '.txt': ['text_extraction', 'nlp', 'summarization'],
        '.jpg': ['ocr', 'keyword_extraction'],
        '.png': ['ocr', 'keyword_extraction'],
        '.jpeg': ['ocr', 'keyword_extraction']
    }
    
    @classmethod
    def detect_capabilities(cls):
        """Dynamically detect what this node can do"""
        capabilities = cls.CAPABILITIES.copy()
        
        # Detect OCR capability
        try:
            import pytesseract
            capabilities['ocr'] = True
        except ImportError:
            try:
                import tesseract
                capabilities['ocr'] = True
            except ImportError:
                capabilities['ocr'] = False
        
        # Detect NLP capabilities
        try:
            import spacy
            capabilities['nlp'] = True
            capabilities['entity_recognition'] = True
        except ImportError:
            capabilities['nlp'] = False
            capabilities['entity_recognition'] = False
        
        try:
            from sumy.parsers.plaintext import PlaintextParser
            from sumy.nlp.tokenizers import Tokenizer
            capabilities['summarization'] = True
        except ImportError:
            capabilities['summarization'] = False
        
        # Always available
        capabilities['text_extraction'] = True
        capabilities['keyword_extraction'] = True
        capabilities['file_processing'] = True
        
        cls.CAPABILITIES = capabilities
        print(f"🔍 Detected capabilities: {[k for k, v in capabilities.items() if v]}")
        return capabilities