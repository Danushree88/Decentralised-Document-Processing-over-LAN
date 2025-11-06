# config.py - FIXED with instance support
import os

class Config:
    """Configuration for specialized document processing nodes"""
    
    # Node specialization
    NODE_TYPE = None  # 'PDF' or 'TXT'
    
    # Network configuration - BASE ports
    BROADCAST_PORT = 8888
    BASE_TASK_PORT = 8889
    BASE_FILE_PORT = 8890
    
    # Actual ports (will be calculated based on instance)
    TASK_PORT = None
    FILE_PORT = None
    
    BROADCAST_ADDR = "255.255.255.255"
    HEARTBEAT_INTERVAL = 5  # seconds
    TASK_TIMEOUT = 30  # seconds
    
    # File handling
    UPLOAD_FOLDER = 'uploads'
    PROCESSED_FOLDER = 'processed'
    INDEX_FOLDER = 'search_index'
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50MB
    
    # Capabilities (will be set by specialization)
    CAPABILITIES = {}
    
    # File type to task mapping
    FILE_TYPE_TASKS = {
        '.pdf': ['pdf_processing'],
        '.txt': ['txt_processing', 'keyword_extraction'],
        '.doc': ['text_extraction'],
        '.docx': ['text_extraction'],
        '.jpg': ['ocr'],
        '.jpeg': ['ocr'],
        '.png': ['ocr']
    }
    
    @classmethod
    def configure_as_pdf_node(cls, instance=0):
        """
        Configure as PDF specialist node
        
        Args:
            instance: Instance number for port allocation (0, 1, 2, ...)
                     - Instance 0: ports 8889, 8890
                     - Instance 1: ports 8899, 8900
                     - Instance 2: ports 8909, 8910
        """
        cls.NODE_TYPE = 'PDF'
        
        # Calculate unique ports based on instance
        cls.TASK_PORT = cls.BASE_TASK_PORT + (instance * 10)
        cls.FILE_PORT = cls.BASE_FILE_PORT + (instance * 10)
        
        # Set PDF-only capabilities
        cls.CAPABILITIES = {
            'pdf_processing': True,
            'txt_processing': False,
            'text_extraction': False,
            'keyword_extraction': False,
            'ocr': False,
            'nlp': False,
            'summarization': False,
            'entity_recognition': False,
            'topic_modeling': False,
            'file_processing': False
        }
        
        # Update folder names to avoid conflicts
        if instance > 0:
            cls.UPLOAD_FOLDER = f'uploads_pdf_{instance}'
            cls.PROCESSED_FOLDER = f'processed_pdf_{instance}'
            cls.INDEX_FOLDER = f'search_index_pdf_{instance}'
        else:
            cls.UPLOAD_FOLDER = 'uploads_pdf'
            cls.PROCESSED_FOLDER = 'processed_pdf'
            cls.INDEX_FOLDER = 'search_index_pdf'
        
        print(f"🔴 Configured as PDF NODE (Instance {instance})")
        print(f"   Task Port: {cls.TASK_PORT}")
        print(f"   File Port: {cls.FILE_PORT}")
        print(f"   Upload Folder: {cls.UPLOAD_FOLDER}")
    
    @classmethod
    def configure_as_txt_node(cls, instance=0):
        """
        Configure as TXT specialist node
        
        Args:
            instance: Instance number for port allocation (0, 1, 2, ...)
                     - Instance 0: ports 8890, 8891
                     - Instance 1: ports 8900, 8901
                     - Instance 2: ports 8910, 8911
        """
        cls.NODE_TYPE = 'TXT'
        
        # Calculate unique ports based on instance (offset by 1 from PDF)
        cls.TASK_PORT = cls.BASE_TASK_PORT + (instance * 10) + 1
        cls.FILE_PORT = cls.BASE_FILE_PORT + (instance * 10) + 1
        
        # Set TXT-only capabilities
        cls.CAPABILITIES = {
            'pdf_processing': False,
            'txt_processing': True,
            'text_extraction': True,
            'keyword_extraction': True,
            'ocr': False,
            'nlp': False,
            'summarization': False,
            'entity_recognition': False,
            'topic_modeling': False,
            'file_processing': False
        }
        
        # Update folder names to avoid conflicts
        if instance > 0:
            cls.UPLOAD_FOLDER = f'uploads_txt_{instance}'
            cls.PROCESSED_FOLDER = f'processed_txt_{instance}'
            cls.INDEX_FOLDER = f'search_index_txt_{instance}'
        else:
            cls.UPLOAD_FOLDER = 'uploads_txt'
            cls.PROCESSED_FOLDER = 'processed_txt'
            cls.INDEX_FOLDER = 'search_index_txt'
        
        print(f"🔵 Configured as TXT NODE (Instance {instance})")
        print(f"   Task Port: {cls.TASK_PORT}")
        print(f"   File Port: {cls.FILE_PORT}")
        print(f"   Upload Folder: {cls.UPLOAD_FOLDER}")
    
    @classmethod
    def get_node_info(cls):
        """Get current node configuration info"""
        return {
            'node_type': cls.NODE_TYPE,
            'task_port': cls.TASK_PORT,
            'file_port': cls.FILE_PORT,
            'capabilities': cls.CAPABILITIES,
            'upload_folder': cls.UPLOAD_FOLDER
        }