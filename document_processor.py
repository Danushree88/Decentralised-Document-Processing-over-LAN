# document_processor.py - CORRECTED VERSION
import os
import logging
from datetime import datetime
from config import Config

logger = logging.getLogger(__name__)

class DocumentProcessor:
    def __init__(self):
        self.node_type = Config.NODE_TYPE
        self.supported_tasks = self._detect_supported_tasks()
        logger.info(f"📋 Document Processor initialized as {self.node_type} NODE")
        logger.info(f"   Supported tasks: {[k for k, v in self.supported_tasks.items() if v]}")
    
    def _detect_supported_tasks(self):
        """Detect which tasks this SPECIALIZED node can perform"""
        tasks = {}
        
        if Config.NODE_TYPE == 'PDF':
            # PDF NODE - ONLY PDF processing
            tasks['pdf_processing'] = True
            try:
                import PyPDF2
                logger.info("✅ PyPDF2 available for PDF processing")
            except ImportError:
                logger.error("❌ PyPDF2 not installed! Install with: pip install PyPDF2")
                tasks['pdf_processing'] = False
            
            # Explicitly mark what we CANNOT do
            tasks['txt_processing'] = False
            tasks['text_extraction'] = False
            tasks['keyword_extraction'] = False
            tasks['file_processing'] = False
            
        elif Config.NODE_TYPE == 'TXT':
            # TXT NODE - ONLY TXT processing
            tasks['txt_processing'] = True
            tasks['text_extraction'] = True
            tasks['keyword_extraction'] = True
            
            # Explicitly mark what we CANNOT do
            tasks['pdf_processing'] = False
            tasks['file_processing'] = False
        
        else:
            logger.error("❌ NODE_TYPE not configured!")
            return {
                'pdf_processing': False,
                'txt_processing': False,
                'text_extraction': False,
                'keyword_extraction': False,
                'file_processing': False
            }
        
        return tasks
    
    def can_process(self, file_path):
        """Check if this node can process the file"""
        file_ext = os.path.splitext(file_path)[1].lower()
        
        if self.node_type == 'PDF':
            can_process = file_ext == '.pdf'
        elif self.node_type == 'TXT':
            can_process = file_ext == '.txt'
        else:
            can_process = False
        
        if not can_process:
            logger.warning(f"❌ {self.node_type} node cannot process {file_ext} files")
        
        return can_process
    
    def process_task(self, file_path, task_type):
        """Process a specific task type on a file - SPECIALIZED"""
        
        # CRITICAL: Check if we can process this file type
        if not self.can_process(file_path):
            file_ext = os.path.splitext(file_path)[1].lower()
            return {
                'success': False,
                'error': f'{self.node_type} node cannot process {file_ext} files',
                'task_type': task_type,
                'node_type': self.node_type
            }
        
        # Check if task is supported
        if task_type not in self.supported_tasks or not self.supported_tasks[task_type]:
            return {
                'success': False,
                'error': f'Task type {task_type} not supported on {self.node_type} node',
                'task_type': task_type,
                'node_type': self.node_type,
                'supported_tasks': [k for k, v in self.supported_tasks.items() if v]
            }
        
        try:
            if self.node_type == 'PDF':
                # PDF NODE - Only process PDFs
                if task_type == 'pdf_processing':
                    return self._process_pdf_extraction(file_path)
                else:
                    return {
                        'success': False,
                        'error': f'PDF node only supports pdf_processing task',
                        'task_type': task_type
                    }
            
            elif self.node_type == 'TXT':
                # TXT NODE - Only process TXT files
                if task_type == 'txt_processing' or task_type == 'text_extraction':
                    return self._process_txt_extraction(file_path)
                elif task_type == 'keyword_extraction':
                    return self._process_keyword_extraction(file_path)
                else:
                    return {
                        'success': False,
                        'error': f'TXT node only supports txt_processing and keyword_extraction',
                        'task_type': task_type
                    }
            
            else:
                return {
                    'success': False,
                    'error': 'Node type not configured',
                    'task_type': task_type
                }
                
        except Exception as e:
            logger.error(f"❌ Error processing {task_type} for {file_path}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': str(e),
                'task_type': task_type,
                'node_type': self.node_type
            }
    
    def _process_pdf_extraction(self, file_path):
        """PDF SPECIALIST - Extract text from PDF"""
        try:
            import PyPDF2
            
            logger.info(f"🔴 PDF NODE processing: {os.path.basename(file_path)}")
            
            text = ""
            page_count = 0
            
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                page_count = len(pdf_reader.pages)
                
                for page_num, page in enumerate(pdf_reader.pages, 1):
                    page_text = page.extract_text()
                    if page_text and page_text.strip():
                        text += f"\n--- Page {page_num} ---\n"
                        text += page_text + "\n"
                    logger.info(f"   Extracted page {page_num}/{page_count}")
            
            # If no text extracted, provide fallback
            if not text.strip():
                text = f"PDF Document: {os.path.basename(file_path)}\n\n"
                text += f"Pages: {page_count}\n"
                text += f"Note: This PDF may contain images or scanned content that requires OCR.\n"
            
            logger.info(f"✅ PDF extraction complete: {len(text)} characters from {page_count} pages")
            
            return {
                'success': True,
                'text': text,
                'keywords': self._extract_simple_keywords(text),
                'task_type': 'pdf_processing',
                'node_type': 'PDF',
                'metadata': {
                    'file_name': os.path.basename(file_path),
                    'processed_time': datetime.now().isoformat(),
                    'characters_extracted': len(text),
                    'word_count': len(text.split()),
                    'file_size': os.path.getsize(file_path),
                    'file_type': '.pdf',
                    'page_count': page_count,
                    'processed_by_node': 'PDF_SPECIALIST'
                }
            }
            
        except ImportError:
            return {
                'success': False,
                'error': 'PyPDF2 not installed on PDF node',
                'task_type': 'pdf_processing'
            }
        except Exception as e:
            logger.error(f"❌ PDF extraction failed: {e}")
            return {
                'success': False,
                'error': f'PDF processing failed: {str(e)}',
                'task_type': 'pdf_processing'
            }
    
    def _process_txt_extraction(self, file_path):
        """TXT SPECIALIST - Extract text from TXT file"""
        try:
            logger.info(f"🔵 TXT NODE processing: {os.path.basename(file_path)}")
            
            # Try different encodings
            encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
            text = None
            used_encoding = None
            
            for encoding in encodings:
                try:
                    with open(file_path, 'r', encoding=encoding) as f:
                        text = f.read()
                    used_encoding = encoding
                    logger.info(f"   Successfully read with {encoding} encoding")
                    break
                except UnicodeDecodeError:
                    continue
            
            if text is None:
                # Last resort: read as binary and decode with errors='ignore'
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    text = f.read()
                used_encoding = 'utf-8 (with errors ignored)'
            
            if not text.strip():
                text = f"Empty text file: {os.path.basename(file_path)}"
            
            logger.info(f"✅ TXT extraction complete: {len(text)} characters")
            
            return {
                'success': True,
                'text': text,
                'keywords': self._extract_simple_keywords(text),
                'task_type': 'txt_processing',
                'node_type': 'TXT',
                'metadata': {
                    'file_name': os.path.basename(file_path),
                    'processed_time': datetime.now().isoformat(),
                    'characters_extracted': len(text),
                    'word_count': len(text.split()),
                    'line_count': len(text.splitlines()),
                    'file_size': os.path.getsize(file_path),
                    'file_type': '.txt',
                    'encoding': used_encoding,
                    'processed_by_node': 'TXT_SPECIALIST'
                }
            }
            
        except Exception as e:
            logger.error(f"❌ TXT extraction failed: {e}")
            return {
                'success': False,
                'error': f'TXT processing failed: {str(e)}',
                'task_type': 'txt_processing'
            }
    
    def _process_keyword_extraction(self, file_path):
        """Extract keywords from TXT file"""
        try:
            # First extract text
            text_result = self._process_txt_extraction(file_path)
            if not text_result['success']:
                return text_result
            
            text = text_result['text']
            keywords = self._extract_simple_keywords(text)
            
            return {
                'success': True,
                'text': text,  # Include text for indexing
                'keywords': keywords,
                'task_type': 'keyword_extraction',
                'node_type': 'TXT',
                'metadata': {
                    'file_name': os.path.basename(file_path),
                    'processed_time': datetime.now().isoformat(),
                    'keywords_found': len(keywords),
                    'processed_by_node': 'TXT_SPECIALIST'
                }
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'Keyword extraction failed: {str(e)}',
                'task_type': 'keyword_extraction'
            }
    
    def _extract_simple_keywords(self, text):
        """Extract simple keywords from text"""
        if not text or not text.strip():
            return ['document', 'file']
        
        # Clean and tokenize
        words = text.lower().split()
        
        # Common stop words
        stop_words = {
            'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
            'of', 'with', 'by', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
            'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'should',
            'could', 'may', 'might', 'can', 'this', 'that', 'these', 'those'
        }
        
        # Filter meaningful words
        meaningful_words = [
            word.strip('.,!?;:"()[]{}') 
            for word in words 
            if len(word) > 3 and word.lower() not in stop_words
        ]
        
        # Count frequencies
        from collections import Counter
        keyword_counts = Counter(meaningful_words)
        
        # Get top 15 keywords
        keywords = [word for word, count in keyword_counts.most_common(15)]
        
        return keywords