import os
import PyPDF2
import docx
import pytesseract
from PIL import Image
import spacy
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import io
import json
import logging

# Download required NLTK data
try:
    nltk.download('punkt', quiet=True)
    nltk.download('stopwords', quiet=True)
    nltk.download('averaged_perceptron_tagger', quiet=True)
except:
    pass

# Load spaCy model
try:
    nlp = spacy.load("en_core_web_sm")
except OSError:
    print("Warning: spaCy model not found. Install with: python -m spacy download en_core_web_sm")
    nlp = None

class DocumentProcessor:
    def __init__(self):
        self.supported_formats = ['.pdf', '.docx', '.doc', '.txt', '.jpg', '.jpeg', '.png']
        self.stop_words = set(stopwords.words('english'))
    
    def extract_text_from_pdf(self, file_path):
        """Extract text from PDF file"""
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                text = ""
                for page in pdf_reader.pages:
                    text += page.extract_text() + "\n"
                return text.strip()
        except Exception as e:
            logging.error(f"Error extracting text from PDF: {e}")
            return ""
    
    def extract_text_from_docx(self, file_path):
        """Extract text from DOCX file"""
        try:
            doc = docx.Document(file_path)
            text = ""
            for paragraph in doc.paragraphs:
                text += paragraph.text + "\n"
            return text.strip()
        except Exception as e:
            logging.error(f"Error extracting text from DOCX: {e}")
            return ""
    
    def extract_text_with_ocr(self, file_path):
        """Extract text from image using OCR"""
        try:
            image = Image.open(file_path)
            text = pytesseract.image_to_string(image)
            return text.strip()
        except Exception as e:
            logging.error(f"Error performing OCR: {e}")
            return ""
    
    def extract_metadata(self, file_path, text):
        """Extract metadata from document"""
        import datetime
        stats = os.stat(file_path)
        
        metadata = {
            'file_name': os.path.basename(file_path),
            'file_size': stats.st_size,
            'modified_time': datetime.datetime.fromtimestamp(stats.st_mtime).isoformat(),
            'processed_time': datetime.datetime.now().isoformat(),
            'word_count': len(text.split()),
            'character_count': len(text)
        }
        return metadata
    
    def extract_keywords(self, text, max_keywords=10):
        """Extract keywords from text using NLP"""
        if not text.strip():
            return []
        
        try:
            # Method 1: Using spaCy for entity recognition
            if nlp and len(text) > 0:
                doc = nlp(text)
                entities = [ent.text for ent in doc.ents if ent.label_ in ['ORG', 'PERSON', 'GPE', 'PRODUCT', 'EVENT']]
                
                # Method 2: Using NLTK for keyword extraction
                words = word_tokenize(text.lower())
                filtered_words = [word for word in words if word.isalnum() and word not in self.stop_words and len(word) > 2]
                
                # Combine methods and get most frequent
                from collections import Counter
                all_keywords = entities + filtered_words
                keyword_counter = Counter(all_keywords)
                
                return [keyword for keyword, count in keyword_counter.most_common(max_keywords)]
        except Exception as e:
            logging.error(f"Error extracting keywords: {e}")
        
        # Fallback: simple word frequency
        words = text.lower().split()
        filtered_words = [word for word in words if word.isalnum() and len(word) > 3]
        from collections import Counter
        return [word for word, count in Counter(filtered_words).most_common(max_keywords)]
    
    def process_document(self, file_path, task_type='full'):
        """Main document processing function"""
        file_ext = os.path.splitext(file_path)[1].lower()
        text = ""
        metadata = {}
        keywords = []
        
        try:
            # Extract text based on file type
            if file_ext == '.pdf':
                text = self.extract_text_from_pdf(file_path)
                # If text extraction failed, try OCR
                if not text.strip():
                    text = self.extract_text_with_ocr(file_path)
            elif file_ext in ['.docx', '.doc']:
                text = self.extract_text_from_docx(file_path)
            elif file_ext in ['.jpg', '.jpeg', '.png']:
                text = self.extract_text_with_ocr(file_path)
            elif file_ext == '.txt':
                with open(file_path, 'r', encoding='utf-8') as f:
                    text = f.read()
            
            # Extract metadata
            metadata = self.extract_metadata(file_path, text)
            
            # Extract keywords if requested
            if task_type in ['full', 'keywords'] and text.strip():
                keywords = self.extract_keywords(text)
            
            return {
                'success': True,
                'text': text,
                'metadata': metadata,
                'keywords': keywords,
                'file_path': file_path
            }
            
        except Exception as e:
            logging.error(f"Error processing document {file_path}: {e}")
            return {
                'success': False,
                'error': str(e),
                'file_path': file_path
            }