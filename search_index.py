import json
import os
import logging
from datetime import datetime
from collections import defaultdict
import re

logger = logging.getLogger(__name__)

class SearchIndex:
    def __init__(self, index_folder='search_index'):
        self.index_folder = index_folder
        self.index_file = os.path.join(index_folder, 'documents.json')
        self.documents = {}
        self.inverted_index = defaultdict(set)
        
        os.makedirs(index_folder, exist_ok=True)
        self._load_index()
        logger.info(f"✅ Search Index initialized with {len(self.documents)} documents")
    
    def _load_index(self):
        """Load index from disk"""
        try:
            if os.path.exists(self.index_file):
                with open(self.index_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.documents = data.get('documents', {})
                    # Rebuild inverted index
                    for doc_id, doc in self.documents.items():
                        self._update_inverted_index(doc_id, doc)
                logger.info(f"✅ Loaded {len(self.documents)} documents from index")
        except Exception as e:
            logger.error(f"Error loading index: {e}")
            self.documents = {}
    
    def _save_index(self):
        """Save index to disk"""
        try:
            with open(self.index_file, 'w', encoding='utf-8') as f:
                json.dump({'documents': self.documents}, f, indent=2)
            logger.info(f"💾 Saved index with {len(self.documents)} documents")
            return True
        except Exception as e:
            logger.error(f"❌ Error saving index: {e}")
            return False
    
    def _update_inverted_index(self, doc_id, doc):
        """Update inverted index for searching"""
        try:
            # Index keywords
            for keyword in doc.get('keywords', []):
                self.inverted_index[keyword.lower()].add(doc_id)
            
            # Index content words
            content = doc.get('content', '')
            words = re.findall(r'\w+', content.lower())
            for word in words:
                if len(word) > 2:  # Only index words longer than 2 chars
                    self.inverted_index[word].add(doc_id)
            
            # Index filename
            filename = doc.get('file_name', '').lower()
            for word in re.findall(r'\w+', filename):
                self.inverted_index[word].add(doc_id)
                
        except Exception as e:
            logger.error(f"Error updating inverted index: {e}")
    
    def add_document(self, file_id, file_name, content, keywords, metadata, node_id):
        """Add document to search index"""
        try:
            logger.info(f"📝 Adding document to index: {file_name}")
            
            # Create document entry
            doc = {
                'file_id': file_id,
                'file_name': file_name,
                'content': content,
                'content_preview': content[:200] + '...' if len(content) > 200 else content,
                'keywords': keywords,
                'metadata': metadata,
                'node_id': node_id,
                'indexed_time': datetime.now().isoformat(),
                'word_count': len(content.split()),
                'file_size': metadata.get('file_size', 0)
            }
            
            # Add to documents
            self.documents[file_id] = doc
            
            # Update inverted index
            self._update_inverted_index(file_id, doc)
            
            # Save to disk
            self._save_index()
            
            logger.info(f"✅ Document added to index: {file_name} (ID: {file_id})")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error adding document to index: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False
    
    def search(self, query, limit=10):
        """Search documents by query"""
        try:
            logger.info(f"🔍 Searching for: '{query}'")
            
            if not query:
                return []
            
            # Tokenize query
            query_words = re.findall(r'\w+', query.lower())
            
            # Find matching documents
            matching_docs = set()
            for word in query_words:
                if word in self.inverted_index:
                    matching_docs.update(self.inverted_index[word])
            
            # If no matches in inverted index, do simple text search
            if not matching_docs:
                logger.info("No inverted index matches, doing full text search")
                for doc_id, doc in self.documents.items():
                    if query.lower() in doc['content'].lower() or query.lower() in doc['file_name'].lower():
                        matching_docs.add(doc_id)
            
            # Score and sort results
            results = []
            for doc_id in matching_docs:
                if doc_id in self.documents:
                    doc = self.documents[doc_id]
                    score = self._calculate_score(doc, query_words)
                    results.append({
                        'file_id': doc['file_id'],
                        'file_name': doc['file_name'],
                        'content': doc['content_preview'],
                        'keywords': doc['keywords'],
                        'metadata': doc['metadata'],
                        'score': score,
                        'node_id': doc['node_id']
                    })
            
            # Sort by score
            results.sort(key=lambda x: x['score'], reverse=True)
            
            logger.info(f"✅ Found {len(results)} matching documents")
            return results[:limit]
            
        except Exception as e:
            logger.error(f"❌ Search error: {e}")
            return []
    
    def _calculate_score(self, doc, query_words):
        """Calculate relevance score"""
        score = 0
        content_lower = doc['content'].lower()
        filename_lower = doc['file_name'].lower()
        
        for word in query_words:
            # Filename matches are highly relevant
            if word in filename_lower:
                score += 5
            # Keyword matches are very relevant
            if word in [k.lower() for k in doc['keywords']]:
                score += 3
            # Content matches
            score += content_lower.count(word)
        
        return score
    
    def get_all_documents(self):
        """Get all documents"""
        try:
            docs = list(self.documents.values())
            logger.info(f"📚 Retrieved {len(docs)} documents")
            return docs
        except Exception as e:
            logger.error(f"Error getting all documents: {e}")
            return []
    
    def get_document(self, file_id):
        """Get specific document"""
        return self.documents.get(file_id)
    
    def delete_document(self, file_id):
        """Delete document from index"""
        try:
            if file_id in self.documents:
                del self.documents[file_id]
                self._save_index()
                logger.info(f"🗑️ Deleted document: {file_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"Error deleting document: {e}")
            return False
    
    def get_stats(self):
        """Get index statistics"""
        try:
            total_words = sum(doc['word_count'] for doc in self.documents.values())
            total_size = sum(doc['file_size'] for doc in self.documents.values())
            
            return {
                'total_documents': len(self.documents),
                'total_words': total_words,
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'indexed_terms': len(self.inverted_index)
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            return {
                'total_documents': 0,
                'total_words': 0,
                'total_size_bytes': 0,
                'total_size_mb': 0,
                'indexed_terms': 0
            }