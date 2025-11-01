import os
import json
import sqlite3
import logging
import whoosh.index as index
from whoosh.fields import Schema, TEXT, ID, KEYWORD, DATETIME
from whoosh.qparser import QueryParser
import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

class SearchIndex:
    def __init__(self, index_path="search_index"):
        self.index_path = index_path
        self.schema = Schema(
            file_id=ID(stored=True, unique=True),
            file_name=TEXT(stored=True),
            content=TEXT(stored=True),
            keywords=KEYWORD(stored=True),
            metadata=TEXT(stored=True),
            processed_time=DATETIME(stored=True),
            node_id=TEXT(stored=True)
        )
        self.setup_index()
        self.setup_database()
    
    def setup_index(self):
        """Setup Whoosh search index"""
        try:
            if not os.path.exists(self.index_path):
                os.makedirs(self.index_path, exist_ok=True)
                self.ix = index.create_in(self.index_path, self.schema)
                logging.info(f"Created new search index at {self.index_path}")
            else:
                # Check if index exists
                if index.exists_in(self.index_path):
                    self.ix = index.open_dir(self.index_path)
                    logging.info(f"Opened existing search index at {self.index_path}")
                else:
                    self.ix = index.create_in(self.index_path, self.schema)
                    logging.info(f"Created new search index at {self.index_path}")
        except Exception as e:
            logging.error(f"Error setting up search index: {e}")
            # Create fresh index if there's any error
            if os.path.exists(self.index_path):
                import shutil
                shutil.rmtree(self.index_path)
            os.makedirs(self.index_path, exist_ok=True)
            self.ix = index.create_in(self.index_path, self.schema)
    
    def setup_database(self):
        """Setup SQLite database for metadata"""
        try:
            self.conn = sqlite3.connect('documents.db', check_same_thread=False)
            cursor = self.conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS documents (
                    file_id TEXT PRIMARY KEY,
                    file_name TEXT,
                    file_path TEXT,
                    file_size INTEGER,
                    word_count INTEGER,
                    character_count INTEGER,
                    keywords TEXT,
                    metadata TEXT,
                    processed_time TEXT,
                    node_id TEXT,
                    content_preview TEXT
                )
            ''')
            self.conn.commit()
            logging.info("SQLite database setup completed")
        except Exception as e:
            logging.error(f"Error setting up database: {e}")
    
    def add_document(self, file_id, file_name, content, keywords, metadata, node_id):
        """Add document to search index and database"""
        try:
            # Add to Whoosh index
            writer = self.ix.writer()
            writer.add_document(
                file_id=file_id,
                file_name=file_name,
                content=content,
                keywords=" ".join(keywords) if keywords else "",
                metadata=json.dumps(metadata),
                processed_time=datetime.datetime.now(),
                node_id=node_id
            )
            writer.commit()
            
            # Add to SQLite database
            cursor = self.conn.cursor()
            content_preview = content[:500] + "..." if len(content) > 500 else content
            
            cursor.execute('''
                INSERT OR REPLACE INTO documents 
                (file_id, file_name, file_path, file_size, word_count, character_count, keywords, metadata, processed_time, node_id, content_preview)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                file_id,
                file_name,
                metadata.get('file_path', ''),
                metadata.get('file_size', 0),
                metadata.get('word_count', 0),
                metadata.get('character_count', 0),
                json.dumps(keywords) if keywords else "[]",
                json.dumps(metadata),
                metadata.get('processed_time', ''),
                node_id,
                content_preview
            ))
            self.conn.commit()
            
            logging.info(f"Added document to index: {file_name}")
            return True
        except Exception as e:
            logging.error(f"Error adding document to index: {e}")
            return False
    
    def search(self, query_string, limit=10):
        """Search documents"""
        try:
            results = []
            with self.ix.searcher() as searcher:
                query = QueryParser("content", self.ix.schema).parse(query_string)
                search_results = searcher.search(query, limit=limit)
                
                for result in search_results:
                    try:
                        metadata = json.loads(result['metadata']) if result['metadata'] else {}
                        keywords = result['keywords'].split() if result['keywords'] else []
                        results.append({
                            'file_id': result['file_id'],
                            'file_name': result['file_name'],
                            'content': result['content'][:200] + "..." if len(result['content']) > 200 else result['content'],
                            'keywords': keywords,
                            'metadata': metadata,
                            'score': result.score
                        })
                    except Exception as e:
                        logging.error(f"Error processing search result: {e}")
                        continue
            return results
        except Exception as e:
            logging.error(f"Error searching: {e}")
            return []
    
    def get_all_documents(self):
        """Get all documents from database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                SELECT file_id, file_name, file_size, word_count, keywords, processed_time, node_id, content_preview
                FROM documents ORDER BY processed_time DESC
            ''')
            documents = []
            for row in cursor.fetchall():
                try:
                    documents.append({
                        'file_id': row[0],
                        'file_name': row[1],
                        'file_size': row[2],
                        'word_count': row[3],
                        'keywords': json.loads(row[4]) if row[4] and row[4] != "[]" else [],
                        'processed_time': row[5],
                        'node_id': row[6],
                        'content_preview': row[7]
                    })
                except Exception as e:
                    logging.error(f"Error processing document row: {e}")
                    continue
            return documents
        except Exception as e:
            logging.error(f"Error getting all documents: {e}")
            return []
    
    def get_stats(self):
        """Get index statistics"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM documents')
            total_docs = cursor.fetchone()[0]
            
            cursor.execute('SELECT SUM(file_size) FROM documents')
            total_size = cursor.fetchone()[0] or 0
            
            cursor.execute('SELECT SUM(word_count) FROM documents')
            total_words = cursor.fetchone()[0] or 0
            
            return {
                'total_documents': total_docs,
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'total_words': total_words
            }
        except Exception as e:
            logging.error(f"Error getting stats: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'conn'):
            self.conn.close()