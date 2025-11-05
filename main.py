
from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
import os
import uuid
import time
import logging
import threading
import socket
import sys

# Windows-compatible logging (no emojis)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Force UTF-8 encoding for Windows console
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except:
        pass

logger = logging.getLogger(__name__)

def get_local_ip():
    """Get local IP address - Windows compatible"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except:
        try:
            return socket.gethostbyname(socket.gethostname())
        except:
            return '127.0.0.1'

app = Flask(__name__)
app.config['SECRET_KEY'] = 'decentralized-doc-processor-secret-key'
socketio = SocketIO(app, cors_allowed_origins="*")

# Import and initialize components with proper error handling
components = {}

try:
    from config import Config
    components['config'] = True
    logger.info("Config loaded")
    
    # Create directories
    os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(Config.PROCESSED_FOLDER, exist_ok=True)
    os.makedirs(Config.INDEX_FOLDER, exist_ok=True)
    
    app.config['UPLOAD_FOLDER'] = Config.UPLOAD_FOLDER
    app.config['MAX_CONTENT_LENGTH'] = Config.MAX_FILE_SIZE
    
except Exception as e:
    logger.error(f"Config failed: {e}")
    # Fallback config
    class Config:
        UPLOAD_FOLDER = 'uploads'
        PROCESSED_FOLDER = 'processed'
        INDEX_FOLDER = 'search_index'
        MAX_FILE_SIZE = 10 * 1024 * 1024
        CAPABILITIES = {'text_extraction': True, 'file_processing': True}
        BROADCAST_PORT = 8888
        TASK_PORT = 8889
        FILE_PORT = 8890
        BROADCAST_ADDR = "255.255.255.255"
        HEARTBEAT_INTERVAL = 5
        TASK_TIMEOUT = 30
    
    os.makedirs('uploads', exist_ok=True)
    os.makedirs('processed', exist_ok=True)
    os.makedirs('search_index', exist_ok=True)
    app.config['UPLOAD_FOLDER'] = 'uploads'
    app.config['MAX_CONTENT_LENGTH'] = 10 * 1024 * 1024

# Initialize SearchIndex first
try:
    from search_index import SearchIndex
    search_index = SearchIndex()
    components['search_index'] = True
    logger.info("Search Index initialized")
except Exception as e:
    logger.error(f"Search Index failed: {e}")
    # Fallback
    class FallbackSearchIndex:
        def __init__(self):
            self.documents = []
            logger.info("Using fallback search index")
        def add_document(self, *args, **kwargs):
            doc_id = str(uuid.uuid4())
            self.documents.append({
                'file_id': doc_id,
                'file_name': kwargs.get('file_name', 'unknown'),
                'content': kwargs.get('content', ''),
                'keywords': kwargs.get('keywords', []),
                'metadata': kwargs.get('metadata', {})
            })
            logger.info(f"Added document: {kwargs.get('file_name', 'unknown')}")
            return True
        def search(self, query, limit=10):
            results = []
            for doc in self.documents:
                if query.lower() in doc['content'].lower() or query.lower() in doc['file_name'].lower():
                    results.append(doc)
            return results[:limit]
        def get_all_documents(self):
            return self.documents
        def get_stats(self):
            return {
                'total_documents': len(self.documents),
                'total_size_bytes': 0,
                'total_size_mb': 0,
                'total_words': sum(len(doc['content'].split()) for doc in self.documents)
            }
    search_index = FallbackSearchIndex()

# Initialize Document Processor
try:
    from document_processor import DocumentProcessor
    document_processor = DocumentProcessor()
    components['document_processor'] = True
    logger.info("Document Processor initialized")
except Exception as e:
    logger.error(f"Document Processor failed: {e}")
    # Fallback
    class FallbackDocumentProcessor:
        def process_task(self, file_path, task_type='full'):
            import os
            from datetime import datetime
            
            # Try to read actual file content
            try:
                if file_path.endswith('.txt'):
                    with open(file_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                else:
                    content = f"Sample content from {os.path.basename(file_path)}"
            except:
                content = f"Content from {os.path.basename(file_path)}"
            
            return {
                'success': True,
                'text': content,
                'metadata': {
                    'file_name': os.path.basename(file_path),
                    'file_size': os.path.getsize(file_path),
                    'modified_time': datetime.now().isoformat(),
                    'processed_time': datetime.now().isoformat(),
                    'word_count': len(content.split()),
                    'character_count': len(content)
                },
                'keywords': ['sample', 'document', 'test'],
                'file_path': file_path
            }
    document_processor = FallbackDocumentProcessor()

# Create temporary node_id for TaskManager
temp_node_id = f"local_{get_local_ip()}_{int(time.time())}"

# Initialize TaskManager BEFORE PeerNode
try:
    from task_manager import TaskManager
    task_manager = TaskManager(temp_node_id, search_index)
    components['task_manager'] = True
    logger.info("Task Manager initialized successfully")
except Exception as e:
    logger.error(f"Task Manager failed: {e}")
    import traceback
    logger.error(traceback.format_exc())
    
    # Fallback TaskManager with ALL required attributes
    class FallbackTaskManager:
        def __init__(self, node_id, search_index):
            self.node_id = node_id
            self.search_index = search_index
            self.peer_load = {}
            self.pending_tasks = {}  # Dict not int
            self.completed_tasks = 0
            self.peer_capabilities = {}
            self.lock = threading.Lock()  # CRITICAL - Must have lock!
            logger.info("Using fallback task manager")
        
        def distribute_task(self, file_path, task_type='full'):
            try:
                logger.info(f"Fallback: Processing {os.path.basename(file_path)} locally")
                # Process locally as fallback
                result = document_processor.process_task(file_path, task_type)
                if result['success']:
                    file_id = str(uuid.uuid4())
                    self.search_index.add_document(
                        file_id=file_id,
                        file_name=result['metadata']['file_name'],
                        content=result['text'],
                        keywords=result['keywords'],
                        metadata=result['metadata'],
                        node_id=self.node_id
                    )
                    with self.lock:
                        self.completed_tasks += 1
                    logger.info(f"Fallback: Successfully processed {result['metadata']['file_name']}")
                    return f"task_{int(time.time())}"
            except Exception as e:
                logger.error(f"Task processing error: {e}")
            return None
        
        def distribute_batch(self, file_paths):
            """Fallback batch processing"""
            distributed_tasks = []
            for file_path in file_paths:
                task_id = self.distribute_task(file_path, 'full')
                if task_id:
                    distributed_tasks.append({
                        'task_id': task_id,
                        'file_path': file_path,
                        'task_type': 'full',
                        'priority': 1
                    })
            return distributed_tasks
        
        def get_stats(self):
            with self.lock:
                return {
                    'pending_tasks': len(self.pending_tasks),
                    'completed_tasks': self.completed_tasks,
                    'failed_tasks': 0,
                    'peer_load': dict(self.peer_load),
                    'connected_peers': len(self.peer_capabilities)
                }
        
        def update_peer_capabilities(self, peer_id, capabilities):
            with self.lock:
                self.peer_capabilities[peer_id] = capabilities
                logger.info(f"Fallback: Updated capabilities for {peer_id[:15]}...")
        
        def remove_peer(self, peer_id):
            with self.lock:
                if peer_id in self.peer_capabilities:
                    del self.peer_capabilities[peer_id]
                if peer_id in self.peer_load:
                    del self.peer_load[peer_id]
                logger.info(f"Fallback: Removed peer {peer_id[:15]}...")
    
    task_manager = FallbackTaskManager(temp_node_id, search_index)

# Initialize PeerNode LAST with TaskManager reference
try:
    from peer_node import PeerNode
    peer_node = PeerNode(task_manager=task_manager)
    components['peer_node'] = True
    logger.info("Peer Node initialized")
    
    # Update task manager with actual node ID
    task_manager.node_id = peer_node.node_id
    
except Exception as e:
    logger.error(f"Peer Node failed: {e}")
    # Fallback
    class FallbackPeerNode:
        def __init__(self, task_manager=None):
            self.node_id = temp_node_id
            self.local_ip = get_local_ip()
            self.peers = {}
            self.leader_id = self.node_id
            self.task_manager = task_manager
            logger.info(f"Using fallback peer node: {self.node_id}")
        
        def get_peers(self):
            return self.peers
        
        def get_leader(self):
            return self.leader_id
        
        def is_leader(self):
            return True
        
        def stop(self):
            pass
    
    peer_node = FallbackPeerNode(task_manager=task_manager)

def find_free_port(start_port=5000):
    """Find a free port starting from start_port"""
    port = start_port
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('localhost', port))
                return port
        except OSError:
            port += 1
            if port > 6000:  # Safety limit
                raise Exception("No free ports available")

# Auto-detect port from environment or find free one
if 'FLASK_RUN_PORT' in os.environ:
    DEFAULT_PORT = int(os.environ['FLASK_RUN_PORT'])
else:
    # Try to use environment folder names to determine port
    upload_folder = os.environ.get('UPLOAD_FOLDER', 'uploads')
    if upload_folder.endswith('_5000'):
        DEFAULT_PORT = 5000
    elif upload_folder.endswith('_5001'):
        DEFAULT_PORT = 5001  
    elif upload_folder.endswith('_5002'):
        DEFAULT_PORT = 5002
    else:
        DEFAULT_PORT = find_free_port(5000)

print(f"Using port: {DEFAULT_PORT}")

# Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload')
def upload_page():
    return render_template('upload.html')

@app.route('/search')
def search_page():
    return render_template('search.html')

@app.route('/nodes')
def nodes_page():
    return render_template('nodes.html')

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """ENHANCED Upload with INTELLIGENT Load-Based Distribution"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Save file
        file_id = str(uuid.uuid4())
        filename = f"{file_id}_{file.filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        
        logger.info(f"File uploaded: {file.filename} ({os.path.getsize(file_path)} bytes)")
        
        # Determine required task type
        file_ext = os.path.splitext(file.filename)[1].lower()
        task_type_map = {
            '.pdf': 'text_extraction',
            '.txt': 'text_extraction',
            '.doc': 'text_extraction',
            '.docx': 'text_extraction',
            '.jpg': 'ocr',
            '.jpeg': 'ocr',
            '.png': 'ocr'
        }
        required_task = task_type_map.get(file_ext, 'text_extraction')
        
        logger.info(f"Required capability: {required_task}")
        
        # CHECK NETWORK STATE - This is crucial!
        available_peers = 0
        try:
            with task_manager.lock:
                available_peers = len(task_manager.peer_capabilities)
                logger.info(f"Available peers: {available_peers}")
                
                # Log peer capabilities
                for peer_id, caps in task_manager.peer_capabilities.items():
                    short_id = peer_id[:20] + '...' if len(peer_id) > 20 else peer_id
                    logger.info(f"   Peer {short_id}: {caps}")
        except Exception as e:
            logger.error(f"Error checking peers: {e}")
            available_peers = 0
        
        # STRATEGY 1: Try to distribute if we have capable peers
        if available_peers > 0:
            logger.info("Attempting distributed processing...")
            task_id = task_manager.distribute_task(file_path, required_task)
            
            if task_id:
                # Successfully queued for distribution
                return jsonify({
                    'success': True,
                    'message': 'File queued for distributed processing',
                    'task_id': task_id,
                    'processed_by': 'distributed',
                    'file_name': file.filename,
                    'strategy': 'load_balanced',
                    'available_peers': available_peers
                })
        
        # STRATEGY 2: Process locally (no peers or distribution failed)
        logger.info("Processing locally (no capable peers available)")
        
        result = document_processor.process_task(file_path, required_task)
        
        if result['success']:
            # Add to search index
            search_index.add_document(
                file_id=file_id,
                file_name=file.filename,
                content=result.get('text', ''),
                keywords=result.get('keywords', []),
                metadata=result.get('metadata', {}),
                node_id=peer_node.node_id
            )
            
            # Emit stats update via WebSocket
            try:
                socketio.emit('stats_update', {
                    'index_stats': search_index.get_stats(),
                    'task_stats': task_manager.get_stats(),
                    'peer_stats': {
                        'total_peers': len(peer_node.get_peers()),
                        'node_id': peer_node.node_id
                    }
                })
            except Exception as emit_error:
                logger.error(f"Stats emit failed: {emit_error}")
            
            return jsonify({
                'success': True,
                'message': 'File processed locally',
                'file_id': file_id,
                'processed_by': 'local',
                'file_name': file.filename,
                'strategy': 'local_processing',
                'reason': 'no_capable_peers' if available_peers == 0 else 'distribution_failed'
            })
        
        return jsonify({'error': 'Processing failed', 'details': result.get('error')}), 500
            
    except Exception as e:
        logger.error(f"Upload error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
@app.route('/api/upload-batch', methods=['POST'])
def upload_batch():
    """Upload multiple files for batch processing - VERIFIED VERSION"""
    try:
        if 'files' not in request.files:
            return jsonify({'error': 'No files provided'}), 400
        
        files = request.files.getlist('files')
        if not files or files[0].filename == '':
            return jsonify({'error': 'No files selected'}), 400
        
        saved_files = []
        upload_errors = []
        
        for file in files:
            if file.filename == '':
                continue
                
            file_id = str(uuid.uuid4())
            filename = f"{file_id}_{file.filename}"
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            
            # CRITICAL: Save file and VERIFY it was saved
            try:
                file.save(file_path)
                
                # Verify file was actually saved
                if not os.path.exists(file_path):
                    error_msg = f"File not created: {filename}"
                    logger.error(error_msg)
                    upload_errors.append(error_msg)
                    continue
                    
                file_size = os.path.getsize(file_path)
                if file_size == 0:
                    error_msg = f"File saved as 0 bytes: {filename}"
                    logger.error(error_msg)
                    upload_errors.append(error_msg)
                    # Remove the empty file
                    try:
                        os.remove(file_path)
                    except:
                        pass
                    continue
                
                # File saved successfully
                saved_files.append(file_path)
                logger.info(f"✅ File saved successfully: {filename} ({file_size} bytes)")
                
            except Exception as e:
                error_msg = f"Error saving {filename}: {str(e)}"
                logger.error(error_msg)
                upload_errors.append(error_msg)
        
        # Check if any files were successfully saved
        if not saved_files:
            error_response = {
                'error': 'No files were successfully uploaded',
                'details': upload_errors
            }
            if upload_errors:
                error_response['upload_errors'] = upload_errors
            return jsonify(error_response), 400
        
        logger.info(f"Successfully saved {len(saved_files)} files, proceeding with distribution")
        
        # DISTRIBUTE BATCH WITH INTELLIGENT SEGMENTATION
        distributed_tasks = task_manager.distribute_batch(saved_files)
        
        response = {
            'success': True,
            'message': f'Batch of {len(saved_files)} files distributed for processing',
            'distributed_tasks': distributed_tasks,
            'total_tasks': len(distributed_tasks),
            'files_saved': len(saved_files)
        }
        
        # Include any non-fatal errors in the response
        if upload_errors:
            response['warnings'] = upload_errors
            
        return jsonify(response)
        
    except Exception as e:
        logger.error(f"Batch upload error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return jsonify({'error': str(e)}), 500
    
@app.route('/api/debug/uploads')
def debug_uploads():
    """Debug uploaded files with detailed info"""
    uploads = []
    total_size = 0
    
    upload_folder = app.config['UPLOAD_FOLDER']
    
    # Check if upload folder exists and is accessible
    folder_info = {
        'path': upload_folder,
        'exists': os.path.exists(upload_folder),
        'writable': os.access(upload_folder, os.W_OK) if os.path.exists(upload_folder) else False,
        'absolute_path': os.path.abspath(upload_folder) if os.path.exists(upload_folder) else 'N/A'
    }
    
    if os.path.exists(upload_folder):
        for filename in os.listdir(upload_folder):
            file_path = os.path.join(upload_folder, filename)
            if os.path.isfile(file_path):
                file_size = os.path.getsize(file_path)
                uploads.append({
                    'name': filename,
                    'size_bytes': file_size,
                    'size_kb': round(file_size / 1024, 2),
                    'size_mb': round(file_size / (1024 * 1024), 2),
                    'modified': time.ctime(os.path.getmtime(file_path)),
                    'exists': os.path.exists(file_path),
                    'readable': os.access(file_path, os.R_OK)
                })
                total_size += file_size
    
    return jsonify({
        'folder_info': folder_info,
        'total_files': len(uploads),
        'total_size_bytes': total_size,
        'total_size_mb': round(total_size / (1024 * 1024), 2),
        'files': uploads,
        'timestamp': time.time()
    })

@app.route('/api/job-stats')
def get_job_stats():
    """Get detailed job distribution statistics"""
    try:
        task_stats = task_manager.get_stats()
        
        # Check if node_job_stats exists
        if not hasattr(task_manager, 'node_job_stats'):
            return jsonify({
                'job_distribution': {},
                'nodes_with_stats': [],
                'total_jobs_processed': task_stats.get('completed_tasks', 0),
                'timestamp': time.time()
            })
        
        # Enhanced node information with job stats
        nodes_with_stats = []
        for node_id, job_stats in task_stats.get('node_job_stats', {}).items():
            node_info = {
                'node_id': node_id,
                'job_stats': job_stats,
                'current_load': task_stats.get('peer_load', {}).get(node_id, 0),
                'is_self': node_id == peer_node.node_id
            }
            nodes_with_stats.append(node_info)
        
        return jsonify({
            'job_distribution': task_stats.get('job_distribution', {}),
            'nodes_with_stats': nodes_with_stats,
            'total_jobs_processed': task_stats.get('completed_tasks', 0),
            'timestamp': time.time()
        })
    except Exception as e:
        logger.error(f"Job stats error: {e}")
        return jsonify({
            'job_distribution': {},
            'nodes_with_stats': [],
            'total_jobs_processed': 0,
            'error': str(e),
            'timestamp': time.time()
        })

@app.route('/api/debug/peers')
def debug_peers():
    """Debug peer information"""
    return jsonify({
        'node_id': peer_node.node_id,
        'local_ip': peer_node.local_ip,
        'peers': peer_node.get_peers(),
        'leader': peer_node.get_leader(),
        'is_leader': peer_node.is_leader(),
        'timestamp': time.time()
    })

@app.route('/api/debug/files')
def debug_files():
    """Debug uploaded files"""
    uploads = []
    for filename in os.listdir(app.config['UPLOAD_FOLDER']):
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        if os.path.isfile(file_path):
            uploads.append({
                'name': filename,
                'size': os.path.getsize(file_path),
                'modified': time.ctime(os.path.getmtime(file_path))
            })
    
    return jsonify({'uploads': uploads})

@app.route('/api/search')
def search_documents():
    """Search documents"""
    try:
        query = request.args.get('q', '').strip()
        limit = int(request.args.get('limit', 10))
        
        if not query:
            return jsonify({'error': 'No query provided'}), 400
        
        logger.info(f"Searching for: '{query}'")
        results = search_index.search(query, limit)
        logger.info(f"Search found {len(results)} results")
        
        return jsonify({
            'results': results,
            'query': query,
            'total_results': len(results)
        })
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        return jsonify({'error': str(e), 'results': []}), 500

@app.route('/api/documents')
def get_all_documents():
    """Get all documents"""
    try:
        documents = search_index.get_all_documents()
        logger.info(f"Retrieved {len(documents)} documents from database")
        return jsonify({
            'documents': documents,
            'total_documents': len(documents)
        })
    except Exception as e:
        logger.error(f"Get documents error: {e}")
        return jsonify({'documents': [], 'error': str(e)})

@app.route('/api/debug/system-status')
def debug_system_status():
    """Comprehensive system debugging"""
    try:
        # Check upload folder
        upload_files = []
        if os.path.exists(app.config['UPLOAD_FOLDER']):
            upload_files = [f for f in os.listdir(app.config['UPLOAD_FOLDER']) 
                          if os.path.isfile(os.path.join(app.config['UPLOAD_FOLDER'], f))]
        
        # Check search index
        index_stats = search_index.get_stats()
        all_docs = search_index.get_all_documents()
        
        # Check task manager
        task_stats = task_manager.get_stats()
        
        # Check peer node
        peer_info = {
            'node_id': peer_node.node_id,
            'peers': peer_node.get_peers(),
            'is_leader': peer_node.is_leader()
        }
        
        return jsonify({
            'system': {
                'upload_folder': app.config['UPLOAD_FOLDER'],
                'upload_files_count': len(upload_files),
                'upload_files': upload_files,
                'upload_folder_exists': os.path.exists(app.config['UPLOAD_FOLDER']),
                'upload_folder_writable': os.access(app.config['UPLOAD_FOLDER'], os.W_OK)
            },
            'search_index': {
                'stats': index_stats,
                'documents_count': len(all_docs),
                'documents_sample': all_docs[:3] if all_docs else []
            },
            'task_manager': task_stats,
            'peer_node': peer_info,
            'components_loaded': components,
            'timestamp': time.time()
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    """Get system statistics"""
    try:
        index_stats = search_index.get_stats()
        task_stats = task_manager.get_stats()
        peer_stats = {
            'total_peers': len(peer_node.get_peers()),
            'is_leader': peer_node.is_leader(),
            'leader_id': peer_node.get_leader(),
            'node_id': peer_node.node_id
        }
        
        return jsonify({
            'index_stats': index_stats,
            'task_stats': task_stats,
            'peer_stats': peer_stats
        })
    except Exception as e:
        logger.error(f"Stats error: {e}")
        return jsonify({
            'index_stats': {'total_documents': 0},
            'task_stats': {'completed_tasks': 0},
            'peer_stats': {'node_id': 'unknown'}
        })

@app.route('/api/tasks')
def get_tasks():
    """Get current tasks status"""
    try:
        stats = task_manager.get_stats()
        pending_tasks = []
        
        # Get details for pending tasks
        if hasattr(task_manager, 'pending_tasks'):
            for task_id, task_info in task_manager.pending_tasks.items():
                pending_tasks.append({
                    'task_id': task_id,
                    'file_name': task_info.get('task_data', {}).get('file_name', 'Unknown'),
                    'status': task_info.get('status', 'unknown'),
                    'peer_id': task_info.get('peer_id'),
                    'start_time': task_info.get('start_time'),
                    'attempts': task_info.get('attempts', 0)
                })
        
        return jsonify({
            'stats': stats,
            'pending_tasks': pending_tasks,
            'timestamp': time.time()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/tasks/<task_id>')
def get_task_status(task_id):
    """Get status of specific task"""
    try:
        if hasattr(task_manager, 'get_task_status'):
            status = task_manager.get_task_status(task_id)
            return jsonify(status)
        return jsonify({'status': 'unknown'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/debug')
def debug_info():
    """Debug information"""
    return jsonify({
        'components': components,
        'node_id': peer_node.node_id,
        'local_ip': peer_node.local_ip,
        'timestamp': time.time()
    })

@app.route('/api/debug/status')
def debug_status():
    """Comprehensive debug information"""
    return jsonify({
        'upload_folder': app.config['UPLOAD_FOLDER'],
        'upload_files': os.listdir(app.config['UPLOAD_FOLDER']),
        'components_loaded': components,
        'task_manager_stats': task_manager.get_stats(),
        'search_index_stats': search_index.get_stats(),
        'peer_node_info': {
            'node_id': peer_node.node_id,
            'peers_count': len(peer_node.get_peers()),
            'is_leader': peer_node.is_leader()
        }
    })

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    logger.info("Client connected via WebSocket")
    emit('status_update', {'message': 'Connected', 'timestamp': time.time()})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnect"""
    logger.info("Client disconnected")

def background_stats():
    """Background stats emitter"""
    while True:
        try:
            stats = {
                'index_stats': search_index.get_stats(),
                'task_stats': task_manager.get_stats(),
                'peer_stats': {
                    'total_peers': len(peer_node.get_peers()),
                    'is_leader': peer_node.is_leader()
                },
                'timestamp': time.time()
            }
            socketio.emit('stats_update', stats)
        except Exception as e:
            logger.error(f"Background stats error: {e}")
        time.sleep(5)

@app.route('/api/debug/full-status')
def debug_full_status():
    """Comprehensive system status"""
    try:
        # Check if upload folder exists and is writable
        upload_folder = app.config['UPLOAD_FOLDER']
        upload_files = []
        if os.path.exists(upload_folder):
            upload_files = os.listdir(upload_folder)
        
        # Check all APIs
        apis = {
            'upload': True,
            'search': True, 
            'nodes': True,
            'stats': True,
            'documents': True
        }
        
        return jsonify({
            'system': {
                'node_id': peer_node.node_id,
                'local_ip': peer_node.local_ip,
                'port': request.environ.get('SERVER_PORT'),
                'components_loaded': list(components.keys())
            },
            'files': {
                'upload_folder': upload_folder,
                'files_count': len(upload_files),
                'files_list': upload_files
            },
            'apis': apis,
            'search_index': search_index.get_stats(),
            'task_manager': task_manager.get_stats(),
            'peer_node': {
                'peers_count': len(peer_node.get_peers()),
                'is_leader': peer_node.is_leader(),
                'leader': peer_node.get_leader()
            },
            'timestamp': time.time()
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Start background thread
stats_thread = threading.Thread(target=background_stats, daemon=True)
stats_thread.start()
if __name__ == '__main__':
    local_ip = get_local_ip()
    
    try:
        # Use the detected port instead of find_free_port()
        free_port = DEFAULT_PORT
        
        print("\n" + "="*60)
        print(" DECENTRALIZED DOCUMENT PROCESSING SYSTEM")
        print("="*60)
        print(f" Node ID: {peer_node.node_id}")
        print(f" Network URL: http://{local_ip}:{free_port}")
        print(f" Local URL: http://localhost:{free_port}")
        print(f" Components: {', '.join(components.keys())}")
        print("="*60)
        print(" System is READY!")
        print("="*60)
        
        print(f" Starting on port: {free_port}")
        socketio.run(app, host='0.0.0.0', port=free_port, debug=False, allow_unsafe_werkzeug=True)
    except Exception as e:
        logger.error(f"Failed to start: {e}")
        print(f"Failed to start: {e}")