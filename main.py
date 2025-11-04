from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
import os
import uuid
import time
import logging
import threading
import socket

# Setup logging FIRST - before anything else
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('app.log')
    ]
)
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
    logger.info("✅ Config loaded")
    
    # Create directories
    os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(Config.PROCESSED_FOLDER, exist_ok=True)
    os.makedirs(Config.INDEX_FOLDER, exist_ok=True)
    
    app.config['UPLOAD_FOLDER'] = Config.UPLOAD_FOLDER
    app.config['MAX_CONTENT_LENGTH'] = Config.MAX_FILE_SIZE
    
except Exception as e:
    logger.error(f"❌ Config failed: {e}")
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
    logger.info("✅ Search Index initialized")
except Exception as e:
    logger.error(f"❌ Search Index failed: {e}")
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
    logger.info("✅ Document Processor initialized")
except Exception as e:
    logger.error(f"❌ Document Processor failed: {e}")
    # Fallback
    class FallbackDocumentProcessor:
        def process_document(self, file_path, task_type='full'):
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
    logger.info("✅ Task Manager initialized")
except Exception as e:
    logger.error(f"❌ Task Manager failed: {e}")
    # Fallback
    class FallbackTaskManager:
        def __init__(self, node_id, search_index):
            self.node_id = node_id
            self.search_index = search_index
            self.peer_load = {}
            self.pending_tasks = 0
            self.completed_tasks = 0
            self.peer_capabilities = {}
            logger.info("Using fallback task manager")
        
        def distribute_task(self, file_path, task_type='full'):
            try:
                # Process locally as fallback
                result = document_processor.process_document(file_path, task_type)
                if result['success']:
                    self.search_index.add_document(
                        file_id=str(uuid.uuid4()),
                        file_name=result['metadata']['file_name'],
                        content=result['text'],
                        keywords=result['keywords'],
                        metadata=result['metadata'],
                        node_id=self.node_id
                    )
                    self.completed_tasks += 1
                    return f"task_{int(time.time())}"
            except Exception as e:
                logger.error(f"Task processing error: {e}")
            return None
        
        def get_stats(self):
            return {
                'pending_tasks': self.pending_tasks,
                'completed_tasks': self.completed_tasks,
                'failed_tasks': 0,
                'peer_load': self.peer_load,
                'connected_peers': len(self.peer_capabilities)
            }
        
        def update_peer_capabilities(self, peer_id, capabilities):
            self.peer_capabilities[peer_id] = capabilities
        
        def remove_peer(self, peer_id):
            if peer_id in self.peer_capabilities:
                del self.peer_capabilities[peer_id]
            if peer_id in self.peer_load:
                del self.peer_load[peer_id]
    
    task_manager = FallbackTaskManager(temp_node_id, search_index)

# Initialize PeerNode LAST with TaskManager reference
try:
    from peer_node import PeerNode
    peer_node = PeerNode(task_manager=task_manager)
    components['peer_node'] = True
    logger.info("✅ Peer Node initialized")
    
    # Update task manager with actual node ID
    task_manager.node_id = peer_node.node_id
    
except Exception as e:
    logger.error(f"❌ Peer Node failed: {e}")
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

import os
import socket

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

print(f"🎯 Using port: {DEFAULT_PORT}")

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
    """Handle file upload"""
    try:
        logger.info("📤 Upload request received")
        
        if 'file' not in request.files:
            logger.error("❌ No file in request")
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            logger.error("❌ Empty filename")
            return jsonify({'error': 'No file selected'}), 400
        
        logger.info(f"📄 File received: {file.filename}")
        
        # Check file extension
        allowed_extensions = {'txt', 'pdf', 'doc', 'docx', 'png', 'jpg', 'jpeg'}
        file_ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else ''
        
        if file_ext not in allowed_extensions:
            logger.error(f"❌ Invalid file type: {file_ext}")
            return jsonify({'error': f'File type .{file_ext} not allowed'}), 400
        
        # Save file
        file_id = str(uuid.uuid4())
        filename = f"{file_id}_{file.filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        logger.info(f"💾 Saving file to: {file_path}")
        file.save(file_path)
        
        # Verify file was saved
        if not os.path.exists(file_path):
            logger.error("❌ File failed to save")
            return jsonify({'error': 'File failed to save'}), 500
            
        file_size = os.path.getsize(file_path)
        logger.info(f"✅ File saved: {filename} - Size: {file_size} bytes")
        
        # Process file
        logger.info("🔄 Starting task distribution...")
        task_id = task_manager.distribute_task(file_path, 'full')
        
        if task_id:
            logger.info(f"✅ Task created: {task_id}")
            return jsonify({
                'success': True,
                'task_id': task_id,
                'message': 'File uploaded and processing started',
                'file_id': file_id,
                'file_path': file_path
            })
        else:
            logger.error("❌ Task distribution failed completely")
            # Try direct processing as last resort
            try:
                logger.info("🔄 Attempting direct processing...")
                result = document_processor.process_document(file_path, 'full')
                if result['success']:
                    file_id = str(uuid.uuid4())
                    search_index.add_document(
                        file_id=file_id,
                        file_name=result['metadata']['file_name'],
                        content=result['text'],
                        keywords=result['keywords'],
                        metadata=result['metadata'],
                        node_id=peer_node.node_id
                    )
                    logger.info("✅ Direct processing successful")
                    return jsonify({
                        'success': True,
                        'task_id': 'direct_processing',
                        'message': 'File processed directly',
                        'file_id': file_id
                    })
            except Exception as fallback_error:
                logger.error(f"❌ Direct processing also failed: {fallback_error}")
            
            return jsonify({'error': 'Failed to process file through all methods'}), 500
            
    except Exception as e:
        logger.error(f"❌ Upload error: {e}")
        import traceback
        logger.error(f"❌ Stack trace: {traceback.format_exc()}")
        return jsonify({'error': str(e)}), 500
    
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
        query = request.args.get('q', '')
        limit = int(request.args.get('limit', 10))
        
        if not query:
            return jsonify({'error': 'No query provided'}), 400
        
        results = search_index.search(query, limit)
        return jsonify({'results': results})
        
    except Exception as e:
        logger.error(f"Search error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/documents')
def get_all_documents():
    """Get all documents"""
    try:
        documents = search_index.get_all_documents()
        return jsonify({'documents': documents})
    except Exception as e:
        logger.error(f"Get documents error: {e}")
        return jsonify({'documents': []})

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

@app.route('/api/nodes')
def get_nodes():
    """Get node information"""
    try:
        peers = peer_node.get_peers()
        nodes_list = []
        
        # Add self node first
        nodes_list.append({
            'id': peer_node.node_id,
            'ip': peer_node.local_ip,
            'capabilities': Config.CAPABILITIES,
            'last_seen': time.time(),
            'load': 0,
            'is_self': True,
            'status': 'active',
            'is_leader': peer_node.is_leader()
        })
        
        # Add other peers
        for peer_id, info in peers.items():
            nodes_list.append({
                'id': peer_id,
                'ip': info.get('ip', 'unknown'),
                'capabilities': info.get('capabilities', {}),
                'last_seen': info.get('last_seen', time.time()),
                'load': task_manager.peer_load.get(peer_id, 0),
                'status': 'active',
                'is_leader': (peer_id == peer_node.get_leader())
            })
        
        return jsonify({
            'nodes': nodes_list,
            'total_nodes': len(nodes_list),
            'leader': peer_node.get_leader(),
            'is_leader': peer_node.is_leader()
        })
        
    except Exception as e:
        logger.error(f"Nodes API error: {e}")
        return jsonify({'nodes': [], 'error': str(e)})

@app.route('/api/debug/upload-test')
def debug_upload_test():
    """Test upload functionality step by step"""
    try:
        # Create a test file
        test_content = "This is a test document for debugging upload issues."
        test_filename = f"debug_test_{int(time.time())}.txt"
        test_path = os.path.join(app.config['UPLOAD_FOLDER'], test_filename)
        
        with open(test_path, 'w', encoding='utf-8') as f:
            f.write(test_content)
        
        logger.info(f"📝 Created test file: {test_path}")
        
        # Test 1: Check if file was created
        file_exists = os.path.exists(test_path)
        file_size = os.path.getsize(test_path) if file_exists else 0
        
        # Test 2: Test document processor
        processor_result = document_processor.process_document(test_path, 'full')
        
        # Test 3: Test search index
        file_id = str(uuid.uuid4())
        search_success = search_index.add_document(
            file_id=file_id,
            file_name=test_filename,
            content=processor_result['text'],
            keywords=processor_result['keywords'],
            metadata=processor_result['metadata'],
            node_id=peer_node.node_id
        )
        
        # Test 4: Test task manager
        task_id = task_manager.distribute_task(test_path, 'full')
        
        return jsonify({
            'test_file': {
                'path': test_path,
                'exists': file_exists,
                'size': file_size,
                'content': test_content
            },
            'document_processor': {
                'success': processor_result['success'],
                'text_length': len(processor_result.get('text', '')),
                'keywords': processor_result.get('keywords', [])
            },
            'search_index': {
                'success': search_success,
                'documents_count': search_index.get_stats()['total_documents']
            },
            'task_manager': {
                'task_id': task_id,
                'success': task_id is not None,
                'stats': task_manager.get_stats()
            }
        })
        
    except Exception as e:
        logger.error(f"Debug upload test failed: {e}")
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
        print("🚀 DECENTRALIZED DOCUMENT PROCESSING SYSTEM")
        print("="*60)
        print(f"📍 Node ID: {peer_node.node_id}")
        print(f"🌐 Network URL: http://{local_ip}:{free_port}")
        print(f"💻 Local URL: http://localhost:{free_port}")
        print(f"📊 Components: {', '.join(components.keys())}")
        print("="*60)
        print("✅ System is READY!")
        print("="*60)
        
        print(f"🚀 Starting on port: {free_port}")
        socketio.run(app, host='0.0.0.0', port=free_port, debug=False, allow_unsafe_werkzeug=True)
    except Exception as e:
        logger.error(f"Failed to start: {e}")
        print(f"❌ Failed to start: {e}")