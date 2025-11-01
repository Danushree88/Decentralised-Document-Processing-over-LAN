from flask import Flask, render_template, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
import os
import uuid
import time
import logging
import threading
import socket  # Add this import

# Setup logging first
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_local_ip():
    """Get local IP address - Windows compatible without netifaces"""
    try:
        # Method 1: Connect to external server to get local IP
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            return ip
    except:
        try:
            # Method 2: Get by hostname
            hostname = socket.gethostname()
            ip = socket.gethostbyname(hostname)
            if ip and ip != '127.0.0.1':
                return ip
        except:
            pass
        
        # Fallback to localhost
        return '127.0.0.1'

# Import components with error handling
try:
    from config import Config
    logger.info("✅ Config imported successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import Config: {e}")
    # Fallback config
    class Config:
        UPLOAD_FOLDER = 'uploads'
        PROCESSED_FOLDER = 'processed'
        MAX_FILE_SIZE = 10 * 1024 * 1024
        CAPABILITIES = {'text_extraction': True, 'file_processing': True}

try:
    from search_index import SearchIndex
    logger.info("✅ SearchIndex imported successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import SearchIndex: {e}")
    SearchIndex = None

try:
    from peer_node import PeerNode
    logger.info("✅ PeerNode imported successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import PeerNode: {e}")
    PeerNode = None

try:
    from task_manager import TaskManager
    logger.info("✅ TaskManager imported successfully")
except ImportError as e:
    logger.error(f"❌ Failed to import TaskManager: {e}")
    TaskManager = None

app = Flask(__name__)
app.config['SECRET_KEY'] = 'decentralized-doc-processor'
app.config['UPLOAD_FOLDER'] = Config.UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = Config.MAX_FILE_SIZE

socketio = SocketIO(app, cors_allowed_origins="*")

# Create necessary directories
os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
os.makedirs(Config.PROCESSED_FOLDER, exist_ok=True)

# Initialize components with error handling
# Search Index
try:
    if SearchIndex:
        search_index = SearchIndex()
        logger.info("✅ Search index initialized successfully")
    else:
        raise ImportError("SearchIndex not available")
except Exception as e:
    logger.error(f"❌ Failed to initialize search index: {e}")
    # Fallback search index
    class FallbackSearchIndex:
        def __init__(self):
            self.documents = []
            logger.info("Using fallback search index")
        def add_document(self, *args, **kwargs):
            logger.info("Fallback: Document added")
            return True
        def search(self, *args, **kwargs):
            return []
        def get_all_documents(self):
            return self.documents
        def get_stats(self):
            return {'total_documents': len(self.documents), 'total_size_mb': 0, 'total_words': 0}
    search_index = FallbackSearchIndex()

# Peer Node
try:
    if PeerNode:
        peer_node = PeerNode()
        logger.info("✅ Peer node initialized successfully")
    else:
        raise ImportError("PeerNode not available")
except Exception as e:
    logger.error(f"❌ Failed to initialize peer node: {e}")
    # Fallback peer node
    class FallbackPeerNode:
        def __init__(self):
            self.node_id = "fallback_node_local"
            self.local_ip = "127.0.0.1"
            logger.info("Using fallback peer node")
        def get_peers(self):
            return {}
        def get_leader(self):
            return self.node_id
        def is_leader(self):
            return True
        def stop(self):
            pass
    peer_node = FallbackPeerNode()

# Task Manager
try:
    if TaskManager:
        task_manager = TaskManager(peer_node.node_id, search_index)
        logger.info("✅ Task manager initialized successfully")
    else:
        raise ImportError("TaskManager not available")
except Exception as e:
    logger.error(f"❌ Failed to initialize task manager: {e}")
    # Fallback task manager
    class FallbackTaskManager:
        def __init__(self, node_id, search_index):
            self.node_id = node_id
            self.search_index = search_index
            self.pending_tasks = {}
            self.completed_tasks = {}
            self.failed_tasks = {}
            self.peer_load = {}
            logger.info("Using fallback task manager")
        def distribute_task(self, file_path, task_type='full'):
            logger.info(f"Fallback: Processing {file_path} locally")
            # Simple local processing
            try:
                from document_processor import DocumentProcessor
                processor = DocumentProcessor()
                result = processor.process_document(file_path, task_type)
                
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
                    return f"local_task_{file_id}"
            except Exception as e:
                logger.error(f"Fallback processing error: {e}")
            return None
        def update_peer_capabilities(self, *args, **kwargs):
            pass
        def remove_peer(self, *args, **kwargs):
            pass
        def get_stats(self):
            return {
                'pending_tasks': len(self.pending_tasks),
                'completed_tasks': len(self.completed_tasks),
                'failed_tasks': len(self.failed_tasks),
                'peer_load': dict(self.peer_load),
                'connected_peers': 0
            }
    task_manager = FallbackTaskManager(peer_node.node_id, search_index)

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
    """Handle file upload and trigger processing"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        # Validate file extension
        allowed_extensions = {'.pdf', '.docx', '.doc', '.txt', '.jpg', '.jpeg', '.png'}
        file_ext = os.path.splitext(file.filename)[1].lower()
        if file_ext not in allowed_extensions:
            return jsonify({'error': f'File type {file_ext} not supported. Use: {", ".join(allowed_extensions)}'}), 400
        
        # Validate file size
        file.seek(0, os.SEEK_END)
        file_length = file.tell()
        file.seek(0)
        
        if file_length > Config.MAX_FILE_SIZE:
            return jsonify({'error': f'File too large. Maximum size is {Config.MAX_FILE_SIZE // (1024*1024)}MB'}), 400
        
        # Save file
        file_id = str(uuid.uuid4())
        filename = f"{file_id}_{file.filename}"
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)
        logger.info(f"File uploaded: {filename} ({file_length} bytes)")
        
        # Determine task type based on file extension
        if file_ext in ['.jpg', '.jpeg', '.png']:
            task_type = 'ocr'
        elif file_ext in ['.pdf', '.docx', '.doc', '.txt']:
            task_type = 'full'
        else:
            task_type = 'text_extraction'
        
        # Distribute task
        task_id = task_manager.distribute_task(file_path, task_type)
        
        if task_id:
            return jsonify({
                'success': True,
                'task_id': task_id,
                'message': f'File uploaded and queued for processing'
            })
        else:
            # Fallback to local processing
            logger.info("No peers available, processing locally")
            from document_processor import DocumentProcessor
            processor = DocumentProcessor()
            result = processor.process_document(file_path, task_type)
            
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
                return jsonify({
                    'success': True,
                    'task_id': f"local_{file_id}",
                    'message': f'File processed locally and added to index'
                })
            else:
                return jsonify({
                    'error': f'Processing failed: {result.get("error", "Unknown error")}'
                }), 500
            
    except Exception as e:
        logger.error(f"Upload error: {e}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

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
        return jsonify({'error': f'Search failed: {str(e)}'}), 500

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
            'index_stats': {},
            'task_stats': {},
            'peer_stats': {'node_id': 'unknown', 'is_leader': False}
        })


@app.route('/api/nodes')
def get_nodes():
    """Get node information"""
    try:
        print(f"DEBUG: Getting nodes, peer_node type: {type(peer_node)}")
        print(f"DEBUG: peer_node attributes: {dir(peer_node)}")
        
        # Get peers from peer_node
        peers = {}
        if hasattr(peer_node, 'get_peers'):
            peers = peer_node.get_peers()
            print(f"DEBUG: Found {len(peers)} peers: {list(peers.keys())}")
        else:
            print("DEBUG: peer_node has no get_peers method")
        
        nodes_list = []
        
        # Add peer nodes
        for peer_id, peer_info in peers.items():
            print(f"DEBUG: Processing peer {peer_id}: {peer_info}")
            nodes_list.append({
                'id': peer_id,
                'ip': peer_info.get('ip', 'unknown'),
                'capabilities': peer_info.get('capabilities', {}),
                'last_seen': peer_info.get('last_seen', time.time()),
                'load': getattr(task_manager, 'peer_load', {}).get(peer_id, 0),
                'is_self': False,
                'is_leader': getattr(peer_node, 'is_leader', lambda: False)() and getattr(peer_node, 'get_leader', lambda: '')() == peer_id
            })
        
        # Add self node (ALWAYS include this)
        self_node = {
            'id': getattr(peer_node, 'node_id', 'local_node'),
            'ip': getattr(peer_node, 'local_ip', '127.0.0.1'),
            'capabilities': getattr(Config, 'CAPABILITIES', {'text_extraction': True}),
            'last_seen': time.time(),
            'load': 0,
            'is_self': True,
            'is_leader': getattr(peer_node, 'is_leader', lambda: True)()
        }
        nodes_list.append(self_node)
        
        print(f"DEBUG: Returning {len(nodes_list)} nodes: {[n['id'] for n in nodes_list]}")
        
        return jsonify({'nodes': nodes_list})
        
    except Exception as e:
        print(f"ERROR in /api/nodes: {e}")
        import traceback
        traceback.print_exc()
        
        # Fallback - always return at least the current node
        return jsonify({'nodes': [{
            'id': 'local_node',
            'ip': '127.0.0.1',
            'capabilities': {'text_extraction': True, 'file_processing': True},
            'last_seen': time.time(),
            'load': 0,
            'is_self': True,
            'is_leader': True
        }]})
    
@app.route('/api/debug/network')
def debug_network():
    """Debug network status"""
    try:
        peers = {}
        if hasattr(peer_node, 'get_peers'):
            peers = peer_node.get_peers()
            
        return jsonify({
            'status': 'ok',
            'node_id': getattr(peer_node, 'node_id', 'unknown'),
            'local_ip': getattr(peer_node, 'local_ip', 'unknown'),
            'is_leader': getattr(peer_node, 'is_leader', lambda: False)(),
            'leader_id': getattr(peer_node, 'get_leader', lambda: 'unknown')(),
            'peer_count': len(peers),
            'peers': list(peers.keys()),
            'timestamp': time.time()
        })
    except Exception as e:
        return jsonify({'status': 'error', 'error': str(e)})

@app.route('/api/debug/status')
def debug_status():
    """Overall system status"""
    return jsonify({
        'flask_running': True,
        'peer_node_available': hasattr(peer_node, 'get_peers'),
        'task_manager_available': hasattr(task_manager, 'distribute_task'),
        'search_index_available': hasattr(search_index, 'search'),
        'timestamp': time.time()
    })

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'node_id': peer_node.node_id,
        'timestamp': time.time()
    })

@socketio.on('connect')
def handle_connect():
    """Handle client connection"""
    logger.info("Client connected to WebSocket")
    emit('status_update', {'message': 'Connected to document processor', 'node_id': peer_node.node_id})

@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnect"""
    logger.info("Client disconnected from WebSocket")

def background_stats_emitter():
    """Emit periodic stats updates"""
    logger.info("Starting background stats emitter")
    while True:
        try:
            stats = {
                'index_stats': search_index.get_stats(),
                'task_stats': task_manager.get_stats(),
                'peer_stats': {
                    'total_peers': len(peer_node.get_peers()),
                    'is_leader': peer_node.is_leader(),
                    'node_id': peer_node.node_id
                },
                'timestamp': time.time()
            }
            socketio.emit('stats_update', stats)
            logger.debug("Emitted stats update")
        except Exception as e:
            logger.error(f"Error emitting stats: {e}")
        time.sleep(5)

# Start background thread for stats
try:
    stats_thread = threading.Thread(target=background_stats_emitter, daemon=True)
    stats_thread.start()
    logger.info("Background stats emitter started")
except Exception as e:
    logger.error(f"Failed to start stats emitter: {e}")

def cleanup_on_exit():
    """Cleanup resources on exit"""
    logger.info("Shutting down...")
    try:
        if hasattr(peer_node, 'stop'):
            peer_node.stop()
        if hasattr(search_index, 'close'):
            search_index.close()
    except Exception as e:
        logger.error(f"Cleanup error: {e}")

import atexit
atexit.register(cleanup_on_exit)

if __name__ == '__main__':
    try:
        # Get local IP directly from the function
        local_ip = get_local_ip()
        logger.info(f"🚀 Starting Decentralized Document Processor...")
        logger.info(f"📋 Node ID: {peer_node.node_id}")
        logger.info(f"🌐 Dashboard: http://{local_ip}:5000")
        logger.info(f"💻 Local access: http://localhost:5000")
        logger.info(f"📁 Upload folder: {Config.UPLOAD_FOLDER}")
        logger.info(f"💡 Capabilities: {Config.CAPABILITIES}")
        logger.info(f"🔍 Search index: {Config.INDEX_FOLDER}")
        
        print("\n" + "="*50)
        print("🚀 Decentralized Document Processing System")
        print("="*50)
        print(f"✅ Node ID: {peer_node.node_id}")
        print(f"🌐 Network URL: http://{local_ip}:5000")
        print(f"💻 Local URL: http://localhost:5000")
        print(f"📊 System is running and ready!")
        print("="*50)
        print("💡 Open your browser and navigate to one of the URLs above")
        print("="*50)
        
        socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)
        
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        print(f"❌ Fatal error: {e}")
        print("💡 Try deleting the search_index folder and running again")