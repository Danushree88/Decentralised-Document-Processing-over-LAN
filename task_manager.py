import threading
import time
import json
import socket
import os
import uuid
import logging
from collections import deque, defaultdict
from config import Config

class TaskManager:
    def __init__(self, node_id, search_index):
        self.node_id = node_id
        self.search_index = search_index
        self.pending_tasks = {}
        self.completed_tasks = {}
        self.failed_tasks = {}
        self.task_queue = deque()
        self.peer_load = defaultdict(int)
        self.peer_capabilities = {}
        self.lock = threading.Lock()
        
        # Task server setup
        self.task_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.task_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.task_socket.bind(('0.0.0.0', Config.TASK_PORT))
        self.task_socket.listen(5)
        
        # File server setup
        self.file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.file_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.file_socket.bind(('0.0.0.0', Config.FILE_PORT))
        self.file_socket.listen(5)
        
        # Start servers
        self.running = True
        threading.Thread(target=self._task_server, daemon=True).start()
        threading.Thread(target=self._file_server, daemon=True).start()
        threading.Thread(target=self._task_monitor, daemon=True).start()
    
    def _task_server(self):
        """Handle incoming task requests"""
        while self.running:
            try:
                client_socket, addr = self.task_socket.accept()
                threading.Thread(target=self._handle_task_request, args=(client_socket, addr), daemon=True).start()
            except Exception as e:
                if self.running:
                    logging.error(f"Task server error: {e}")
    
    def _file_server(self):
        """Handle file transfers"""
        while self.running:
            try:
                client_socket, addr = self.file_socket.accept()
                threading.Thread(target=self._handle_file_transfer, args=(client_socket, addr), daemon=True).start()
            except Exception as e:
                if self.running:
                    logging.error(f"File server error: {e}")
    
    def _handle_task_request(self, client_socket, addr):
        """Handle incoming task execution request"""
        try:
            # Receive task data
            data = b""
            while True:
                chunk = client_socket.recv(4096)
                if not chunk:
                    break
                data += chunk
                if b"<END_TASK>" in data:
                    break
            
            task_data = json.loads(data.decode('utf-8').replace("<END_TASK>", ""))
            
            # Process the task
            from document_processor import DocumentProcessor
            processor = DocumentProcessor()
            result = processor.process_document(task_data['file_path'], task_data['task_type'])
            
            # Send result back
            response = json.dumps(result).encode('utf-8') + b"<END_RESULT>"
            client_socket.sendall(response)
            
        except Exception as e:
            logging.error(f"Error handling task request: {e}")
            error_response = json.dumps({'success': False, 'error': str(e)}).encode('utf-8') + b"<END_RESULT>"
            client_socket.sendall(error_response)
        finally:
            client_socket.close()
    
    def _handle_file_transfer(self, client_socket, addr):
        """Handle file upload/download"""
        try:
            # Receive file metadata
            metadata_data = b""
            while True:
                chunk = client_socket.recv(1024)
                if not chunk:
                    break
                metadata_data += chunk
                if b"<END_METADATA>" in metadata_data:
                    break
            
            metadata = json.loads(metadata_data.decode('utf-8').replace("<END_METADATA>", ""))
            file_path = os.path.join(Config.UPLOAD_FOLDER, metadata['file_name'])
            
            # Receive file data
            with open(file_path, 'wb') as f:
                while True:
                    data = client_socket.recv(4096)
                    if not data or data.endswith(b"<END_FILE>"):
                        if data:
                            f.write(data[:-10])  # Remove end marker
                        break
                    f.write(data)
            
            # Send acknowledgment
            client_socket.sendall(b"FILE_RECEIVED<END_ACK>")
            
        except Exception as e:
            logging.error(f"Error handling file transfer: {e}")
        finally:
            client_socket.close()
    
    def distribute_task(self, file_path, task_type='full'):
        """Distribute task to peer - FIXED VERSION"""
        task_id = str(uuid.uuid4())
        
        logging.info(f"🎯 Starting task distribution for: {os.path.basename(file_path)}")
        
        # Verify file exists and is readable
        if not os.path.exists(file_path):
            logging.error(f"❌ File does not exist: {file_path}")
            return None
        
        try:
            file_size = os.path.getsize(file_path)
            logging.info(f"📄 File verified: {os.path.basename(file_path)} ({file_size} bytes)")
        except Exception as e:
            logging.error(f"❌ Cannot access file {file_path}: {e}")
            return None
        
        # First, try to find peers for distribution
        peers = self._find_suitable_peers(task_type)
        logging.info(f"🔍 Found {len(peers)} suitable peers")
        
        if peers:
            # Try to distribute to peers
            peer = peers[0]
            logging.info(f"📤 Attempting to distribute to peer: {peer['id']}")
            
            self.peer_load[peer['id']] += 1
            
            task_data = {
                'task_id': task_id,
                'file_path': file_path,
                'task_type': task_type,
                'requester_id': self.node_id
            }
            
            self.pending_tasks[task_id] = {
                'task_data': task_data,
                'peer_id': peer['id'],
                'start_time': time.time(),
                'status': 'distributed'
            }
            
            # Send task to peer
            if self._send_task_to_peer(peer, task_data):
                logging.info(f"✅ Task successfully distributed to peer {peer['id']}")
                return task_id
            else:
                logging.error(f"❌ Failed to distribute to peer {peer['id']}, falling back to local processing")
                del self.pending_tasks[task_id]
                self.peer_load[peer['id']] -= 1
                # Continue to local processing
        
        # Fallback to local processing
        logging.info("🔄 Processing file locally (no suitable peers or distribution failed)")
        
        try:
            from document_processor import DocumentProcessor
            processor = DocumentProcessor()
            result = processor.process_document(file_path, task_type)
            
            if result['success']:
                file_id = str(uuid.uuid4())
                # Add to search index
                index_success = self.search_index.add_document(
                    file_id=file_id,
                    file_name=result['metadata']['file_name'],
                    content=result['text'],
                    keywords=result['keywords'],
                    metadata=result['metadata'],
                    node_id=self.node_id
                )
                
                if index_success:
                    self.completed_tasks += 1
                    logging.info(f"✅ Local processing completed for {os.path.basename(file_path)}")
                    logging.info(f"📊 Extracted {len(result['text'])} characters, {len(result['keywords'])} keywords")
                    return task_id
                else:
                    logging.error(f"❌ Failed to add document to search index: {os.path.basename(file_path)}")
                    return None
            else:
                logging.error(f"❌ Document processing failed: {result.get('error', 'Unknown error')}")
                return None
                
        except Exception as e:
            logging.error(f"❌ Local processing failed for {file_path}: {e}")
            import traceback
            logging.error(f"❌ Stack trace: {traceback.format_exc()}")
            return None
    
    def _find_suitable_peers(self, task_type):
        """Find peers suitable for the given task type"""
        suitable_peers = []
        
        with self.lock:
            # If no peer capabilities, return empty list (process locally)
            if not self.peer_capabilities:
                return []
                
            for peer_id, capabilities in self.peer_capabilities.items():
                # Check if peer has the required capability
                if task_type == 'ocr' and capabilities.get('ocr', False):
                    suitable_peers.append({'id': peer_id, 'capabilities': capabilities, 'ip': capabilities.get('ip', peer_id.split('_')[0])})
                elif task_type == 'nlp' and capabilities.get('nlp', False):
                    suitable_peers.append({'id': peer_id, 'capabilities': capabilities, 'ip': capabilities.get('ip', peer_id.split('_')[0])})
                elif capabilities.get('text_extraction', False) or capabilities.get('file_processing', False):
                    suitable_peers.append({'id': peer_id, 'capabilities': capabilities, 'ip': capabilities.get('ip', peer_id.split('_')[0])})
        
        # Sort by load (least loaded first)
        suitable_peers.sort(key=lambda x: self.peer_load.get(x['id'], 0))
        
        logging.info(f"🔍 Found {len(suitable_peers)} suitable peers for {task_type} task")
        for peer in suitable_peers:
            logging.info(f"   - {peer['id']} (load: {self.peer_load.get(peer['id'], 0)})")
        
        return suitable_peers
    
    def _send_task_to_peer(self, peer, task_data):
        """Send task to peer node - FIXED VERSION"""
        try:
            # Use peer IP instead of node_id for connection
            peer_ip = None
            peer_task_port = Config.TASK_PORT
            peer_file_port = Config.FILE_PORT
            
            # Get peer connection info - avoid circular import
            # Instead of importing peer_node, use the peer info we already have
            if 'ip' in peer:
                # If peer info already contains IP (from capabilities update)
                peer_ip = peer['ip']
            else:
                # Try to extract IP from peer ID (format: IP_UUID)
                peer_id_parts = peer['id'].split('_')
                if len(peer_id_parts) >= 2:
                    peer_ip = peer_id_parts[0]
                else:
                    logging.error(f"Cannot extract IP from peer ID: {peer['id']}")
                    return False
            
            if not peer_ip or peer_ip == 'unknown':
                logging.error(f"No valid IP address for peer {peer['id']}")
                return False
            
            logging.info(f"Attempting to send task to {peer_ip}:{peer_task_port}")
            
            # First, send the file
            file_sent = self._send_file_to_peer(peer_ip, peer_file_port, task_data['file_path'])
            if not file_sent:
                logging.error(f"Failed to send file to {peer_ip}")
                return False
            
            # Then, send the task
            task_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            task_socket.settimeout(Config.TASK_TIMEOUT)
            
            try:
                task_socket.connect((peer_ip, peer_task_port))
                
                task_data_bytes = json.dumps(task_data).encode('utf-8') + b"<END_TASK>"
                task_socket.sendall(task_data_bytes)
                
                # Receive result
                result_data = b""
                start_time = time.time()
                while time.time() - start_time < Config.TASK_TIMEOUT:
                    try:
                        chunk = task_socket.recv(4096)
                        if not chunk:
                            break
                        result_data += chunk
                        if b"<END_RESULT>" in result_data:
                            break
                    except socket.timeout:
                        continue
                
                if b"<END_RESULT>" not in result_data:
                    logging.error(f"Task timeout from {peer_ip}")
                    return False
                    
                result = json.loads(result_data.decode('utf-8').replace("<END_RESULT>", ""))
                task_socket.close()
                
                # Process result
                self._handle_task_result(task_data['task_id'], result, peer['id'])
                logging.info(f"✅ Task completed by {peer['id']}")
                return True
                
            except Exception as e:
                logging.error(f"Connection error to {peer_ip}:{peer_task_port}: {e}")
                return False
                
        except Exception as e:
            logging.error(f"Error sending task to peer {peer['id']}: {e}")
            return False

    def _send_file_to_peer(self, peer_ip, peer_file_port, file_path):
        """Send file to peer node - FIXED VERSION"""
        try:
            file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            file_socket.settimeout(Config.TASK_TIMEOUT)
            file_socket.connect((peer_ip, peer_file_port))
            
            # Send file metadata
            metadata = {
                'file_name': os.path.basename(file_path),
                'file_size': os.path.getsize(file_path)
            }
            metadata_bytes = json.dumps(metadata).encode('utf-8') + b"<END_METADATA>"
            file_socket.sendall(metadata_bytes)
            
            # Send file data
            with open(file_path, 'rb') as f:
                while True:
                    data = f.read(4096)
                    if not data:
                        break
                    file_socket.sendall(data)
            
            file_socket.sendall(b"<END_FILE>")
            
            # Wait for acknowledgment
            ack_data = b""
            while True:
                chunk = file_socket.recv(1024)
                if not chunk:
                    break
                ack_data += chunk
                if b"<END_ACK>" in ack_data:
                    break
            
            file_socket.close()
            return True
            
        except Exception as e:
            logging.error(f"Error sending file to peer {peer_ip}:{peer_file_port}: {e}")
            return False
    
    def _handle_task_result(self, task_id, result, peer_id):
        """Handle completed task result"""
        with self.lock:
            if task_id in self.pending_tasks:
                task_info = self.pending_tasks[task_id]
                
                if result['success']:
                    # Add to search index
                    file_id = str(uuid.uuid4())
                    self.search_index.add_document(
                        file_id=file_id,
                        file_name=result['metadata']['file_name'],
                        content=result['text'],
                        keywords=result['keywords'],
                        metadata=result['metadata'],
                        node_id=peer_id
                    )
                    
                    self.completed_tasks[task_id] = {
                        'task_info': task_info,
                        'result': result,
                        'completion_time': time.time()
                    }
                else:
                    self.failed_tasks[task_id] = {
                        'task_info': task_info,
                        'error': result.get('error', 'Unknown error'),
                        'failure_time': time.time()
                    }
                
                del self.pending_tasks[task_id]
                self.peer_load[peer_id] = max(0, self.peer_load[peer_id] - 1)
    
    def _task_monitor(self):
        """Monitor task timeouts"""
        while self.running:
            try:
                current_time = time.time()
                timed_out_tasks = []
                
                with self.lock:
                    for task_id, task_info in self.pending_tasks.items():
                        if current_time - task_info['start_time'] > Config.TASK_TIMEOUT:
                            timed_out_tasks.append(task_id)
                    
                    for task_id in timed_out_tasks:
                        task_info = self.pending_tasks[task_id]
                        self.failed_tasks[task_id] = {
                            'task_info': task_info,
                            'error': 'Task timeout',
                            'failure_time': current_time
                        }
                        del self.pending_tasks[task_id]
                        self.peer_load[task_info['peer_id']] = max(0, self.peer_load[task_info['peer_id']] - 1)
                
            except Exception as e:
                logging.error(f"Task monitor error: {e}")
            
            time.sleep(5)
    
    def update_peer_capabilities(self, peer_id, capabilities):
        """Update peer capabilities"""
        with self.lock:
            self.peer_capabilities[peer_id] = capabilities
    
    def remove_peer(self, peer_id):
        """Remove peer and reassign its tasks"""
        with self.lock:
            if peer_id in self.peer_capabilities:
                del self.peer_capabilities[peer_id]
            
            if peer_id in self.peer_load:
                del self.peer_load[peer_id]
            
            # Mark tasks from this peer as failed
            current_time = time.time()
            for task_id, task_info in list(self.pending_tasks.items()):
                if task_info['peer_id'] == peer_id:
                    self.failed_tasks[task_id] = {
                        'task_info': task_info,
                        'error': 'Peer disconnected',
                        'failure_time': current_time
                    }
                    del self.pending_tasks[task_id]
    
    def get_stats(self):
        """Get task manager statistics"""
        with self.lock:
            return {
                'pending_tasks': len(self.pending_tasks),
                'completed_tasks': len(self.completed_tasks),
                'failed_tasks': len(self.failed_tasks),
                'peer_load': dict(self.peer_load),
                'connected_peers': len(self.peer_capabilities)
            }