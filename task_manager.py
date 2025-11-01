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
    
    def distribute_task(self, file_path, task_type='full', target_peer=None):
        """Distribute task to peer"""
        task_id = str(uuid.uuid4())
        
        if target_peer:
            # Send to specific peer
            peers = [target_peer]
        else:
            # Find suitable peer
            peers = self._find_suitable_peers(task_type)
        
        if not peers:
            return None  # No suitable peers available
        
        peer = peers[0]  # Select first suitable peer
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
            return task_id
        else:
            del self.pending_tasks[task_id]
            self.peer_load[peer['id']] -= 1
            return None
    
    def _find_suitable_peers(self, task_type):
        """Find peers suitable for the given task type"""
        suitable_peers = []
        
        with self.lock:
            for peer_id, capabilities in self.peer_capabilities.items():
                if task_type == 'ocr' and capabilities.get('ocr', False):
                    suitable_peers.append({'id': peer_id, 'capabilities': capabilities})
                elif task_type == 'nlp' and capabilities.get('nlp', False):
                    suitable_peers.append({'id': peer_id, 'capabilities': capabilities})
                elif capabilities.get('text_extraction', False):
                    suitable_peers.append({'id': peer_id, 'capabilities': capabilities})
        
        # Sort by load (least loaded first)
        suitable_peers.sort(key=lambda x: self.peer_load.get(x['id'], 0))
        return suitable_peers
    
    def _send_task_to_peer(self, peer, task_data):
        """Send task to peer node"""
        try:
            # First, send the file
            file_sent = self._send_file_to_peer(peer, task_data['file_path'])
            if not file_sent:
                return False
            
            # Then, send the task
            task_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            task_socket.settimeout(Config.TASK_TIMEOUT)
            task_socket.connect((peer['id'], Config.TASK_PORT))
            
            task_data_bytes = json.dumps(task_data).encode('utf-8') + b"<END_TASK>"
            task_socket.sendall(task_data_bytes)
            
            # Receive result
            result_data = b""
            while True:
                chunk = task_socket.recv(4096)
                if not chunk:
                    break
                result_data += chunk
                if b"<END_RESULT>" in result_data:
                    break
            
            result = json.loads(result_data.decode('utf-8').replace("<END_RESULT>", ""))
            task_socket.close()
            
            # Process result
            self._handle_task_result(task_data['task_id'], result, peer['id'])
            return True
            
        except Exception as e:
            logging.error(f"Error sending task to peer {peer['id']}: {e}")
            return False
    
    def _send_file_to_peer(self, peer, file_path):
        """Send file to peer node"""
        try:
            file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            file_socket.settimeout(Config.TASK_TIMEOUT)
            file_socket.connect((peer['id'], Config.FILE_PORT))
            
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
            logging.error(f"Error sending file to peer {peer['id']}: {e}")
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