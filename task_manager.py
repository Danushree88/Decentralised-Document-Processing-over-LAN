import threading
import time
import json
import socket
import os
import uuid
import logging
import shutil
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
        threading.Thread(target=self._process_queued_tasks, daemon=True).start()
        
        logging.info(f"✅ Task Manager started on ports {Config.TASK_PORT} (tasks) and {Config.FILE_PORT} (files)")

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
            logging.info(f"📥 Received task from {addr}: {task_data['task_id']}")
            
            # Process the task
            from document_processor import DocumentProcessor
            processor = DocumentProcessor()
            result = processor.process_document(task_data['file_path'], task_data['task_type'])
            
            # Send result back
            response = json.dumps(result).encode('utf-8') + b"<END_RESULT>"
            client_socket.sendall(response)
            logging.info(f"📤 Sent result for task {task_data['task_id']} to {addr}")
            
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
            
            logging.info(f"📥 Receiving file: {metadata['file_name']} from {addr}")
            
            # Receive file data
            with open(file_path, 'wb') as f:
                while True:
                    data = client_socket.recv(4096)
                    if not data:
                        break
                    if data.endswith(b"<END_FILE>"):
                        f.write(data[:-10])  # Remove end marker
                        break
                    f.write(data)
            
            # Verify file was received
            if os.path.exists(file_path):
                file_size = os.path.getsize(file_path)
                logging.info(f"✅ File received: {metadata['file_name']} ({file_size} bytes)")
                client_socket.sendall(b"FILE_RECEIVED<END_ACK>")
            else:
                logging.error(f"❌ File failed to save: {file_path}")
                client_socket.sendall(b"FILE_ERROR<END_ACK>")
            
        except Exception as e:
            logging.error(f"Error handling file transfer: {e}")
            client_socket.sendall(b"FILE_ERROR<END_ACK>")
        finally:
            client_socket.close()

    def distribute_task(self, file_path, task_type='full'):
        """Distribute task automatically - ENHANCED VERSION"""
        task_id = str(uuid.uuid4())
        
        logging.info(f"🎯 Starting AUTOMATIC task distribution for: {os.path.basename(file_path)}")
        
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
        
        # Create task data
        task_data = {
            'task_id': task_id,
            'file_path': file_path,
            'task_type': task_type,
            'requester_id': self.node_id,
            'file_name': os.path.basename(file_path),
            'timestamp': time.time()
        }
        
        # Add to queue for processing
        with self.lock:
            self.task_queue.append(task_data)
            self.pending_tasks[task_id] = {
                'task_data': task_data,
                'start_time': time.time(),
                'status': 'queued',
                'attempts': 0
            }
        
        logging.info(f"✅ Task {task_id} queued for distribution")
        return task_id

    def _process_queued_tasks(self):
        """Process queued tasks automatically"""
        while self.running:
            try:
                if self.task_queue:
                    task_data = self.task_queue[0]  # Peek at first task
                    task_id = task_data['task_id']
                    
                    with self.lock:
                        task_info = self.pending_tasks.get(task_id)
                        if not task_info:
                            self.task_queue.popleft()
                            continue
                    
                    # Try to distribute to peers first
                    distributed = self._try_distribute_to_peers(task_data)
                    
                    if distributed:
                        # Successfully distributed, remove from queue
                        with self.lock:
                            if self.task_queue and self.task_queue[0]['task_id'] == task_id:
                                self.task_queue.popleft()
                    else:
                        # Distribution failed, process locally
                        logging.info(f"🔄 Distribution failed, processing locally: {task_data['file_name']}")
                        self._process_locally(task_data)
                        with self.lock:
                            if self.task_queue and self.task_queue[0]['task_id'] == task_id:
                                self.task_queue.popleft()
                
                time.sleep(1)  # Small delay between processing attempts
                
            except Exception as e:
                logging.error(f"Error in task processor: {e}")
                time.sleep(5)

    def _try_distribute_to_peers(self, task_data):
        """Try to distribute task to suitable peers"""
        task_id = task_data['task_id']
        task_type = task_data['task_type']
        file_path = task_data['file_path']
        
        # Find suitable peers
        suitable_peers = self._find_suitable_peers(task_type)
        
        if not suitable_peers:
            logging.info(f"ℹ️ No suitable peers found for {task_data['file_name']}, will process locally")
            return False
        
        logging.info(f"🔍 Found {len(suitable_peers)} suitable peers for {task_data['file_name']}")
        
        # Try each suitable peer in order (least loaded first)
        for peer in suitable_peers:
            logging.info(f"📤 Attempting distribution to {peer['id']} (IP: {peer['ip']})")
            
            # Update task status
            with self.lock:
                if task_id in self.pending_tasks:
                    self.pending_tasks[task_id].update({
                        'status': 'distributing',
                        'peer_id': peer['id'],
                        'attempts': self.pending_tasks[task_id].get('attempts', 0) + 1
                    })
                    self.peer_load[peer['id']] += 1
            
            # Send file to peer
            file_sent = self._send_file_to_peer(peer['ip'], Config.FILE_PORT, file_path)
            if not file_sent:
                logging.error(f"❌ Failed to send file to {peer['id']}")
                with self.lock:
                    self.peer_load[peer['id']] -= 1
                continue
            
            # Send task to peer
            task_sent = self._send_task_to_peer(peer, task_data)
            
            if task_sent:
                logging.info(f"✅ Successfully distributed {task_data['file_name']} to {peer['id']}")
                with self.lock:
                    if task_id in self.pending_tasks:
                        self.pending_tasks[task_id]['status'] = 'distributed'
                return True
            else:
                logging.error(f"❌ Failed to send task to {peer['id']}")
                with self.lock:
                    self.peer_load[peer['id']] -= 1
                    if task_id in self.pending_tasks:
                        self.pending_tasks[task_id]['status'] = 'distribution_failed'
        
        return False

    def _send_task_to_peer(self, peer, task_data):
        """Send task to peer node - SIMPLIFIED & ROBUST"""
        try:
            peer_ip = peer['ip']
            peer_task_port = Config.TASK_PORT
            
            logging.info(f"🔗 Connecting to {peer_ip}:{peer_task_port} for task distribution")
            
            # Create socket with timeout
            task_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            task_socket.settimeout(10.0)
            
            try:
                # Connect to peer
                task_socket.connect((peer_ip, peer_task_port))
                logging.info(f"✅ Connected to {peer_ip}:{peer_task_port}")
                
                # Send task data
                task_data_bytes = json.dumps(task_data).encode('utf-8') + b"<END_TASK>"
                task_socket.sendall(task_data_bytes)
                logging.info(f"📤 Sent task data to {peer_ip}")
                
                # Receive result with timeout
                result_data = b""
                start_time = time.time()
                while time.time() - start_time < 30:  # 30 second timeout
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
                    logging.error(f"❌ Task timeout from {peer_ip}")
                    return False
                
                # Process result
                result_str = result_data.decode('utf-8').replace("<END_RESULT>", "")
                result = json.loads(result_str)
                
                if result.get('success', False):
                    logging.info(f"✅ Task completed successfully by {peer['id']}")
                    self._handle_task_result(task_data['task_id'], result, peer['id'])
                    return True
                else:
                    logging.error(f"❌ Task failed on peer {peer['id']}: {result.get('error', 'Unknown error')}")
                    return False
                
            except socket.timeout:
                logging.error(f"❌ Connection timeout to {peer_ip}:{peer_task_port}")
                return False
            except ConnectionRefusedError:
                logging.error(f"❌ Connection refused by {peer_ip}:{peer_task_port}")
                return False
            except Exception as e:
                logging.error(f"❌ Connection error to {peer_ip}:{peer_task_port}: {e}")
                return False
            finally:
                task_socket.close()
                
        except Exception as e:
            logging.error(f"❌ Error sending task to peer {peer['id']}: {e}")
            return False

    def _send_file_to_peer(self, peer_ip, peer_file_port, file_path):
        """Send file to peer node - ROBUST VERSION"""
        try:
            file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            file_socket.settimeout(10.0)
            file_socket.connect((peer_ip, peer_file_port))
            
            # Send file metadata
            metadata = {
                'file_name': os.path.basename(file_path),
                'file_size': os.path.getsize(file_path),
                'timestamp': time.time()
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
            
            if b"FILE_RECEIVED" in ack_data:
                logging.info(f"✅ File successfully sent to {peer_ip}")
                return True
            else:
                logging.error(f"❌ File transfer failed to {peer_ip}")
                return False
            
        except Exception as e:
            logging.error(f"❌ Error sending file to {peer_ip}:{peer_file_port}: {e}")
            return False

    def _process_locally(self, task_data):
        """Process file locally as fallback"""
        try:
            task_id = task_data['task_id']
            file_path = task_data['file_path']
            task_type = task_data['task_type']
            
            logging.info(f"🔄 Processing locally: {task_data['file_name']}")
            
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
                    with self.lock:
                        self.completed_tasks[task_id] = {
                            'result': result,
                            'completion_time': time.time(),
                            'processed_by': 'local'
                        }
                        if task_id in self.pending_tasks:
                            del self.pending_tasks[task_id]
                    
                    logging.info(f"✅ Local processing completed for {task_data['file_name']}")
                    logging.info(f"📊 Extracted {len(result['text'])} characters, {len(result['keywords'])} keywords")
                    return True
                else:
                    logging.error(f"❌ Failed to add document to search index: {task_data['file_name']}")
            else:
                logging.error(f"❌ Document processing failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logging.error(f"❌ Local processing failed for {task_data['file_name']}: {e}")
        
        # Mark as failed
        with self.lock:
            self.failed_tasks[task_id] = {
                'task_data': task_data,
                'error': 'Local processing failed',
                'failure_time': time.time()
            }
            if task_id in self.pending_tasks:
                del self.pending_tasks[task_id]
        
        return False

    def _find_suitable_peers(self, task_type):
        """Find peers suitable for the given task type - ENHANCED"""
        suitable_peers = []
        
        with self.lock:
            if not self.peer_capabilities:
                return []
                
            for peer_id, capabilities in self.peer_capabilities.items():
                peer_ip = capabilities.get('ip', 'unknown')
                if peer_ip == 'unknown':
                    continue
                    
                # Enhanced capability matching
                capability_score = 0
                if task_type == 'ocr' and capabilities.get('ocr', False):
                    capability_score = 3
                elif task_type == 'nlp' and capabilities.get('nlp', False):
                    capability_score = 3
                elif task_type == 'text_extraction' and capabilities.get('text_extraction', False):
                    capability_score = 2
                elif capabilities.get('file_processing', False):
                    capability_score = 1
                
                if capability_score > 0:
                    suitable_peers.append({
                        'id': peer_id,
                        'ip': peer_ip,
                        'capabilities': capabilities,
                        'load': self.peer_load.get(peer_id, 0),
                        'score': capability_score
                    })
        
        # Sort by capability score (highest first), then by load (lowest first)
        suitable_peers.sort(key=lambda x: (-x['score'], x['load']))
        
        return suitable_peers

    def _handle_task_result(self, task_id, result, peer_id):
        """Handle completed task result from peer"""
        with self.lock:
            if task_id in self.pending_tasks:
                task_info = self.pending_tasks[task_id]
                
                if result['success']:
                    # Add to search index
                    file_id = str(uuid.uuid4())
                    index_success = self.search_index.add_document(
                        file_id=file_id,
                        file_name=result['metadata']['file_name'],
                        content=result['text'],
                        keywords=result['keywords'],
                        metadata=result['metadata'],
                        node_id=peer_id
                    )
                    
                    if index_success:
                        self.completed_tasks[task_id] = {
                            'task_info': task_info,
                            'result': result,
                            'completion_time': time.time(),
                            'processed_by': peer_id
                        }
                        logging.info(f"📝 Successfully indexed document from peer {peer_id}")
                    else:
                        logging.error(f"❌ Failed to index document from peer {peer_id}")
                        self.failed_tasks[task_id] = {
                            'task_info': task_info,
                            'error': 'Indexing failed',
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
                        if 'peer_id' in task_info:
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
            
            # Mark tasks from this peer as failed and requeue them
            current_time = time.time()
            for task_id, task_info in list(self.pending_tasks.items()):
                if task_info.get('peer_id') == peer_id:
                    # Requeue the task for redistribution
                    if task_info['status'] == 'distributed':
                        task_info.update({
                            'status': 'queued',
                            'peer_id': None,
                            'attempts': task_info.get('attempts', 0) + 1
                        })
                        # Move back to front of queue for quick retry
                        self.task_queue.appendleft(task_info['task_data'])
                    else:
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
                'queued_tasks': len(self.task_queue),
                'peer_load': dict(self.peer_load),
                'connected_peers': len(self.peer_capabilities),
                'total_tasks_processed': len(self.completed_tasks) + len(self.failed_tasks)
            }

    def get_task_status(self, task_id):
        """Get status of specific task"""
        with self.lock:
            if task_id in self.pending_tasks:
                return self.pending_tasks[task_id]
            elif task_id in self.completed_tasks:
                return {'status': 'completed', **self.completed_tasks[task_id]}
            elif task_id in self.failed_tasks:
                return {'status': 'failed', **self.failed_tasks[task_id]}
            else:
                return {'status': 'unknown'}

    def stop(self):
        """Stop the task manager"""
        self.running = False
        self.task_socket.close()
        self.file_socket.close()