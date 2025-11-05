import threading
import time
import json
import socket
import os
import uuid
import logging
import heapq
import shutil
from collections import deque, defaultdict
from config import Config

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TaskManager:
    def __init__(self, node_id, search_index):
        self.node_id = node_id
        self.search_index = search_index
        self.pending_tasks = {}
        self.completed_tasks = {}
        self.failed_tasks = {}
        self.task_queue = deque()
        self.task_priority_queue = []  # For priority-based task processing
        self.peer_load = defaultdict(int)
        self.peer_capabilities = {}
        self.lock = threading.Lock()
        
        # Ensure upload folder exists
        os.makedirs(Config.UPLOAD_FOLDER, exist_ok=True)
        
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

        # Enhanced job statistics tracking
        self.node_job_stats = defaultdict(lambda: {
            'total_jobs': 0,
            'completed_jobs': 0,
            'failed_jobs': 0,
            'job_types': defaultdict(int),
            'total_processing_time': 0,
            'last_job_time': None
        })
        
        # Start servers
        self.running = True
        threading.Thread(target=self._task_server, daemon=True).start()
        threading.Thread(target=self._file_server, daemon=True).start()
        threading.Thread(target=self._task_monitor, daemon=True).start()
        threading.Thread(target=self._process_queued_tasks, daemon=True).start()
        
        logger.info(f" Task Manager started on ports {Config.TASK_PORT} (tasks) and {Config.FILE_PORT} (files)")

    def _task_server(self):
        """Handle incoming task requests"""
        while self.running:
            try:
                client_socket, addr = self.task_socket.accept()
                threading.Thread(target=self._handle_task_request, args=(client_socket, addr), daemon=True).start()
            except Exception as e:
                if self.running:
                    logger.error(f"Task server error: {e}")

    def _file_server(self):
        """Handle file transfers"""
        while self.running:
            try:
                client_socket, addr = self.file_socket.accept()
                threading.Thread(target=self._handle_file_transfer, args=(client_socket, addr), daemon=True).start()
            except Exception as e:
                if self.running:
                    logger.error(f"File server error: {e}")

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
            logger.info(f" Received task from {addr}: {task_data['task_id']}")
            
            # Process the task
            from document_processor import DocumentProcessor
            processor = DocumentProcessor()
            result = processor.process_task(task_data['file_path'], task_data['task_type'])
            
            # Send result back
            response = json.dumps(result).encode('utf-8') + b"<END_RESULT>"
            client_socket.sendall(response)
            logger.info(f" Sent result for task {task_data['task_id']} to {addr}")
            
        except Exception as e:
            logger.error(f"Error handling task request: {e}")
            error_response = json.dumps({'success': False, 'error': str(e)}).encode('utf-8') + b"<END_RESULT>"
            client_socket.sendall(error_response)
        finally:
            client_socket.close()

    def _handle_file_transfer(self, client_socket, addr):
        """Handle file transfers with LENGTH-PREFIXED protocol"""
        try:
            # Receive metadata length (4 bytes)
            metadata_length_bytes = client_socket.recv(4)
            if len(metadata_length_bytes) != 4:
                logger.error(f"Incomplete metadata length from {addr}")
                client_socket.sendall(b"FAILED_INCOMPLETE")
                return
                
            metadata_length = int.from_bytes(metadata_length_bytes, 'big')
            
            # Receive metadata
            metadata_data = b""
            while len(metadata_data) < metadata_length:
                chunk = client_socket.recv(metadata_length - len(metadata_data))
                if not chunk:
                    break
                metadata_data += chunk
            
            if len(metadata_data) != metadata_length:
                logger.error(f"Incomplete metadata from {addr}")
                client_socket.sendall(b"FAILED_INCOMPLETE")
                return
                
            metadata = json.loads(metadata_data.decode('utf-8'))
            
            # Receive file length (8 bytes)
            file_length_bytes = client_socket.recv(8)
            if len(file_length_bytes) != 8:
                logger.error(f"Incomplete file length from {addr}")
                client_socket.sendall(b"FAILED_INCOMPLETE")
                return
                
            file_length = int.from_bytes(file_length_bytes, 'big')
            
            # Save file
            file_path = os.path.join(Config.UPLOAD_FOLDER, metadata['file_name'])
            received_bytes = 0
            
            with open(file_path, 'wb') as f:
                while received_bytes < file_length:
                    chunk = client_socket.recv(min(4096, file_length - received_bytes))
                    if not chunk:
                        break
                    f.write(chunk)
                    received_bytes += len(chunk)
            
            # Verify file was received correctly
            actual_size = os.path.getsize(file_path)
            if actual_size == file_length:
                logger.info(f"File successfully received: {metadata['file_name']} ({actual_size} bytes)")
                client_socket.sendall(b"SUCCESS")
            else:
                logger.error(f"File size mismatch: expected {file_length}, got {actual_size}")
                client_socket.sendall(b"FAILED_SIZE_MISMATCH")
                
        except Exception as e:
            logger.error(f"File transfer error from {addr}: {e}")
            client_socket.sendall(b"FAILED_ERROR")
        finally:
            client_socket.close()

    def _send_file_to_peer(self, peer_ip, peer_file_port, file_path):
        """Send file to peer with LENGTH-PREFIXED protocol"""
        try:
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return False
            
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                logger.error(f"File is empty: {file_path}")
                return False
                
            file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            file_socket.settimeout(30.0)
            file_socket.connect((peer_ip, peer_file_port))
            
            # Send metadata with length prefix
            metadata = {
                'file_name': os.path.basename(file_path),
                'file_size': file_size,
                'timestamp': time.time()
            }
            metadata_bytes = json.dumps(metadata).encode('utf-8')
            metadata_length = len(metadata_bytes).to_bytes(4, 'big')
            
            file_socket.sendall(metadata_length)
            file_socket.sendall(metadata_bytes)
            
            # Send file with length prefix
            file_length_bytes = file_size.to_bytes(8, 'big')
            file_socket.sendall(file_length_bytes)
            
            # Send file data
            sent_bytes = 0
            with open(file_path, 'rb') as f:
                while sent_bytes < file_size:
                    chunk = f.read(4096)
                    if not chunk:
                        break
                    file_socket.sendall(chunk)
                    sent_bytes += len(chunk)
            
            # Wait for acknowledgment
            ack = file_socket.recv(1024)
            file_socket.close()
            
            if b"SUCCESS" in ack:
                logger.info(f"File successfully sent to {peer_ip}: {os.path.basename(file_path)} ({file_size} bytes)")
                return True
            else:
                logger.error(f"File transfer failed to {peer_ip}: {ack.decode('utf-8', errors='ignore')}")
                return False
                
        except Exception as e:
            logger.error(f"Error sending file to {peer_ip}:{peer_file_port}: {e}")
            return False
        
    def distribute_batch(self, file_paths):
        """Distribute a batch of files with intelligent segmentation"""
        from task_segmenter import TaskSegmenter
        segmenter = TaskSegmenter(Config)
        
        # Segment files into specialized tasks
        tasks_by_type = segmenter.analyze_document_batch(file_paths)
        
        distributed_tasks = []
        
        for task_type, tasks in tasks_by_type.items():
            for task_data in tasks:
                task_id = self._distribute_single_task(task_data)
                if task_id:
                    distributed_tasks.append({
                        'task_id': task_id,
                        'file_path': task_data['file_path'],
                        'task_type': task_type,
                        'priority': task_data['priority']
                    })
        
        logger.info(f" Distributed {len(distributed_tasks)} specialized tasks")
        return distributed_tasks

    def _distribute_single_task(self, task_data):
        """Distribute a single specialized task"""
        task_id = str(uuid.uuid4())
        
        enhanced_task_data = {
            'task_id': task_id,
            'file_path': task_data['file_path'],
            'task_type': task_data['task_type'],
            'file_name': task_data['file_name'],
            'file_size': task_data['file_size'],
            'priority': task_data['priority'],
            'estimated_time': task_data['estimated_time'],
            'requester_id': self.node_id,
            'timestamp': time.time()
        }
        
        # Add to priority queue and regular queue
        with self.lock:
            heapq.heappush(self.task_priority_queue, (-enhanced_task_data['priority'], time.time(), task_id, enhanced_task_data))
            self.task_queue.append(enhanced_task_data)
            self.pending_tasks[task_id] = {
                'task_data': enhanced_task_data,
                'start_time': time.time(),
                'status': 'queued',
                'attempts': 0
            }
    
        logger.info(f" Queued {task_data['task_type']} task for {task_data['file_name']}")
        return task_id

    def _process_queued_tasks(self):
        """Process queued tasks automatically"""
        while self.running:
            try:
                task_to_process = None
                
                # Check priority queue first
                with self.lock:
                    if self.task_priority_queue:
                        _, _, task_id, task_data = heapq.heappop(self.task_priority_queue)
                        if task_id in self.pending_tasks:
                            task_to_process = task_data
                    elif self.task_queue:
                        task_to_process = self.task_queue.popleft()
                
                if task_to_process:
                    task_id = task_to_process['task_id']
                    
                    # Try to distribute to peers first
                    distributed = self._try_distribute_to_peers(task_to_process)
                    
                    if distributed:
                        logger.info(f" Successfully distributed {task_to_process['file_name']}")
                    else:
                        # Distribution failed, process locally
                        logger.info(f" Distribution failed, processing locally: {task_to_process['file_name']}")
                        self._process_locally(task_to_process)
                
                time.sleep(1)  # Small delay between processing attempts
                
            except Exception as e:
                logger.error(f"Error in task processor: {e}")
                time.sleep(5)

    def _try_distribute_to_peers(self, task_data):
        """Try to distribute task to suitable peers"""
        task_id = task_data['task_id']
        task_type = task_data['task_type']
        file_path = task_data['file_path']
        
        # Find suitable peers
        suitable_peers = self._find_suitable_peers(task_type)
        
        if not suitable_peers:
            logger.info(f" No suitable peers found for {task_data['file_name']}, will process locally")
            return False
        
        logger.info(f" Found {len(suitable_peers)} suitable peers for {task_data['file_name']}")
        
        # Try each suitable peer in order (least loaded first)
        for peer in suitable_peers:
            logger.info(f" Attempting distribution to {peer['id']} (IP: {peer['ip']})")
            
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
                logger.error(f" Failed to send file to {peer['id']}")
                with self.lock:
                    self.peer_load[peer['id']] -= 1
                continue
            
            # Send task to peer
            task_sent = self._send_task_to_peer(peer, task_data)
            
            if task_sent:
                logger.info(f" Successfully distributed {task_data['file_name']} to {peer['id']}")
                with self.lock:
                    if task_id in self.pending_tasks:
                        self.pending_tasks[task_id]['status'] = 'distributed'
                return True
            else:
                logger.error(f" Failed to send task to {peer['id']}")
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
            
            logger.info(f" Connecting to {peer_ip}:{peer_task_port} for task distribution")
            
            # Create socket with timeout
            task_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            task_socket.settimeout(10.0)
            
            try:
                # Connect to peer
                task_socket.connect((peer_ip, peer_task_port))
                logger.info(f" Connected to {peer_ip}:{peer_task_port}")
                
                # Send task data
                task_data_bytes = json.dumps(task_data).encode('utf-8') + b"<END_TASK>"
                task_socket.sendall(task_data_bytes)
                logger.info(f" Sent task data to {peer_ip}")
                
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
                    logger.error(f" Task timeout from {peer_ip}")
                    return False
                
                # Process result
                result_str = result_data.decode('utf-8').replace("<END_RESULT>", "")
                result = json.loads(result_str)
                
                if result.get('success', False):
                    logger.info(f" Task completed successfully by {peer['id']}")
                    self._handle_task_result(task_data['task_id'], result, peer['id'])
                    return True
                else:
                    logger.error(f" Task failed on peer {peer['id']}: {result.get('error', 'Unknown error')}")
                    return False
                
            except socket.timeout:
                logger.error(f" Connection timeout to {peer_ip}:{peer_task_port}")
                return False
            except ConnectionRefusedError:
                logger.error(f" Connection refused by {peer_ip}:{peer_task_port}")
                return False
            except Exception as e:
                logger.error(f" Connection error to {peer_ip}:{peer_task_port}: {e}")
                return False
            finally:
                task_socket.close()
                
        except Exception as e:
            logger.error(f" Error sending task to peer {peer['id']}: {e}")
            return False


    def _process_locally(self, task_data):
        """Process file locally as fallback"""
        try:
            task_id = task_data['task_id']
            file_path = task_data['file_path']
            task_type = task_data['task_type']
            
            logger.info(f" Processing locally: {task_data['file_name']}")
            
            from document_processor import DocumentProcessor
            processor = DocumentProcessor()
            result = processor.process_task(file_path, task_type)
            
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
                    
                    logger.info(f" Local processing completed for {task_data['file_name']}")
                    logger.info(f" Extracted {len(result['text'])} characters, {len(result['keywords'])} keywords")
                    return True
                else:
                    logger.error(f" Failed to add document to search index: {task_data['file_name']}")
            else:
                logger.error(f" Document processing failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            logger.error(f" Local processing failed for {task_data['file_name']}: {e}")
        
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
    def distribute_task(self, file_path, task_type='auto'):
        """Distribute task with DETAILED LOGGING"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f" TASK DISTRIBUTION REQUEST")
            logger.info(f"{'='*60}")
            logger.info(f"File: {os.path.basename(file_path)}")
            logger.info(f"Task Type: {task_type}")
            logger.info(f"File Size: {os.path.getsize(file_path)} bytes")
            
            # Create task data
            task_data = {
                'file_path': file_path,
                'task_type': task_type,
                'file_name': os.path.basename(file_path),
                'file_size': os.path.getsize(file_path),
                'priority': 1,
                'estimated_time': 30
            }
            
            # Check available peers
            with self.lock:
                logger.info(f"\n NETWORK STATUS:")
                logger.info(f"Total peers: {len(self.peer_capabilities)}")
                
                if not self.peer_capabilities:
                    logger.warning("  NO PEERS AVAILABLE - Will process locally")
                    self._process_locally_immediate(task_data)
                    return None
                
                # Find suitable peers
                suitable_peers = self._find_suitable_peers(task_type)
                logger.info(f"\n PEER SELECTION:")
                logger.info(f"Suitable peers found: {len(suitable_peers)}")
                
                for peer in suitable_peers:
                    logger.info(f"    {peer['id'][:20]}... (IP: {peer['ip']}, Load: {peer['load']}, Score: {peer['score']})")
                
                if not suitable_peers:
                    logger.warning(f"  No peers with {task_type} capability - Processing locally")
                    self._process_locally_immediate(task_data)
                    return None
            
            # Queue the task
            task_id = self._distribute_single_task(task_data)
            logger.info(f"\n Task queued: {task_id}")
            logger.info(f"{'='*60}\n")
            return task_id
                
        except Exception as e:
            logger.error(f" Distribution error: {e}")
            return None

    def _process_locally_immediate(self, task_data):
        """Immediate local processing for when no peers are available"""
        logger.info(" Starting immediate local processing...")
        threading.Thread(target=self._process_locally, args=(task_data,), daemon=True).start()

    def _handle_task_result(self, task_id, result, peer_id):
        """Handle completed task result - FIXED VERSION"""
        with self.lock:
            if task_id in self.pending_tasks:
                task_info = self.pending_tasks[task_id]
                
                # CRITICAL FIX: Check if processing actually succeeded
                if result.get('success', False) and result.get('text', '').strip():
                    # Valid result - process normally
                    self.node_job_stats[peer_id]['total_jobs'] += 1
                    self.node_job_stats[peer_id]['completed_jobs'] += 1
                    self.node_job_stats[peer_id]['job_types'][task_info['task_data']['task_type']] += 1
                    self.node_job_stats[peer_id]['last_job_time'] = time.time()
                    
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
                        logger.info(f"Successfully processed and indexed document from peer {peer_id}")
                    else:
                        logger.error(f"Failed to index document from peer {peer_id}")
                        self.failed_tasks[task_id] = {
                            'task_info': task_info,
                            'error': 'Indexing failed',
                            'failure_time': time.time()
                        }
                else:
                    # Task failed - mark appropriately
                    error_msg = result.get('error', 'No content extracted')
                    logger.error(f"Task failed on peer {peer_id}: {error_msg}")
                    self.node_job_stats[peer_id]['total_jobs'] += 1
                    self.node_job_stats[peer_id]['failed_jobs'] += 1
                    self.failed_tasks[task_id] = {
                        'task_info': task_info,
                        'error': error_msg,
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
                logger.error(f"Task monitor error: {e}")
            
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
        """Get task manager statistics with job distribution"""
        with self.lock:
            return {
                'pending_tasks': len(self.pending_tasks),
                'completed_tasks': len(self.completed_tasks),
                'failed_tasks': len(self.failed_tasks),
                'queued_tasks': len(self.task_queue),
                'peer_load': dict(self.peer_load),
                'connected_peers': len(self.peer_capabilities),
                'total_tasks_processed': len(self.completed_tasks) + len(self.failed_tasks),
                'node_job_stats': dict(self.node_job_stats),  # Add job distribution stats
                'job_distribution': self._get_job_distribution_summary()
            }

    def _get_job_distribution_summary(self):
        """Get summary of job distribution across nodes"""
        summary = {}
        for node_id, stats in self.node_job_stats.items():
            total_jobs = stats['total_jobs']
            completed_jobs = stats['completed_jobs']
            summary[node_id] = {
                'total_jobs': total_jobs,
                'completion_rate': completed_jobs / max(1, total_jobs),
                'job_types': dict(stats['job_types']),
                'last_active': stats['last_job_time']
            }
        return summary

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
        try:
            self.task_socket.close()
        except:
            pass
        try:
            self.file_socket.close()
        except:
            pass
        logger.info(" Task Manager stopped")


