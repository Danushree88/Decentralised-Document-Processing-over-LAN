import threading
import time
import json
import socket
import os
import uuid
import logging
import heapq
from collections import deque, defaultdict
from config import Config
from datetime import datetime

logger = logging.getLogger(__name__)

class TaskManager:
    def __init__(self, node_id, search_index):
        self.node_id = node_id
        self.search_index = search_index
        self.pending_tasks = {}
        self.completed_tasks = {}
        self.failed_tasks = {}
        self.task_queue = deque()
        self.task_priority_queue = []
        self.peer_load = defaultdict(int)
        self.peer_capabilities = {}
        self.lock = threading.Lock()
        self.file_locks = {}  
        self.files_in_progress = set() 
        
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

        # Job statistics
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
        
        logger.info(f"✅ Task Manager started on ports {Config.TASK_PORT} and {Config.FILE_PORT}")

    def _get_file_lock(self, file_path):
        """Get or create a lock for a specific file"""
        if file_path not in self.file_locks:
            self.file_locks[file_path] = threading.Lock()
        return self.file_locks[file_path]

    def _wait_for_file_ready(self, file_path, max_wait=5.0):
        """Wait for file to be ready for reading - CRITICAL FIX"""
        start_time = time.time()
        
        while time.time() - start_time < max_wait:
            try:
                # Check if file exists
                if not os.path.exists(file_path):
                    time.sleep(0.1)
                    continue
                
                # Check if file has non-zero size
                file_size = os.path.getsize(file_path)
                if file_size == 0:
                    time.sleep(0.1)
                    continue
                
                # Try to open file exclusively
                try:
                    with open(file_path, 'rb') as f:
                        # Try to read the entire file to verify it's complete
                        f.seek(0, 2)  # Seek to end
                        actual_size = f.tell()
                        if actual_size != file_size:
                            time.sleep(0.1)
                            continue
                        
                        # File is complete and readable
                        return True
                except (IOError, OSError) as e:
                    # File is still being written
                    time.sleep(0.1)
                    continue
                    
            except Exception as e:
                logger.error(f"Error checking file readiness: {e}")
                time.sleep(0.1)
        
        logger.error(f"❌ Timeout waiting for file to be ready: {file_path}")
        return False

    def _send_file_to_peer(self, peer_ip, peer_file_port, file_path):
        """COMPLETELY FIXED file transfer with proper locking"""
        file_lock = self._get_file_lock(file_path)
        
        with file_lock:  # CRITICAL: Lock for entire operation
            try:
                # Wait for file to be completely written
                if not self._wait_for_file_ready(file_path, max_wait=10.0):
                    logger.error(f"File not ready: {file_path}")
                    return False
                
                # Validate file
                if not os.path.exists(file_path):
                    logger.error(f"File not found: {file_path}")
                    return False
                
                # Get STABLE file size (wait for writes to complete)
                stable_size = None
                for attempt in range(5):
                    current_size = os.path.getsize(file_path)
                    time.sleep(0.1)
                    next_size = os.path.getsize(file_path)
                    if current_size == next_size and current_size > 0:
                        stable_size = current_size
                        break
                
                if not stable_size or stable_size == 0:
                    logger.error(f"File size unstable or zero: {file_path}")
                    return False
                
                logger.info(f"Sending file: {os.path.basename(file_path)} ({stable_size} bytes) to {peer_ip}")
                
                # Connect to peer
                file_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                file_socket.settimeout(30.0)
                file_socket.connect((peer_ip, peer_file_port))
                
                # Send metadata with stable size
                metadata = {
                    'file_name': os.path.basename(file_path),
                    'file_size': stable_size,
                    'timestamp': time.time()
                }
                metadata_bytes = json.dumps(metadata).encode('utf-8')
                metadata_length = len(metadata_bytes).to_bytes(4, 'big')
                
                file_socket.sendall(metadata_length)
                file_socket.sendall(metadata_bytes)
                
                # Send file size
                file_socket.sendall(stable_size.to_bytes(8, 'big'))
                
                # Send file data - READ ENTIRE FILE FIRST
                with open(file_path, 'rb') as f:
                    file_data = f.read()  # Read ALL at once
                
                if len(file_data) != stable_size:
                    logger.error(f"File size mismatch: expected {stable_size}, read {len(file_data)}")
                    file_socket.close()
                    return False
                
                # Send in chunks
                sent_bytes = 0
                chunk_size = 8192
                
                while sent_bytes < len(file_data):
                    chunk = file_data[sent_bytes:sent_bytes + chunk_size]
                    file_socket.sendall(chunk)
                    sent_bytes += len(chunk)
                
                if sent_bytes != stable_size:
                    logger.error(f"Incomplete send: {sent_bytes}/{stable_size}")
                    file_socket.close()
                    return False
                
                logger.info(f"File transfer complete: {sent_bytes} bytes")
                
                # Wait for acknowledgment
                ack_data = file_socket.recv(1024)
                file_socket.close()
                
                if b"SUCCESS" in ack_data:
                    logger.info(f"File verified by peer")
                    return True
                else:
                    logger.error(f"Peer verification failed")
                    return False
                    
            except Exception as e:
                logger.error(f"File transfer error: {e}")
                import traceback
                logger.error(traceback.format_exc())
                return False
        
    # [Rest of your existing methods remain the same - just include the fixes above]
    # I'll include the critical _process_queued_tasks fix:
    
    def _process_queued_tasks(self):
        """Process queued tasks with PROPER file-level locking"""
        while self.running:
            try:
                task_to_process = None
                
                with self.lock:
                    # Clean up completed tasks
                    self._clean_completed_tasks_from_queues()
                    
                    # Try to find a task whose file is NOT currently being processed
                    if self.task_priority_queue:
                        available_tasks = []
                        
                        for item in self.task_priority_queue:
                            task_id = item[2]
                            task_data = item[3]
                            file_path = task_data['file_path']
                            
                            # Skip if this file is already being processed
                            if file_path not in self.files_in_progress and task_id in self.pending_tasks:
                                available_tasks.append(item)
                        
                        if available_tasks:
                            # Get highest priority available task
                            available_tasks.sort(key=lambda x: x[0])  # Sort by priority
                            _, _, task_id, task_data = available_tasks[0]
                            file_path = task_data['file_path']
                            
                            # Mark file as in-progress
                            self.files_in_progress.add(file_path)
                            task_to_process = task_data
                            
                            # Remove from queue
                            self.task_priority_queue = [item for item in self.task_priority_queue if item[2] != task_id]
                            heapq.heapify(self.task_priority_queue)
                    
                    elif self.task_queue:
                        # Try regular queue
                        for _ in range(len(self.task_queue)):
                            task_data = self.task_queue.popleft()
                            task_id = task_data['task_id']
                            file_path = task_data['file_path']
                            
                            if task_id in self.pending_tasks and file_path not in self.files_in_progress:
                                self.files_in_progress.add(file_path)
                                task_to_process = task_data
                                break
                            else:
                                # Put back at end if file is busy
                                if task_id in self.pending_tasks:
                                    self.task_queue.append(task_data)
                
                if task_to_process:
                    task_id = task_to_process['task_id']
                    file_path = task_to_process['file_path']
                    
                    try:
                        # Double-check task is still pending
                        with self.lock:
                            if task_id not in self.pending_tasks:
                                self.files_in_progress.discard(file_path)
                                continue
                        
                        # Try to distribute
                        distributed = self._try_distribute_to_peers(task_to_process)
                        
                        if not distributed:
                            logger.info(f"🔄 Distribution failed, processing locally: {task_to_process['file_name']}")
                            self._process_locally(task_to_process)
                        
                    finally:
                        # CRITICAL: Always release the file after processing
                        with self.lock:
                            self.files_in_progress.discard(file_path)
                            logger.info(f"🔓 Released file lock: {os.path.basename(file_path)}")
                
                time.sleep(1)  # Small delay
                
            except Exception as e:
                logger.error(f"❌ Error in task processor: {e}")
                import traceback
                logger.error(traceback.format_exc())
                time.sleep(5)


    def _clean_completed_tasks_from_queues(self):
        """Remove completed tasks"""
        new_priority_queue = []
        for item in self.task_priority_queue:
            if item[2] in self.pending_tasks:
                new_priority_queue.append(item)
        self.task_priority_queue = new_priority_queue
        heapq.heapify(self.task_priority_queue)
        
        self.task_queue = deque([task for task in self.task_queue if task['task_id'] in self.pending_tasks])

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
        """Handle file transfers with ROBUST length-prefixed protocol"""
        try:
            client_socket.settimeout(120.0)
            
            # Receive metadata length (4 bytes)
            metadata_length_bytes = client_socket.recv(4)
            if len(metadata_length_bytes) != 4:
                logger.error(f"Incomplete metadata length from {addr}")
                client_socket.sendall(b"FAILED_INCOMPLETE_METADATA_LENGTH")
                return
                
            metadata_length = int.from_bytes(metadata_length_bytes, 'big')
            logger.info(f"Receiving metadata: {metadata_length} bytes from {addr}")
            
            # Receive metadata
            metadata_data = b""
            while len(metadata_data) < metadata_length:
                chunk = client_socket.recv(metadata_length - len(metadata_data))
                if not chunk:
                    break
                metadata_data += chunk
            
            if len(metadata_data) != metadata_length:
                logger.error(f"Incomplete metadata from {addr}: {len(metadata_data)}/{metadata_length}")
                client_socket.sendall(b"FAILED_INCOMPLETE_METADATA")
                return
                
            metadata = json.loads(metadata_data.decode('utf-8'))
            logger.info(f"Metadata received: {metadata['file_name']} ({metadata['file_size']} bytes)")
            
            # Receive file length (8 bytes)
            file_length_bytes = client_socket.recv(8)
            if len(file_length_bytes) != 8:
                logger.error(f"Incomplete file length from {addr}")
                client_socket.sendall(b"FAILED_INCOMPLETE_FILE_LENGTH")
                return
                
            expected_file_size = int.from_bytes(file_length_bytes, 'big')
            
            # Save file
            file_path = os.path.join(Config.UPLOAD_FOLDER, metadata['file_name'])
            received_bytes = 0
            
            with open(file_path, 'wb') as f:
                while received_bytes < expected_file_size:
                    chunk = client_socket.recv(min(8192, expected_file_size - received_bytes))
                    if not chunk:
                        break
                    f.write(chunk)
                    received_bytes += len(chunk)
                    
                    # Log progress for large files
                    if expected_file_size > 1024 * 1024 and received_bytes % (1024 * 1024) == 0:
                        progress = (received_bytes / expected_file_size) * 100
                        logger.info(f"Receiving progress: {progress:.1f}% ({received_bytes}/{expected_file_size} bytes)")
            
            # Verify file was received correctly
            actual_size = os.path.getsize(file_path)
            if actual_size == expected_file_size:
                logger.info(f"✅ File successfully received: {metadata['file_name']} ({actual_size} bytes)")
                client_socket.sendall(b"SUCCESS")
            else:
                logger.error(f"❌ File size mismatch: expected {expected_file_size}, got {actual_size}")
                client_socket.sendall(b"FAILED_SIZE_MISMATCH")
                
        except Exception as e:
            logger.error(f"❌ File transfer error from {addr}: {e}")
            client_socket.sendall(b"FAILED_ERROR")
        finally:
            client_socket.close()


        
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

    def _try_distribute_to_peers(self, task_data):
        """Try to distribute task to suitable peers - FIXED VERSION"""
        task_id = task_data['task_id']
        task_type = task_data['task_type']
        file_path = task_data['file_path']
        
        # Check if task was already completed
        with self.lock:
            if task_id in self.completed_tasks or task_id in self.failed_tasks:
                logger.info(f" Task {task_id} already completed/failed, skipping distribution")
                return True  # Return True to prevent reprocessing
        
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
                if task_id not in self.pending_tasks:
                    logger.info(f" Task {task_id} no longer pending, skipping")
                    return True
                    
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
                    if task_id in self.pending_tasks:
                        self.pending_tasks[task_id]['status'] = 'distribution_failed'
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


    def _is_file_already_indexed(self, file_path):
        """Check if file is already indexed - PREVENTS DUPLICATES"""
        filename = os.path.basename(file_path)
        
        try:
            all_docs = self.search_index.get_all_documents()
            for doc in all_docs:
                if doc.get('file_name') == filename:
                    logger.info(f"File already indexed: {filename}")
                    return True
            return False
        except Exception as e:
            logger.error(f"Error checking duplicates: {e}")
            return False

    def _process_locally(self, task_data):
        """Process file locally - FIXED with duplicate check"""
        try:
            task_id = task_data['task_id']
            file_path = task_data['file_path']
            task_type = task_data['task_type']
            
            logger.info(f"Processing locally: {task_data['file_name']} (task: {task_type})")
            
            # CHECK FOR DUPLICATES FIRST
            if self._is_file_already_indexed(file_path):
                with self.lock:
                    self.completed_tasks[task_id] = {
                        'result': {'success': True, 'text': 'Already indexed'},
                        'completion_time': time.time(),
                        'processed_by': 'duplicate_skip'
                    }
                    if task_id in self.pending_tasks:
                        del self.pending_tasks[task_id]
                logger.info(f"Skipped duplicate: {task_data['file_name']}")
                return True
            
            # Verify file exists and is stable
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                return False
            
            # Wait for stable file size
            stable_size = None
            for _ in range(10):
                size1 = os.path.getsize(file_path)
                time.sleep(0.2)
                size2 = os.path.getsize(file_path) if os.path.exists(file_path) else 0
                if size1 == size2 and size1 > 0:
                    stable_size = size1
                    break
            
            if not stable_size or stable_size == 0:
                logger.error(f"File is empty or unstable: {file_path}")
                return False
            
            logger.info(f"File verified: {stable_size} bytes")
            
            # Process the file
            from document_processor import DocumentProcessor
            processor = DocumentProcessor()
            result = processor.process_task(file_path, task_type)
            
            if not result.get('success', False):
                logger.error(f"Processing failed: {result.get('error')}")
                return False
            
            # Get text content
            text_content = result.get('text', '')
            
            # If specialized task, also run text_extraction
            if not text_content or task_type != 'text_extraction':
                text_result = processor.process_task(file_path, 'text_extraction')
                if text_result.get('success'):
                    text_content = text_result.get('text', '')
                    if 'keywords' not in result:
                        result['keywords'] = text_result.get('keywords', [])
                    if 'metadata' not in result:
                        result['metadata'] = text_result.get('metadata', {})
            
            if not text_content or not text_content.strip():
                logger.error(f"No text content extracted")
                return False
            
            # ONE FINAL DUPLICATE CHECK before indexing
            if self._is_file_already_indexed(file_path):
                logger.info(f"File was indexed by another task, skipping")
                with self.lock:
                    self.completed_tasks[task_id] = {
                        'result': result,
                        'completion_time': time.time(),
                        'processed_by': 'duplicate_skip'
                    }
                    if task_id in self.pending_tasks:
                        del self.pending_tasks[task_id]
                return True
            
            # Index the document
            file_id = str(uuid.uuid4())
            keywords = result.get('keywords', text_content.split()[:10])
            metadata = result.get('metadata', {
                'file_name': os.path.basename(file_path),
                'processed_time': datetime.now().isoformat(),
                'file_size': stable_size
            })
            
            index_success = self.search_index.add_document(
                file_id=file_id,
                file_name=os.path.basename(file_path),
                content=text_content,
                keywords=keywords,
                metadata=metadata,
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
                
                logger.info(f"Successfully processed: {task_data['file_name']}")
                logger.info(f"  {len(text_content)} chars, {len(keywords)} keywords")
                return True
            
            logger.error(f"Failed to index")
            return False
                
        except Exception as e:
            logger.error(f"Local processing error: {e}")
            import traceback
            logger.error(traceback.format_exc())
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
        """Handle completed task result - FIXED for keyword_extraction"""
        with self.lock:
            if task_id in self.pending_tasks:
                task_info = self.pending_tasks[task_id]
                task_type = task_info['task_data']['task_type']
                
                # CRITICAL FIX: keyword_extraction doesn't return text
                if task_type == 'keyword_extraction':
                    # For keyword extraction, we just need keywords, not text
                    if result.get('success', False) and result.get('keywords'):
                        # Mark as completed without indexing
                        self.node_job_stats[peer_id]['total_jobs'] += 1
                        self.node_job_stats[peer_id]['completed_jobs'] += 1
                        self.node_job_stats[peer_id]['job_types'][task_type] += 1
                        self.node_job_stats[peer_id]['last_job_time'] = time.time()
                        
                        self.completed_tasks[task_id] = {
                            'task_info': task_info,
                            'result': result,
                            'completion_time': time.time(),
                            'processed_by': peer_id
                        }
                        logger.info(f"Keyword extraction completed by {peer_id}")
                        del self.pending_tasks[task_id]
                        self.peer_load[peer_id] = max(0, self.peer_load[peer_id] - 1)
                        return
                
                # For text_extraction and other tasks, check for text content
                if result.get('success', False) and result.get('text', '').strip():
                    self.node_job_stats[peer_id]['total_jobs'] += 1
                    self.node_job_stats[peer_id]['completed_jobs'] += 1
                    self.node_job_stats[peer_id]['job_types'][task_type] += 1
                    self.node_job_stats[peer_id]['last_job_time'] = time.time()
                    
                    # Only index for text_extraction tasks
                    if task_type == 'text_extraction':
                        file_id = str(uuid.uuid4())
                        index_success = self.search_index.add_document(
                            file_id=file_id,
                            file_name=result['metadata']['file_name'],
                            content=result['text'],
                            keywords=result.get('keywords', []),
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
                        # Non-indexing task completed successfully
                        self.completed_tasks[task_id] = {
                            'task_info': task_info,
                            'result': result,
                            'completion_time': time.time(),
                            'processed_by': peer_id
                        }
                        logger.info(f"Task {task_type} completed by {peer_id}")
                else:
                    # Task failed
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


