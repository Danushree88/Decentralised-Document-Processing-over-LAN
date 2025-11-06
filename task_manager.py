import socket
import json
import threading
import time
import logging
import os
from config import Config

logger = logging.getLogger(__name__)

class TaskManager:
    def __init__(self, node_id, search_index, document_processor=None):
        self.node_id = node_id
        self.search_index = search_index
        self.document_processor = document_processor
        self.pending_tasks = {}
        self.completed_tasks = 0
        self.failed_tasks = 0
        self.peer_load = {}
        self.peer_capabilities = {}
        self.lock = threading.Lock()
        
        # Job statistics per node
        self.node_job_stats = {}
        
        # ✅ FIX 1: Get this node's IP address
        self.my_ip = self._get_local_ip()
        
        # Setup task listener
        self.task_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.task_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.task_socket.bind(('', Config.TASK_PORT))
        
        # Start task listener thread
        threading.Thread(target=self._listen_for_responses, daemon=True).start()
        threading.Thread(target=self._monitor_tasks, daemon=True).start()
        
        logger.info(f"✅ Task Manager initialized for node: {node_id}")
        logger.info(f"   My IP: {self.my_ip}")
        logger.info(f"   Listening on port: {Config.TASK_PORT}")
    
    def _get_local_ip(self):
        """Get the local IP address of this machine"""
        try:
            # Create a socket to determine local IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))  # Connect to external IP (doesn't actually send data)
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception:
            return "127.0.0.1"
    
    def update_peer_capabilities(self, peer_id, capabilities):
        """Update capabilities for a peer - CRITICAL for routing"""
        with self.lock:
            # Extract and normalize node type
            node_type = capabilities.get('node_type') or capabilities.get('NODE_TYPE', 'UNKNOWN')
            
            # CRITICAL: Extract the peer's task port
            peer_task_port = capabilities.get('task_port', Config.BASE_TASK_PORT)
            peer_file_port = capabilities.get('file_port', Config.BASE_FILE_PORT)
            
            # Build comprehensive capability record
            self.peer_capabilities[peer_id] = {
                'ip': capabilities.get('ip'),
                'task_port': peer_task_port,
                'file_port': peer_file_port,
                'node_type': node_type,
                'pdf_processing': capabilities.get('pdf_processing', False) or node_type == 'PDF',
                'txt_processing': capabilities.get('txt_processing', False) or node_type == 'TXT',
                'text_extraction': capabilities.get('text_extraction', False) or node_type == 'TXT',
                'keyword_extraction': capabilities.get('keyword_extraction', False) or node_type == 'TXT',
                'capabilities': capabilities.get('capabilities', {}),
                'last_seen': time.time()
            }
            
            # Initialize load tracking
            if peer_id not in self.peer_load:
                self.peer_load[peer_id] = 0
            
            # Initialize job stats
            if peer_id not in self.node_job_stats:
                self.node_job_stats[peer_id] = {
                    'total_jobs': 0,
                    'completed_jobs': 0,
                    'failed_jobs': 0,
                    'job_types': {},
                    'last_active': None,
                    'completion_rate': 0.0
                }
            
            # Log only new peers or significant changes
            if peer_id not in self.peer_load or self.peer_load.get(peer_id, 0) == 0:
                logger.info(f"📋 Registered peer: {peer_id[:25]}...")
                logger.info(f"   Type: {node_type} | Port: {peer_task_port} | PDF: {self.peer_capabilities[peer_id]['pdf_processing']} | TXT: {self.peer_capabilities[peer_id]['txt_processing']}")
    
    def remove_peer(self, peer_id):
        """Remove a peer that's no longer available"""
        with self.lock:
            if peer_id in self.peer_capabilities:
                del self.peer_capabilities[peer_id]
            if peer_id in self.peer_load:
                del self.peer_load[peer_id]
            logger.info(f"❌ Removed peer: {peer_id[:25]}...")
    
    def _find_suitable_peers(self, required_capability):
        """Find peers capable of handling a specific task type"""
        with self.lock:
            suitable_peers = []
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🔍 Finding peers for: {required_capability}")
            logger.info(f"{'='*60}")
            logger.info(f"Total registered peers: {len(self.peer_capabilities)}")
            
            for peer_id, caps in self.peer_capabilities.items():
                node_type = caps.get('node_type', 'UNKNOWN')
                peer_port = caps.get('task_port', 8889)
                
                can_handle = False
                
                if required_capability == 'pdf_processing':
                    can_handle = (
                        caps.get('pdf_processing', False) or 
                        node_type.upper() == 'PDF'
                    )
                    
                elif required_capability in ['txt_processing', 'text_extraction']:
                    can_handle = (
                        caps.get('txt_processing', False) or 
                        caps.get('text_extraction', False) or
                        node_type.upper() == 'TXT'
                    )
                    
                elif required_capability == 'keyword_extraction':
                    can_handle = (
                        caps.get('keyword_extraction', False) or
                        node_type.upper() == 'TXT'
                    )
                
                short_id = peer_id[:20] + '...' if len(peer_id) > 20 else peer_id
                if can_handle:
                    suitable_peers.append({
                        'id': peer_id,
                        'ip': caps.get('ip'),
                        'task_port': peer_port,
                        'node_type': node_type,
                        'load': self.peer_load.get(peer_id, 0)
                    })
                    logger.info(f"   ✅ {node_type} node ({short_id}) Port:{peer_port} - Load: {self.peer_load.get(peer_id, 0)}")
                else:
                    logger.info(f"   ❌ {node_type} node ({short_id}) Port:{peer_port} - Cannot handle {required_capability}")
            
            # Sort by load (least loaded first)
            suitable_peers.sort(key=lambda p: p['load'])
            
            logger.info(f"{'='*60}")
            logger.info(f"Found {len(suitable_peers)} suitable peer(s)")
            logger.info(f"{'='*60}\n")
            
            return suitable_peers
    
    def distribute_task(self, file_path, task_type='full'):
        """Distribute a task to the most suitable peer"""
        try:
            # Determine required capability based on file extension
            file_ext = os.path.splitext(file_path)[1].lower()
            
            if file_ext == '.pdf':
                required_capability = 'pdf_processing'
            elif file_ext == '.txt':
                required_capability = 'txt_processing'
            else:
                required_capability = 'text_extraction'
            
            logger.info(f"\n{'='*60}")
            logger.info(f"📤 DISTRIBUTING TASK")
            logger.info(f"{'='*60}")
            logger.info(f"File: {os.path.basename(file_path)}")
            logger.info(f"Type: {file_ext}")
            logger.info(f"Required: {required_capability}")
            logger.info(f"{'='*60}")
            
            # Find suitable peers
            suitable_peers = self._find_suitable_peers(required_capability)
            
            if not suitable_peers:
                logger.warning(f"⚠️  No peers found for {required_capability}")
                logger.warning(f"   Processing locally with fallback")
                
                # FALLBACK: Process locally if no peers available
                if self.document_processor:
                    try:
                        result = self.document_processor.process_task(file_path, required_capability)
                        if result['success']:
                            file_id = str(time.time())
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
                            logger.info(f"✅ Processed locally: {os.path.basename(file_path)}")
                            return f"local_{int(time.time())}"
                    except Exception as e:
                        logger.error(f"Local processing failed: {e}")
                
                return None
            
            # Select least loaded peer
            selected_peer = suitable_peers[0]
            
            logger.info(f"\n{'='*60}")
            logger.info(f"🎯 SELECTED PEER")
            logger.info(f"{'='*60}")
            logger.info(f"Peer ID: {selected_peer['id'][:30]}...")
            logger.info(f"Node Type: {selected_peer['node_type']}")
            logger.info(f"IP: {selected_peer['ip']}")
            logger.info(f"Port: {selected_peer['task_port']}")
            logger.info(f"Current Load: {selected_peer['load']}")
            logger.info(f"{'='*60}\n")
            
            # Create task
            task_id = f"task_{int(time.time())}_{os.path.basename(file_path)}"
            
            # ✅ FIX 2: Include coordinator's IP and port in task data
            task_data = {
                'task_id': task_id,
                'file_path': file_path,
                'file_name': os.path.basename(file_path),
                'task_type': required_capability,
                'node_id': self.node_id,
                'coordinator_ip': self.my_ip,  # ⬅️ ADD THIS
                'coordinator_port': Config.TASK_PORT,  # ⬅️ ADD THIS
                'timestamp': time.time()
            }
            
            # Send task to peer
            success = self._send_task_to_peer(selected_peer, task_data)
            
            if success:
                with self.lock:
                    # Track pending task
                    self.pending_tasks[task_id] = {
                        'peer_id': selected_peer['id'],
                        'peer_ip': selected_peer['ip'],
                        'peer_port': selected_peer['task_port'],
                        'task_data': task_data,
                        'start_time': time.time(),
                        'status': 'pending',
                        'attempts': 1
                    }
                    
                    # Update peer load
                    self.peer_load[selected_peer['id']] = self.peer_load.get(selected_peer['id'], 0) + 1
                    
                    # Update job stats
                    if selected_peer['id'] not in self.node_job_stats:
                        self.node_job_stats[selected_peer['id']] = {
                            'total_jobs': 0,
                            'completed_jobs': 0,
                            'failed_jobs': 0,
                            'job_types': {},
                            'last_active': None,
                            'completion_rate': 0.0
                        }
                    
                    self.node_job_stats[selected_peer['id']]['total_jobs'] += 1
                    
                    # Track job type
                    job_type = self.node_job_stats[selected_peer['id']]['job_types']
                    job_type[required_capability] = job_type.get(required_capability, 0) + 1
                
                logger.info(f"✅ Task {task_id} sent to {selected_peer['node_type']} node at port {selected_peer['task_port']}")
                return task_id
            else:
                logger.error(f"❌ Failed to send task to peer")
                return None
                
        except Exception as e:
            logger.error(f"Task distribution error: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None
    
    def distribute_batch(self, file_paths):
        """Distribute multiple tasks - SMART ROUTING"""
        logger.info(f"\n{'='*60}")
        logger.info(f"📦 BATCH DISTRIBUTION")
        logger.info(f"{'='*60}")
        logger.info(f"Total files: {len(file_paths)}")
        
        # Group files by type
        pdf_files = []
        txt_files = []
        other_files = []
        
        for file_path in file_paths:
            ext = os.path.splitext(file_path)[1].lower()
            if ext == '.pdf':
                pdf_files.append(file_path)
            elif ext == '.txt':
                txt_files.append(file_path)
            else:
                other_files.append(file_path)
        
        logger.info(f"PDF files: {len(pdf_files)}")
        logger.info(f"TXT files: {len(txt_files)}")
        logger.info(f"Other files: {len(other_files)}")
        logger.info(f"{'='*60}\n")
        
        distributed_tasks = []
        
        # Distribute each file
        for file_path in file_paths:
            task_id = self.distribute_task(file_path)
            if task_id:
                distributed_tasks.append({
                    'task_id': task_id,
                    'file_path': file_path,
                    'file_name': os.path.basename(file_path),
                    'status': 'distributed'
                })
                time.sleep(0.1)  # Small delay between tasks
        
        logger.info(f"\n✅ Distributed {len(distributed_tasks)}/{len(file_paths)} tasks")
        return distributed_tasks
    
    def _send_task_to_peer(self, peer, task_data):
        """Send task to a specific peer via UDP"""
        try:
            message = json.dumps({
                'type': 'task_assignment',
                'data': task_data
            }).encode('utf-8')
            
            peer_port = peer.get('task_port', Config.TASK_PORT)
            peer_ip = peer['ip']
            
            self.task_socket.sendto(message, (peer_ip, peer_port))
            logger.info(f"📨 Task sent to {peer_ip}:{peer_port}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send task: {e}")
            return False
    
    def _listen_for_responses(self):
        """Listen for task completion responses"""
        logger.info("👂 Listening for task responses...")
        
        while True:
            try:
                data, addr = self.task_socket.recvfrom(65536)
                message = json.loads(data.decode('utf-8'))
                
                if message.get('type') == 'task_result':
                    self._handle_task_result(message.get('data'))
                elif message.get('type') == 'task_assignment':
                    # This node received a task assignment
                    self._handle_task_assignment(message.get('data'))
                    
            except Exception as e:
                logger.error(f"Response listener error: {e}")
    
    def _handle_task_result(self, result_data):
        """Handle task completion result"""
        try:
            task_id = result_data.get('task_id')
            
            with self.lock:
                if task_id not in self.pending_tasks:
                    return
                
                task_info = self.pending_tasks[task_id]
                peer_id = task_info['peer_id']
                
                if result_data.get('success'):
                    # Task completed successfully
                    logger.info(f"✅ Task completed: {task_id}")
                    
                    # Add to search index
                    self.search_index.add_document(
                        file_id=task_id,
                        file_name=result_data.get('file_name'),
                        content=result_data.get('text', ''),
                        keywords=result_data.get('keywords', []),
                        metadata=result_data.get('metadata', {}),
                        node_id=peer_id
                    )
                    
                    self.completed_tasks += 1
                    
                    # Update job stats
                    if peer_id in self.node_job_stats:
                        self.node_job_stats[peer_id]['completed_jobs'] += 1
                        self.node_job_stats[peer_id]['last_active'] = time.time()
                        
                        # Update completion rate
                        total = self.node_job_stats[peer_id]['total_jobs']
                        completed = self.node_job_stats[peer_id]['completed_jobs']
                        self.node_job_stats[peer_id]['completion_rate'] = completed / total if total > 0 else 0
                else:
                    logger.error(f"❌ Task failed: {task_id}")
                    self.failed_tasks += 1
                    
                    if peer_id in self.node_job_stats:
                        self.node_job_stats[peer_id]['failed_jobs'] += 1
                
                # Update peer load
                if peer_id in self.peer_load:
                    self.peer_load[peer_id] = max(0, self.peer_load[peer_id] - 1)
                
                # Remove from pending
                del self.pending_tasks[task_id]
                
        except Exception as e:
            logger.error(f"Error handling task result: {e}")
    
    def _handle_task_assignment(self, task_data):
        """Handle incoming task assignment (when this node is a worker)"""
        try:
            logger.info(f"\n{'='*60}")
            logger.info(f"📥 RECEIVED TASK ASSIGNMENT")
            logger.info(f"{'='*60}")
            logger.info(f"Task ID: {task_data.get('task_id')}")
            logger.info(f"File: {task_data.get('file_name')}")
            logger.info(f"Type: {task_data.get('task_type')}")
            logger.info(f"{'='*60}\n")
            
            # Process task locally using document_processor
            if not self.document_processor:
                logger.error("No document processor available!")
                return
            
            file_path = task_data.get('file_path')
            task_type = task_data.get('task_type')
            
            result = self.document_processor.process_task(file_path, task_type)
            
            # Send result back
            result_message = {
                'type': 'task_result',
                'data': {
                    'task_id': task_data.get('task_id'),
                    'success': result.get('success'),
                    'text': result.get('text', ''),
                    'keywords': result.get('keywords', []),
                    'metadata': result.get('metadata', {}),
                    'file_name': task_data.get('file_name'),
                    'error': result.get('error')
                }
            }
            
            # ✅ FIX 3: Use coordinator IP and port from task_data
            coordinator_ip = task_data.get('coordinator_ip')
            coordinator_port = task_data.get('coordinator_port', Config.TASK_PORT)
            
            if coordinator_ip:
                try:
                    message = json.dumps(result_message).encode('utf-8')
                    self.task_socket.sendto(message, (coordinator_ip, coordinator_port))
                    logger.info(f"📤 Result sent back to coordinator at {coordinator_ip}:{coordinator_port}")
                except Exception as e:
                    logger.error(f"Failed to send result back: {e}")
            else:
                logger.error(f"No coordinator IP in task data!")
            
        except Exception as e:
            logger.error(f"Error handling task assignment: {e}")
            import traceback
            logger.error(traceback.format_exc())
    
    def _monitor_tasks(self):
        """Monitor and timeout stale tasks"""
        while True:
            try:
                time.sleep(10)
                
                with self.lock:
                    current_time = time.time()
                    stale_tasks = []
                    
                    for task_id, task_info in self.pending_tasks.items():
                        if current_time - task_info['start_time'] > Config.TASK_TIMEOUT:
                            stale_tasks.append(task_id)
                    
                    for task_id in stale_tasks:
                        task_info = self.pending_tasks[task_id]
                        peer_id = task_info['peer_id']
                        
                        logger.warning(f"⏰ Task timeout: {task_id}")
                        
                        # Update load
                        if peer_id in self.peer_load:
                            self.peer_load[peer_id] = max(0, self.peer_load[peer_id] - 1)
                        
                        # Update stats
                        if peer_id in self.node_job_stats:
                            self.node_job_stats[peer_id]['failed_jobs'] += 1
                        
                        self.failed_tasks += 1
                        del self.pending_tasks[task_id]
                        
            except Exception as e:
                logger.error(f"Task monitor error: {e}")
    
    def get_stats(self):
        """Get current statistics"""
        with self.lock:
            # Calculate job distribution
            job_distribution = {}
            for node_id, stats in self.node_job_stats.items():
                total = stats['total_jobs']
                completed = stats['completed_jobs']
                job_distribution[node_id] = {
                    'total_jobs': total,
                    'completed_jobs': completed,
                    'failed_jobs': stats['failed_jobs'],
                    'completion_rate': completed / total if total > 0 else 0
                }
            
            return {
                'pending_tasks': len(self.pending_tasks),
                'completed_tasks': self.completed_tasks,
                'failed_tasks': self.failed_tasks,
                'peer_load': dict(self.peer_load),
                'connected_peers': len(self.peer_capabilities),
                'job_distribution': job_distribution,
                'node_job_stats': self.node_job_stats
            }