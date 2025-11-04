import socket
import threading
import time
import json
import uuid
import logging
from config import Config, get_local_ip

class PeerNode:
    def __init__(self, node_id=None, task_manager=None):
        self.node_id = node_id or f"{get_local_ip()}_{uuid.uuid4().hex[:8]}"
        self.local_ip = get_local_ip()
        self.peers = {}
        self.running = True
        self.leader_id = None
        self.task_manager = task_manager  # Reference to task manager
        
        # Setup broadcast socket
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Setup listener socket
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener_socket.bind(('0.0.0.0', Config.BROADCAST_PORT))
        self.listener_socket.settimeout(1.0)
        
        # Start threads
        threading.Thread(target=self._broadcast_presence, daemon=True).start()
        threading.Thread(target=self._listen_for_peers, daemon=True).start()
        threading.Thread(target=self._cleanup_peers, daemon=True).start()
        threading.Thread(target=self._leader_election, daemon=True).start()
        
        logging.info(f"Peer node {self.node_id} started on {self.local_ip}")
        print(f"🔍 Peer Discovery Started - Node ID: {self.node_id}")
    
    def _broadcast_presence(self):
        """Broadcast presence to LAN"""
        message = {
            'type': 'heartbeat',
            'node_id': self.node_id,
            'ip': self.local_ip,
            'capabilities': Config.CAPABILITIES,
            'timestamp': time.time(),
            'task_port': Config.TASK_PORT,
            'file_port': Config.FILE_PORT
        }
        
        while self.running:
            try:
                message_bytes = json.dumps(message).encode('utf-8')
                self.broadcast_socket.sendto(message_bytes, 
                                           (Config.BROADCAST_ADDR, Config.BROADCAST_PORT))
                print(f"📡 Broadcast sent from {self.node_id}")
            except Exception as e:
                print(f"❌ Broadcast error: {e}")
            
            time.sleep(Config.HEARTBEAT_INTERVAL)

    def _send_task_to_peer(self, peer, task_data):
        """Send task to peer node - FIXED VERSION"""
        try:
            # Extract IP from peer ID (format: IP_UUID)
            peer_id_parts = peer['id'].split('_')
            if len(peer_id_parts) >= 2:
                peer_ip = peer_id_parts[0]
            else:
                logging.error(f"Cannot extract IP from peer ID: {peer['id']}")
                return False
            
            # Use default ports (simplified approach)
            peer_task_port = Config.TASK_PORT
            peer_file_port = Config.FILE_PORT
            
            logging.info(f"📤 Attempting to send task to {peer_ip}:{peer_task_port}")
            
            # First, send the file
            file_sent = self._send_file_to_peer(peer_ip, peer_file_port, task_data['file_path'])
            if not file_sent:
                logging.error(f"❌ Failed to send file to {peer_ip}")
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
                    logging.error(f"❌ Task timeout from {peer_ip}")
                    return False
                    
                result = json.loads(result_data.decode('utf-8').replace("<END_RESULT>", ""))
                task_socket.close()
                
                # Process result
                self._handle_task_result(task_data['task_id'], result, peer['id'])
                logging.info(f"✅ Task completed by {peer['id']}")
                return True
                
            except Exception as e:
                logging.error(f"❌ Connection error to {peer_ip}:{peer_task_port}: {e}")
                return False
                
        except Exception as e:
            logging.error(f"❌ Error sending task to peer {peer['id']}: {e}")
            return False
    
    def _listen_for_peers(self):
        """Listen for peer broadcasts"""
        while self.running:
            try:
                data, addr = self.listener_socket.recvfrom(1024)
                message = json.loads(data.decode('utf-8'))
                
                if message['type'] == 'heartbeat' and message['node_id'] != self.node_id:
                    peer_id = message['node_id']
                    self.peers[peer_id] = {
                        'ip': message['ip'],
                        'capabilities': message['capabilities'],
                        'last_seen': time.time(),
                        'task_port': message.get('task_port', Config.TASK_PORT),
                        'file_port': message.get('file_port', Config.FILE_PORT)
                    }
                    
                    # Update task manager with peer capabilities
                    if self.task_manager:
                        self.task_manager.update_peer_capabilities(peer_id, message['capabilities'])
                    
                    print(f"✅ Discovered peer: {peer_id} at {message['ip']}")
                    print(f"   Active peers: {len(self.peers)}")
                    
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:  # Only log if we're supposed to be running
                    print(f"❌ Listener error: {e}")
    
    def _cleanup_peers(self):
        """Remove peers that haven't been seen recently"""
        while self.running:
            try:
                current_time = time.time()
                dead_peers = []
                
                for peer_id, peer_info in self.peers.items():
                    if current_time - peer_info['last_seen'] > Config.HEARTBEAT_INTERVAL * 3:
                        dead_peers.append(peer_id)
                
                for peer_id in dead_peers:
                    # Notify task manager about dead peer
                    if self.task_manager:
                        self.task_manager.remove_peer(peer_id)
                    del self.peers[peer_id]
                    print(f"💀 Peer {peer_id} timed out")
                    
            except Exception as e:
                print(f"❌ Cleanup error: {e}")
            
            time.sleep(Config.HEARTBEAT_INTERVAL)
    
    def _leader_election(self):
        """Simple leader election"""
        while self.running:
            try:
                if not self.peers:
                    self.leader_id = self.node_id
                else:
                    all_nodes = [self.node_id] + list(self.peers.keys())
                    all_nodes.sort()
                    new_leader = all_nodes[0]
                    if new_leader != self.leader_id:
                        self.leader_id = new_leader
                        print(f"👑 New leader elected: {self.leader_id}")
                
            except Exception as e:
                print(f"❌ Leader election error: {e}")
            
            time.sleep(Config.HEARTBEAT_INTERVAL * 2)
    def update_peer_capabilities(self, peer_id, capabilities):
        """Update peer capabilities with IP information"""
        with self.lock:
            # Include IP in the capabilities info for TaskManager
            if peer_id in self.peers:
                capabilities_with_ip = capabilities.copy()
                capabilities_with_ip['ip'] = self.peers[peer_id]['ip']
                self.peer_capabilities[peer_id] = capabilities_with_ip
            else:
                self.peer_capabilities[peer_id] = capabilities
    
    def get_peers(self):
        """Get current peer list"""
        return self.peers.copy()
    
    def get_leader(self):
        """Get current leader"""
        return self.leader_id
    
    def is_leader(self):
        """Check if this node is the leader"""
        return self.leader_id == self.node_id
    
    def stop(self):
        """Stop the peer node"""
        self.running = False
        self.broadcast_socket.close()
        self.listener_socket.close()