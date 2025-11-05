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
        self.task_manager = task_manager
        self.lock = threading.Lock()
        
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
        print(f" Peer Discovery Started - Node ID: {self.node_id}")
    
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
                # print(f" Broadcast sent from {self.node_id}")
            except Exception as e:
                print(f" Broadcast error: {e}")
            
            time.sleep(Config.HEARTBEAT_INTERVAL)

    def _listen_for_peers(self):
        """Listen for peer broadcasts - FIXED VERSION"""
        while self.running:
            try:
                data, addr = self.listener_socket.recvfrom(1024)
                message = json.loads(data.decode('utf-8'))
                
                if message['type'] == 'heartbeat' and message['node_id'] != self.node_id:
                    peer_id = message['node_id']
                    peer_ip = addr[0]  # Use the actual sender IP
                    
                    with self.lock:
                        self.peers[peer_id] = {
                            'ip': peer_ip,  # Use actual sender IP, not the one in message
                            'capabilities': message.get('capabilities', {}),
                            'last_seen': time.time(),
                            'task_port': message.get('task_port', Config.TASK_PORT),
                            'file_port': message.get('file_port', Config.FILE_PORT)
                        }
                    
                    # Update task manager with peer capabilities
                    if self.task_manager:
                        self.task_manager.update_peer_capabilities(peer_id, {
                            **message.get('capabilities', {}),
                            'ip': peer_ip  # Include IP for task distribution
                        })
                    
                    print(f" Discovered peer: {peer_id} at {peer_ip}")
                    print(f"   Active peers: {len(self.peers)}")
                    
            except socket.timeout:
                continue
            except json.JSONDecodeError:
                continue
            except Exception as e:
                if self.running:  # Only log if we're supposed to be running
                    print(f" Listener error: {e}")
    
    def _cleanup_peers(self):
        """Remove peers that haven't been seen recently"""
        while self.running:
            try:
                current_time = time.time()
                dead_peers = []
                
                with self.lock:
                    for peer_id, peer_info in self.peers.items():
                        if current_time - peer_info['last_seen'] > Config.HEARTBEAT_INTERVAL * 3:
                            dead_peers.append(peer_id)
                
                for peer_id in dead_peers:
                    # Notify task manager about dead peer
                    if self.task_manager:
                        self.task_manager.remove_peer(peer_id)
                    with self.lock:
                        del self.peers[peer_id]
                    print(f" Peer {peer_id} timed out")
                    
            except Exception as e:
                print(f" Cleanup error: {e}")
            
            time.sleep(Config.HEARTBEAT_INTERVAL)
    
    def _leader_election(self):
        """Simple leader election"""
        while self.running:
            try:
                with self.lock:
                    all_nodes = [self.node_id] + list(self.peers.keys())
                    all_nodes.sort()
                    new_leader = all_nodes[0]
                    
                    if new_leader != self.leader_id:
                        self.leader_id = new_leader
                        print(f" New leader elected: {self.leader_id}")
                
            except Exception as e:
                print(f" Leader election error: {e}")
            
            time.sleep(Config.HEARTBEAT_INTERVAL * 2)

    def get_peers(self):
        """Get current peer list"""
        with self.lock:
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