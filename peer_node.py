import socket
import threading
import time
import json
import uuid
import logging
from config import Config, get_local_ip

class PeerNode:
    def __init__(self, node_id=None):
        self.node_id = node_id or f"{get_local_ip()}_{uuid.uuid4().hex[:8]}"
        self.local_ip = get_local_ip()
        self.peers = {}  # {peer_id: {'ip': ip, 'capabilities': capabilities, 'last_seen': timestamp}}
        self.running = True
        self.leader_id = None
        
        # Setup broadcast socket
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.settimeout(1.0)
        
        # Setup listener socket
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listener_socket.bind(('0.0.0.0', Config.BROADCAST_PORT))
        
        # Start threads
        threading.Thread(target=self._broadcast_presence, daemon=True).start()
        threading.Thread(target=self._listen_for_peers, daemon=True).start()
        threading.Thread(target=self._cleanup_peers, daemon=True).start()
        threading.Thread(target=self._leader_election, daemon=True).start()
        
        logging.info(f"Peer node {self.node_id} started on {self.local_ip}")
    
    def _broadcast_presence(self):
        """Broadcast presence to LAN"""
        message = {
            'type': 'heartbeat',
            'node_id': self.node_id,
            'ip': self.local_ip,
            'capabilities': Config.CAPABILITIES,
            'timestamp': time.time()
        }
        
        while self.running:
            try:
                message_bytes = json.dumps(message).encode('utf-8')
                self.broadcast_socket.sendto(message_bytes, (Config.BROADCAST_ADDR, Config.BROADCAST_PORT))
            except Exception as e:
                logging.error(f"Broadcast error: {e}")
            
            time.sleep(Config.HEARTBEAT_INTERVAL)
    
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
                        'last_seen': time.time()
                    }
                    
                    logging.debug(f"Discovered peer: {peer_id} at {message['ip']}")
                    
            except socket.timeout:
                continue
            except Exception as e:
                logging.error(f"Listener error: {e}")
    
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
                    del self.peers[peer_id]
                    logging.info(f"Peer {peer_id} timed out")
                    
            except Exception as e:
                logging.error(f"Cleanup error: {e}")
            
            time.sleep(Config.HEARTBEAT_INTERVAL)
    
    def _leader_election(self):
        """Simple leader election using Bully algorithm"""
        while self.running:
            try:
                if not self.peers:
                    self.leader_id = self.node_id
                else:
                    # Simple election: node with "lowest" ID becomes leader
                    all_nodes = [self.node_id] + list(self.peers.keys())
                    all_nodes.sort()
                    self.leader_id = all_nodes[0]
                
                time.sleep(Config.HEARTBEAT_INTERVAL * 2)
                
            except Exception as e:
                logging.error(f"Leader election error: {e}")
                time.sleep(5)
    
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