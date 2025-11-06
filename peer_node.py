# peer_node.py - FIXED with port extraction

import socket
import json
import threading
import time
import logging
from config import Config

logger = logging.getLogger(__name__)

class PeerNode:
    def __init__(self, task_manager=None):
        self.node_id = f"{Config.NODE_TYPE}_{socket.gethostname()}_{int(time.time())}"
        self.local_ip = self._get_local_ip()
        self.task_manager = task_manager
        self.peers = {}
        self.running = True
        
        # Setup broadcast socket
        self.broadcast_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.broadcast_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Setup listener socket
        self.listener_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.listener_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.listener_socket.bind(('', Config.BROADCAST_PORT))
        
        # Start threads
        threading.Thread(target=self._broadcast_presence, daemon=True).start()
        threading.Thread(target=self._listen_for_peers, daemon=True).start()
        
        logger.info(f"✅ Peer Node initialized: {self.node_id}")
        logger.info(f"   Node Type: {Config.NODE_TYPE}")
        logger.info(f"   Local IP: {self.local_ip}")
        logger.info(f"   Task Port: {Config.TASK_PORT}")
        logger.info(f"   File Port: {Config.FILE_PORT}")
        logger.info(f"   Capabilities: {Config.CAPABILITIES}")
    
    def _get_local_ip(self):
        """Get local IP address"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("8.8.8.8", 80))
                return s.getsockname()[0]
        except:
            return '127.0.0.1'
    
    def _broadcast_presence(self):
        """Broadcast presence with COMPLETE capabilities including ports"""
        broadcast_count = 0
        
        while self.running:
            try:
                # Create COMPREHENSIVE announcement with ports
                announcement = {
                    'node_id': self.node_id,
                    'node_type': Config.NODE_TYPE,
                    'NODE_TYPE': Config.NODE_TYPE,  # Backup
                    'ip': self.local_ip,
                    'task_port': Config.TASK_PORT,  # ⬅️ CRITICAL
                    'file_port': Config.FILE_PORT,  # ⬅️ CRITICAL
                    'timestamp': time.time(),
                    # Send FULL capabilities dict
                    'capabilities': dict(Config.CAPABILITIES),
                    # ALSO send individual flags for easy checking
                    'pdf_processing': Config.CAPABILITIES.get('pdf_processing', False),
                    'txt_processing': Config.CAPABILITIES.get('txt_processing', False),
                    'text_extraction': Config.CAPABILITIES.get('text_extraction', False),
                    'keyword_extraction': Config.CAPABILITIES.get('keyword_extraction', False),
                }
                
                # Broadcast
                message = json.dumps(announcement).encode('utf-8')
                self.broadcast_socket.sendto(
                    message, 
                    (Config.BROADCAST_ADDR, Config.BROADCAST_PORT)
                )
                
                # Log every 10th broadcast to reduce spam
                broadcast_count += 1
                if broadcast_count % 10 == 0:
                    logger.info(f"📡 Broadcasting: {Config.NODE_TYPE} node at {self.local_ip}:{Config.TASK_PORT}")
                
            except Exception as e:
                logger.error(f"Broadcast error: {e}")
            
            time.sleep(Config.HEARTBEAT_INTERVAL)
    
    def _listen_for_peers(self):
        """Listen for peer announcements and extract ALL information including ports"""
        logger.info(f"👂 Listening for peers on port {Config.BROADCAST_PORT}")
        
        while self.running:
            try:
                data, addr = self.listener_socket.recvfrom(4096)
                announcement = json.loads(data.decode('utf-8'))
                
                peer_id = announcement.get('node_id')
                peer_ip = announcement.get('ip')
                
                # Skip self-announcements
                if peer_id == self.node_id:
                    continue
                
                # Extract capabilities
                node_type = announcement.get('node_type') or announcement.get('NODE_TYPE', 'UNKNOWN')
                
                # ⬅️ CRITICAL: Extract port information
                peer_task_port = announcement.get('task_port', 8889)
                peer_file_port = announcement.get('file_port', 8890)
                
                # Build complete peer info WITH PORTS
                peer_capabilities = {
                    'ip': peer_ip,
                    'task_port': peer_task_port,  # ⬅️ STORE PORT
                    'file_port': peer_file_port,  # ⬅️ STORE PORT
                    'node_type': node_type,
                    'NODE_TYPE': node_type,
                    'last_seen': time.time(),
                    # Copy all capability flags
                    'pdf_processing': announcement.get('pdf_processing', False),
                    'txt_processing': announcement.get('txt_processing', False),
                    'text_extraction': announcement.get('text_extraction', False),
                    'keyword_extraction': announcement.get('keyword_extraction', False),
                    # Include full capabilities dict
                    'capabilities': announcement.get('capabilities', {})
                }
                
                # Check if new peer
                is_new_peer = peer_id not in self.peers
                
                # Update/Add peer
                self.peers[peer_id] = peer_capabilities
                
                # Log new discoveries with full detail INCLUDING PORTS
                if is_new_peer:
                    logger.info(f"\n{'='*60}")
                    logger.info(f"🎉 NEW PEER DISCOVERED!")
                    logger.info(f"{'='*60}")
                    logger.info(f"Peer ID: {peer_id}")
                    logger.info(f"Node Type: {node_type}")
                    logger.info(f"IP Address: {peer_ip}")
                    logger.info(f"Task Port: {peer_task_port}")  # ⬅️ LOG PORT
                    logger.info(f"File Port: {peer_file_port}")  # ⬅️ LOG PORT
                    logger.info(f"Capabilities:")
                    logger.info(f"  - pdf_processing: {peer_capabilities.get('pdf_processing')}")
                    logger.info(f"  - txt_processing: {peer_capabilities.get('txt_processing')}")
                    logger.info(f"  - text_extraction: {peer_capabilities.get('text_extraction')}")
                    logger.info(f"  - keyword_extraction: {peer_capabilities.get('keyword_extraction')}")
                    logger.info(f"{'='*60}\n")
                
                # Update task manager WITH PORT INFO
                if self.task_manager:
                    self.task_manager.update_peer_capabilities(peer_id, peer_capabilities)
                
            except json.JSONDecodeError:
                continue
            except Exception as e:
                logger.error(f"Error processing peer announcement: {e}")
    
    def get_peers(self):
        """Get all discovered peers (with stale cleanup)"""
        current_time = time.time()
        stale_peers = []
        
        for peer_id, info in self.peers.items():
            if current_time - info['last_seen'] > 30:
                stale_peers.append(peer_id)
        
        for peer_id in stale_peers:
            logger.warning(f"⚠️  Removing stale peer: {peer_id[:30]}...")
            del self.peers[peer_id]
            if self.task_manager:
                self.task_manager.remove_peer(peer_id)
        
        return self.peers
    
    def get_leader(self):
        """Get current leader"""
        return self.node_id
    
    def is_leader(self):
        """Check if this node is leader"""
        return True
    
    def stop(self):
        """Stop the peer node"""
        self.running = False
        try:
            self.broadcast_socket.close()
            self.listener_socket.close()
        except:
            pass
        logger.info("Peer node stopped")