#!/usr/bin/env python3
"""
Run multiple node instances on a single machine for testing distributed functionality
"""
import subprocess
import sys
import time
import os
import signal
import requests

class MultiNodeRunner:
    def __init__(self):
        self.processes = []
        
    def run_node(self, port, node_name):
        """Run a node instance on specified port"""
        print(f"🚀 Starting {node_name} on port {port}...")
        
        # Create unique directories for each node
        upload_folder = f"uploads_node_{port}"
        processed_folder = f"processed_node_{port}" 
        index_folder = f"search_index_node_{port}"
        
        # Create directories
        os.makedirs(upload_folder, exist_ok=True)
        os.makedirs(processed_folder, exist_ok=True)
        os.makedirs(index_folder, exist_ok=True)
        
        # Set environment variables
        env = os.environ.copy()
        env['UPLOAD_FOLDER'] = upload_folder
        env['PROCESSED_FOLDER'] = processed_folder
        env['INDEX_FOLDER'] = index_folder
        
        try:
            # Run the node process
            process = subprocess.Popen([
                sys.executable, 'main.py'
            ], 
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
            )
            
            self.processes.append((process, port, node_name))
            return process
            
        except Exception as e:
            print(f"❌ Failed to start {node_name}: {e}")
            return None

    def monitor_output(self):
        """Monitor output from all nodes"""
        while True:
            for process, port, name in self.processes:
                # Read stdout
                output = process.stdout.readline()
                if output:
                    print(f"[{name}:{port}] {output.strip()}")
                
                # Read stderr
                error = process.stderr.readline()
                if error:
                    print(f"[{name}:{port} ERROR] {error.strip()}")
            
            time.sleep(0.1)

    def test_network_connectivity(self):
        """Test if nodes can discover each other"""
        print("\n🔍 Testing network connectivity...")
        time.sleep(8)  # Give nodes time to start and discover each other
        
        test_ports = [5000, 5001, 5002]
        
        for port in test_ports:
            try:
                response = requests.get(f"http://localhost:{port}/api/nodes", timeout=10)
                data = response.json()
                node_count = data['total_nodes']
                print(f"📍 Node {port}: Can see {node_count} total nodes in network")
                
                # Show detailed node info
                if node_count > 0:
                    for node in data['nodes']:
                        status = "SELF" if node.get('is_self') else "PEER"
                        leader = " 👑" if node.get('is_leader') else ""
                        print(f"   └─ {node['id']} ({status}{leader})")
                        
            except Exception as e:
                print(f"❌ Node {port}: Not accessible - {e}")

    def stop_all_nodes(self):
        """Stop all running node processes"""
        print("\n⏹️ Stopping all nodes...")
        for process, port, name in self.processes:
            try:
                process.terminate()
                process.wait(timeout=5)
                print(f"✅ Stopped {name} (port {port})")
            except:
                process.kill()
                print(f"⚠️  Force stopped {name} (port {port})")
        
        self.processes = []

    def signal_handler(self, sig, frame):
        """Handle Ctrl+C gracefully"""
        print("\n\n🛑 Received interrupt signal...")
        self.stop_all_nodes()
        sys.exit(0)

def main():
    runner = MultiNodeRunner()
    
    # Set up signal handler for Ctrl+C
    signal.signal(signal.SIGINT, runner.signal_handler)
    
    print("🔧 Single System Multi-Node Distributed Test")
    print("=" * 60)
    
    # Define nodes to run (port, name, capabilities can be customized)
    nodes = [
        (5000, "Node-Alpha", "Primary node with all capabilities"),
        (5001, "Node-Beta", "Worker node with text processing"), 
        (5002, "Node-Gamma", "Worker node with basic capabilities")
    ]
    
    print("📋 Starting the following nodes:")
    for port, name, desc in nodes:
        print(f"   • {name}: port {port} - {desc}")
    
    print("\n" + "=" * 60)
    
    try:
        # Start all nodes
        successful_starts = 0
        for port, name, desc in nodes:
            if runner.run_node(port, name):
                successful_starts += 1
            time.sleep(3)  # Stagger startup to avoid port conflicts
        
        print(f"\n✅ Successfully started {successful_starts}/{len(nodes)} nodes")
        
        if successful_starts == 0:
            print("❌ No nodes started successfully. Exiting.")
            return
        
        # Test network connectivity
        runner.test_network_connectivity()
        
        print("\n" + "=" * 60)
        print("🌐 NETWORK STATUS:")
        print("=" * 60)
        print("Access each node at:")
        for port, name, desc in nodes:
            print(f"   {name}: http://localhost:{port}")
        
        print("\n🎯 TESTING INSTRUCTIONS:")
        print("1. Open multiple browser tabs/windows for different nodes")
        print("2. Upload files to any node - they should distribute automatically")
        print("3. Check 'Network Nodes' page to see peer discovery")
        print("4. Search for documents on any node - all should see the same index")
        print("5. Watch the console for task distribution logs")
        
        print("\n" + "=" * 60)
        print("🔄 Nodes are running. Press Ctrl+C to stop all nodes.")
        print("=" * 60)
        
        # Start monitoring output (optional)
        # runner.monitor_output()
        
        # Keep running until interrupted
        while True:
            time.sleep(1)
            
    except Exception as e:
        print(f"❌ Error: {e}")
        runner.stop_all_nodes()

if __name__ == "__main__":
    main()