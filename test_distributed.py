#!/usr/bin/env python3
"""
Test distributed features across multiple nodes
"""
import requests
import time
import json
import random

class DistributedTester:
    def __init__(self):
        self.nodes = [
            {'url': 'http://localhost:5000', 'name': 'Node-Alpha'},
            {'url': 'http://localhost:5001', 'name': 'Node-Beta'},
            {'url': 'http://localhost:5002', 'name': 'Node-Gamma'}
        ]

    def test_basic_connectivity(self):
        """Test that all nodes are accessible"""
        print("🔍 Testing basic connectivity...")
        accessible_nodes = []
        
        for node in self.nodes:
            try:
                response = requests.get(f"{node['url']}/api/stats", timeout=5)
                if response.status_code == 200:
                    accessible_nodes.append(node)
                    stats = response.json()
                    print(f"✅ {node['name']}: {stats['peer_stats']['node_id']}")
                else:
                    print(f"❌ {node['name']}: HTTP {response.status_code}")
            except Exception as e:
                print(f"❌ {node['name']}: {e}")
        
        return accessible_nodes

    def test_peer_discovery(self):
        """Test that nodes can discover each other"""
        print("\n🔗 Testing peer discovery...")
        
        for node in self.nodes:
            try:
                response = requests.get(f"{node['url']}/api/nodes", timeout=5)
                data = response.json()
                total_nodes = data['total_nodes']
                print(f"📍 {node['name']} sees {total_nodes} total nodes")
                
                # Show peer details
                for peer in data['nodes']:
                    if not peer.get('is_self'):
                        print(f"   └─ Peer: {peer['id']} at {peer['ip']}")
                        
            except Exception as e:
                print(f"❌ {node['name']} discovery test failed: {e}")

    def test_distributed_upload(self):
        """Test file upload and distribution"""
        print("\n📤 Testing distributed upload...")
        
        # Test files with different types
        test_files = [
            ('sample_text.txt', 'This is a sample text file for distributed processing. ' * 5),
            ('research_paper.txt', 'Academic research paper about distributed systems and load balancing. ' * 10),
            ('data_analysis.txt', 'Data analysis report with statistical findings and conclusions. ' * 8)
        ]
        
        for filename, content in test_files:
            # Upload to random node
            upload_node = random.choice(self.nodes)
            print(f"\n📄 Uploading {filename} to {upload_node['name']}...")
            
            try:
                files = {'file': (filename, content, 'text/plain')}
                response = requests.post(f"{upload_node['url']}/api/upload", files=files)
                result = response.json()
                
                if result.get('success'):
                    task_id = result.get('task_id')
                    print(f"✅ Upload successful - Task ID: {task_id}")
                    
                    # Wait for processing
                    print("⏳ Waiting for distributed processing...")
                    time.sleep(5)
                    
                    # Check which node processed it
                    self.check_processing_result(filename)
                    
                else:
                    print(f"❌ Upload failed: {result.get('error')}")
                    
            except Exception as e:
                print(f"❌ Upload test failed: {e}")

    def check_processing_result(self, filename):
        """Check which nodes have the processed file"""
        print(f"🔍 Checking processing results for {filename}...")
        
        for node in self.nodes:
            try:
                # Search for the file content
                search_term = filename.split('.')[0].split('_')[0]  # Get first word
                response = requests.get(f"{node['url']}/api/search?q={search_term}")
                data = response.json()
                
                if data.get('results'):
                    print(f"   ✅ {node['name']} has the document")
                    for result in data['results']:
                        if filename in result['file_name']:
                            print(f"      └─ Processed by: {result.get('metadata', {}).get('node_id', 'Unknown')}")
                else:
                    print(f"   ❌ {node['name']} doesn't have the document")
                    
            except Exception as e:
                print(f"   ❌ {node['name']} search failed: {e}")

    def test_load_distribution(self):
        """Test if load is being distributed"""
        print("\n⚖️ Testing load distribution...")
        
        # Get initial stats
        initial_load = {}
        for node in self.nodes:
            try:
                response = requests.get(f"{node['url']}/api/stats")
                data = response.json()
                load = data['task_stats']['completed_tasks']
                initial_load[node['name']] = load
            except:
                initial_load[node['name']] = 0
        
        print("Initial task distribution:")
        for node_name, load in initial_load.items():
            print(f"   {node_name}: {load} tasks")

    def run_comprehensive_test(self):
        """Run all tests"""
        print("🚀 Distributed System Comprehensive Test")
        print("=" * 60)
        
        # Test 1: Basic connectivity
        accessible_nodes = self.test_basic_connectivity()
        if len(accessible_nodes) < 2:
            print("❌ Need at least 2 nodes for distributed testing")
            return
        
        # Test 2: Peer discovery
        self.test_peer_discovery()
        
        # Test 3: Load distribution
        self.test_load_distribution()
        
        # Test 4: Distributed upload
        self.test_distributed_upload()
        
        print("\n" + "=" * 60)
        print("🎉 DISTRIBUTED TESTING COMPLETE!")
        print("=" * 60)

if __name__ == "__main__":
    tester = DistributedTester()
    tester.run_comprehensive_test()