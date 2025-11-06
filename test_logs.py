
import requests
import json
import time

def test_node(port, node_name):
    """Test a single node"""
    base_url = f"http://localhost:{port}"
    
    print(f"\n{'='*60}")
    print(f"Testing {node_name} Node on port {port}")
    print(f"{'='*60}")
    
    try:
        # Test discovery endpoint
        response = requests.get(f"{base_url}/api/debug/discovery")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Node Type: {data['node']['node_type']}")
            print(f"✅ Node IP: {data['node']['local_ip']}")
            print(f"✅ Peers Discovered: {data['peers_discovered']}")
            print(f"✅ Peers in Task Manager: {data['peers_in_task_manager']}")
            
            print(f"\nPeer Analysis:")
            for peer in data['peer_analysis']:
                print(f"  - {peer['node_type']} node at {peer['ip']}")
                print(f"    Can process PDF: {peer['can_process_pdf']}")
                print(f"    Can process TXT: {peer['can_process_txt']}")
                print(f"    In task manager: {peer['in_task_manager']}")
        else:
            print(f"❌ Discovery check failed: {response.status_code}")
    
    except Exception as e:
        print(f"❌ Error testing {node_name}: {e}")

def test_task_distribution(port):
    """Test task distribution logic"""
    base_url = f"http://localhost:{port}"
    
    print(f"\n{'='*60}")
    print(f"Testing Task Distribution on port {port}")
    print(f"{'='*60}")
    
    try:
        response = requests.get(f"{base_url}/api/debug/task-distribution")
        if response.status_code == 200:
            data = response.json()
            print(f"✅ PDF Specialists: {data['pdf_specialists']}")
            print(f"✅ TXT Specialists: {data['txt_specialists']}")
            
            if data['pdf_specialists'] > 0:
                print(f"\nPDF Peers:")
                for peer in data['pdf_peers']:
                    print(f"  - {peer['id'][:30]}... at {peer['ip']} (load: {peer['load']})")
            
            if data['txt_specialists'] > 0:
                print(f"\nTXT Peers:")
                for peer in data['txt_peers']:
                    print(f"  - {peer['id'][:30]}... at {peer['ip']} (load: {peer['load']})")
        else:
            print(f"❌ Distribution check failed: {response.status_code}")
    
    except Exception as e:
        print(f"❌ Error testing distribution: {e}")

if __name__ == "__main__":
    print("\n" + "="*60)
    print("PEER DISCOVERY TEST")
    print("="*60)
    print("Make sure both nodes are running:")
    print("  Terminal 1: python main.py --type pdf")
    print("  Terminal 2: python main.py --type txt")
    print("="*60)
    
    # Wait a bit for discovery
    print("\nWaiting 5 seconds for peer discovery...")
    time.sleep(5)
    
    # Test both nodes (assuming default ports 5000 and 5001)
    test_node(5000, "First")
    test_node(5001, "Second")
    
    # Test task distribution
    test_task_distribution(5000)
    test_task_distribution(5001)
    
    print(f"\n{'='*60}")
    print("TEST COMPLETE")
    print("="*60)