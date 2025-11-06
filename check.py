# check_nodes_fixed.py
import requests
import json

def check_running_nodes():
    """Check what nodes are currently running with proper error handling"""
    try:
        response = requests.get('http://localhost:5000/api/debug/peers', timeout=5)
        data = response.json()
        
        print("🔍 CURRENTLY RUNNING NODES:")
        print("=" * 50)
        
        # Handle different response structures
        if isinstance(data, dict):
            peers = data
        else:
            print("❌ Unexpected response format")
            return
        
        if not peers:
            print("❌ No peers found - only the main node is running")
            return
        
        ocr_nodes = []
        text_nodes = []
        other_nodes = []
        
        for peer_id, peer_info in peers.items():
            # Handle case where peer_info might be boolean or other type
            if not isinstance(peer_info, dict):
                print(f"⚠️  Invalid peer data for {peer_id}: {peer_info}")
                continue
                
            node_type = peer_info.get('node_type', 'unknown')
            capabilities = peer_info.get('capabilities', {})
            ip = peer_info.get('ip', 'unknown')
            
            # Extract active capabilities safely
            active_caps = []
            if isinstance(capabilities, dict):
                active_caps = [k for k, v in capabilities.items() 
                              if v and k not in ['node_type', 'ip']]
            else:
                active_caps = ['unknown_capabilities']
            
            print(f"📡 {peer_id[:20]}...")
            print(f"   Type: {node_type}")
            print(f"   IP: {ip}")
            print(f"   Capabilities: {active_caps}")
            print()
            
            # Categorize nodes
            if node_type == 'ocr_specialist':
                ocr_nodes.append(peer_id)
            elif node_type == 'text_specialist':
                text_nodes.append(peer_id)
            else:
                other_nodes.append(peer_id)
        
        # Summary
        print("📊 NODE SUMMARY:")
        print(f"   📝 Text Specialists: {len(text_nodes)}")
        print(f"   🖼️  OCR Specialists: {len(ocr_nodes)}")
        print(f"   🔧 Other Nodes: {len(other_nodes)}")
        print(f"   📡 Total Peers: {len(peers)}")
        
        if ocr_nodes:
            print(f"\n✅ OCR SPECIALIST AVAILABLE!")
            print(f"   Node IDs: {[node[:15] + '...' for node in ocr_nodes]}")
        else:
            print(f"\n❌ NO OCR SPECIALIST FOUND!")
            print("\n🚀 START OCR NODE WITH:")
            print("   set FLASK_RUN_PORT=5001")
            print("   python main.py")
            
    except requests.exceptions.ConnectionError:
        print("❌ Cannot connect to main node on port 5000")
        print("   Make sure the main node is running first")
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"   Response data: {data if 'data' in locals() else 'No data'}")

def check_port_mapping():
    """Show which ports create which node types"""
    print("\n🎯 PORT TO NODE TYPE MAPPING:")
    print("=" * 40)
    
    node_types = ['text_specialist', 'ocr_specialist', 'nlp_specialist', 'general_worker']
    
    for port in [5000, 5001, 5002, 5003, 5004, 5005]:
        type_index = (port - 5000) % len(node_types)
        node_type = node_types[type_index]
        
        icon = "🖼️ " if node_type == 'ocr_specialist' else "📝 " if node_type == 'text_specialist' else "🧠 " if node_type == 'nlp_specialist' else "🔧 "
        print(f"{icon}Port {port} → {node_type}")

if __name__ == "__main__":
    check_running_nodes()
    check_port_mapping()