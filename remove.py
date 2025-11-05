import re
import os

files_to_fix = ['main.py', 'search_index.py', 'document_processor.py', 'task_manager.py', 'peer_node.py']

for filename in files_to_fix:
    if os.path.exists(filename):
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Remove emojis but keep the rest
        content = re.sub(r'[^\x00-\x7F]+', '', content)
        
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        
        print(f"Fixed {filename}")