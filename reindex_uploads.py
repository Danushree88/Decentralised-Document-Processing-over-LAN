"""
Re-index all existing uploaded files that weren't processed
Run this script to add all your existing uploads to the search index
"""

import os
import sys
import uuid
from search_index import SearchIndex
from document_processor import DocumentProcessor

def reindex_all_uploads():
    """Re-process all files in the uploads folder"""
    
    print("="*60)
    print("🔄 RE-INDEXING EXISTING UPLOADS")
    print("="*60)
    
    # Initialize components
    print("\n📦 Initializing components...")
    search_index = SearchIndex('search_index')
    document_processor = DocumentProcessor()
    
    # Get existing documents to avoid duplicates
    existing_docs = search_index.get_all_documents()
    existing_files = set(doc['file_name'] for doc in existing_docs)
    print(f"✅ Currently indexed: {len(existing_docs)} documents")
    
    # Get all files in uploads folder
    uploads_folder = 'uploads'
    if not os.path.exists(uploads_folder):
        print(f"❌ Uploads folder not found: {uploads_folder}")
        return
    
    all_files = [f for f in os.listdir(uploads_folder) if os.path.isfile(os.path.join(uploads_folder, f))]
    print(f"📁 Total files in uploads: {len(all_files)}")
    
    # Find files that need indexing
    files_to_process = [f for f in all_files if f not in existing_files]
    print(f"🔍 Files needing indexing: {len(files_to_process)}")
    
    if not files_to_process:
        print("\n✅ All files are already indexed!")
        return
    
    print("\n" + "="*60)
    print("🚀 Starting re-indexing...")
    print("="*60 + "\n")
    
    # Process each file
    success_count = 0
    error_count = 0
    
    for i, filename in enumerate(files_to_process, 1):
        file_path = os.path.join(uploads_folder, filename)
        
        print(f"\n[{i}/{len(files_to_process)}] Processing: {filename}")
        
        try:
            # Process the document
            result = document_processor.process_document(file_path, 'full')
            
            if result['success']:
                # Add to search index
                file_id = str(uuid.uuid4())
                success = search_index.add_document(
                    file_id=file_id,
                    file_name=filename,
                    content=result['text'],
                    keywords=result['keywords'],
                    metadata=result['metadata'],
                    node_id='reindex_script'
                )
                
                if success:
                    print(f"   ✅ Indexed: {len(result['text'])} chars, {len(result['keywords'])} keywords")
                    success_count += 1
                else:
                    print(f"   ❌ Failed to add to index")
                    error_count += 1
            else:
                print(f"   ❌ Processing failed: {result.get('error', 'Unknown error')}")
                error_count += 1
                
        except Exception as e:
            print(f"   ❌ Error: {e}")
            error_count += 1
    
    # Final summary
    print("\n" + "="*60)
    print("📊 RE-INDEXING COMPLETE")
    print("="*60)
    print(f"✅ Successfully indexed: {success_count}")
    print(f"❌ Failed: {error_count}")
    print(f"📚 Total documents now: {len(search_index.get_all_documents())}")
    print("="*60)
    
    # Show stats
    stats = search_index.get_stats()
    print(f"\n📈 Index Statistics:")
    print(f"   Total documents: {stats['total_documents']}")
    print(f"   Total words: {stats['total_words']}")
    print(f"   Total size: {stats['total_size_mb']} MB")
    print(f"   Indexed terms: {stats['indexed_terms']}")
    print("\n✅ Re-indexing complete! Check the search page now.")

if __name__ == "__main__":
    try:
        reindex_all_uploads()
    except KeyboardInterrupt:
        print("\n\n⚠️  Re-indexing interrupted by user")
    except Exception as e:
        print(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()