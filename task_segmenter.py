# task_segmenter.py
import os
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)

class TaskSegmenter:
    def __init__(self, config):
        self.config = config
    
    def analyze_document_batch(self, file_paths):
        """Analyze batch of documents and segment into specialized tasks"""
        tasks_by_type = defaultdict(list)
        
        for file_path in file_paths:
            if not os.path.exists(file_path):
                continue
                
            file_ext = os.path.splitext(file_path)[1].lower()
            file_size = os.path.getsize(file_path)
            
            # Get required tasks for this file type
            required_tasks = self.config.FILE_TYPE_TASKS.get(file_ext, ['file_processing'])
            
            # Create individual tasks
            for task_type in required_tasks:
                task_data = {
                    'file_path': file_path,
                    'file_name': os.path.basename(file_path),
                    'file_size': file_size,
                    'file_type': file_ext,
                    'task_type': task_type,
                    'priority': self._calculate_priority(file_size, task_type),
                    'estimated_time': self._estimate_processing_time(file_size, task_type)
                }
                tasks_by_type[task_type].append(task_data)
        
        logger.info(f"📊 Segmented {len(file_paths)} files into {sum(len(tasks) for tasks in tasks_by_type.values())} tasks")
        
        # Log task distribution
        for task_type, tasks in tasks_by_type.items():
            logger.info(f"   - {task_type}: {len(tasks)} tasks")
        
        return tasks_by_type
    
    def _calculate_priority(self, file_size, task_type):
        """Calculate task priority"""
        priority = 1  # Base priority
        
        # Large files get higher priority for distribution
        if file_size > 10 * 1024 * 1024:  # > 10MB
            priority += 2
        elif file_size > 5 * 1024 * 1024:  # > 5MB
            priority += 1
        
        # Complex tasks get higher priority
        if task_type in ['ocr', 'nlp', 'summarization']:
            priority += 2
        elif task_type in ['text_extraction', 'keyword_extraction']:
            priority += 1
        
        return min(priority, 5)  # Cap at 5
    
    def _estimate_processing_time(self, file_size, task_type):
        """Estimate processing time for load balancing"""
        base_times = {
            'ocr': file_size / (1024 * 1024) * 3,  # 3 sec per MB
            'text_extraction': file_size / (1024 * 1024) * 0.5,  # 0.5 sec per MB
            'keyword_extraction': file_size / (1024 * 1024) * 1,
            'nlp': file_size / (1024 * 1024) * 2,
            'summarization': file_size / (1024 * 1024) * 4,
            'entity_recognition': file_size / (1024 * 1024) * 3,
            'topic_modeling': file_size / (1024 * 1024) * 5,
            'file_processing': file_size / (1024 * 1024) * 0.2
        }
        return base_times.get(task_type, 1)