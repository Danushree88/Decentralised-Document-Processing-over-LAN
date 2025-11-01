// Global application JavaScript
document.addEventListener('DOMContentLoaded', function() {
    console.log("Document Processor UI loaded");
    
    // Initialize tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });
    
    // Initialize Socket.IO connection
    const socket = io();
    
    // Socket.IO event handlers
    socket.on('connect', function() {
        console.log('Connected to document processor server');
        showNotification('Connected to document processor', 'success');
    });
    
    socket.on('disconnect', function() {
        console.log('Disconnected from server');
        showNotification('Disconnected from server', 'warning');
    });
    
    socket.on('status_update', function(data) {
        console.log('Status update:', data);
        if (data.message) {
            showNotification(data.message, 'info');
        }
    });
    
    socket.on('stats_update', function(data) {
        console.log('Stats update received:', data);
        updateDashboardStats(data);
    });
    
    socket.on('task_update', function(data) {
        console.log('Task update:', data);
        showNotification(`Task ${data.task_id} ${data.status}`, 'info');
    });
    
    socket.on('node_discovered', function(data) {
        console.log('Node discovered:', data);
        showNotification(`New node discovered: ${data.node_id}`, 'success');
    });
    
    socket.on('node_lost', function(data) {
        console.log('Node lost:', data);
        showNotification(`Node lost: ${data.node_id}`, 'warning');
    });
    
    // Auto-refresh functionality
    let autoRefresh = true;
    
    function toggleAutoRefresh() {
        autoRefresh = !autoRefresh;
        const btn = document.getElementById('autoRefreshBtn');
        if (btn) {
            btn.textContent = autoRefresh ? 'Pause Auto-Refresh' : 'Start Auto-Refresh';
            btn.className = autoRefresh ? 'btn btn-success btn-sm' : 'btn btn-secondary btn-sm';
        }
    }
    
    // Add auto-refresh button to appropriate pages
    if (document.getElementById('nodesContainer') || document.getElementById('searchResults')) {
        const refreshBtn = document.createElement('button');
        refreshBtn.id = 'autoRefreshBtn';
        refreshBtn.className = 'btn btn-success btn-sm';
        refreshBtn.textContent = 'Pause Auto-Refresh';
        refreshBtn.onclick = toggleAutoRefresh;
        
        const header = document.querySelector('.card-header h4, .card-header h5');
        if (header) {
            const headerParent = header.parentElement;
            headerParent.classList.add('d-flex', 'justify-content-between', 'align-items-center');
            const newHeader = headerParent.cloneNode(true);
            newHeader.appendChild(refreshBtn);
            headerParent.parentNode.replaceChild(newHeader, headerParent);
        }
    }
    
    // Dashboard stats update function
    function updateDashboardStats(data) {
        // Update dashboard stats if elements exist
        if (document.getElementById('doc-count')) {
            document.getElementById('doc-count').textContent = data.index_stats.total_documents || 0;
        }
        if (document.getElementById('node-count')) {
            document.getElementById('node-count').textContent = (data.peer_stats.total_peers || 0) + 1;
        }
        if (document.getElementById('task-count')) {
            document.getElementById('task-count').textContent = data.task_stats.pending_tasks || 0;
        }
        if (document.getElementById('data-size')) {
            document.getElementById('data-size').textContent = data.index_stats.total_size_mb || '0';
        }
        
        // Update system status
        if (document.getElementById('leader-status')) {
            const isLeader = data.peer_stats.is_leader;
            document.getElementById('leader-status').textContent = isLeader ? 'This Node (Leader)' : 'Another Node';
            document.getElementById('leader-status').className = isLeader ? 'badge bg-success' : 'badge bg-secondary';
        }
        
        if (document.getElementById('network-status')) {
            const totalNodes = (data.peer_stats.total_peers || 0) + 1;
            document.getElementById('network-status').textContent = `Connected (${totalNodes} nodes)`;
            document.getElementById('network-status').className = 'badge bg-success';
        }
        
        // Update chart if it exists
        const statsChart = window.statsChart;
        if (statsChart && data.task_stats) {
            statsChart.data.datasets[0].data = [
                data.task_stats.completed_tasks || 0,
                data.task_stats.pending_tasks || 0,
                data.task_stats.failed_tasks || 0
            ];
            statsChart.update('none'); // Silent update
        }
    }
    
    // Notification system
    function showNotification(message, type = 'info') {
        // Create toast container if it doesn't exist
        let toastContainer = document.getElementById('toastContainer');
        if (!toastContainer) {
            toastContainer = document.createElement('div');
            toastContainer.id = 'toastContainer';
            toastContainer.className = 'toast-container position-fixed top-0 end-0 p-3';
            toastContainer.style.zIndex = '9999';
            document.body.appendChild(toastContainer);
        }
        
        const toastId = 'toast-' + Date.now();
        const typeClass = {
            'success': 'bg-success',
            'error': 'bg-danger',
            'warning': 'bg-warning',
            'info': 'bg-info'
        }[type] || 'bg-info';
        
        const toastHTML = `
            <div id="${toastId}" class="toast align-items-center text-white ${typeClass} border-0" role="alert">
                <div class="d-flex">
                    <div class="toast-body">
                        ${message}
                    </div>
                    <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
                </div>
            </div>
        `;
        
        toastContainer.insertAdjacentHTML('beforeend', toastHTML);
        const toastElement = document.getElementById(toastId);
        const toast = new bootstrap.Toast(toastElement);
        toast.show();
        
        // Remove toast after it's hidden
        toastElement.addEventListener('hidden.bs.toast', function() {
            toastElement.remove();
        });
    }
    
    // File upload drag and drop
    const fileInput = document.getElementById('fileInput');
    const uploadForm = document.getElementById('uploadForm');
    
    if (uploadForm && fileInput) {
        uploadForm.addEventListener('dragover', function(e) {
            e.preventDefault();
            uploadForm.classList.add('bg-light');
        });
        
        uploadForm.addEventListener('dragleave', function(e) {
            e.preventDefault();
            uploadForm.classList.remove('bg-light');
        });
        
        uploadForm.addEventListener('drop', function(e) {
            e.preventDefault();
            uploadForm.classList.remove('bg-light');
            if (e.dataTransfer.files.length > 0) {
                fileInput.files = e.dataTransfer.files;
                // Update file input display
                const fileList = document.getElementById('fileList') || createFileList();
                fileList.innerHTML = '';
                Array.from(e.dataTransfer.files).forEach(file => {
                    const fileItem = document.createElement('div');
                    fileItem.className = 'alert alert-info py-2 mt-2';
                    fileItem.innerHTML = `<i class="fas fa-file"></i> ${file.name} (${(file.size / 1024).toFixed(1)} KB)`;
                    fileList.appendChild(fileItem);
                });
            }
        });
        
        function createFileList() {
            const fileList = document.createElement('div');
            fileList.id = 'fileList';
            fileList.className = 'mt-3';
            uploadForm.appendChild(fileList);
            return fileList;
        }
    }
    
    // Initialize charts on dashboard
    const statsChartCanvas = document.getElementById('statsChart');
    if (statsChartCanvas) {
        const ctx = statsChartCanvas.getContext('2d');
        window.statsChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: ['Completed', 'Pending', 'Failed'],
                datasets: [{
                    data: [0, 0, 0],
                    backgroundColor: ['#28a745', '#ffc107', '#dc3545']
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: {
                        position: 'bottom'
                    }
                }
            }
        });
    }
    
    console.log("UI initialization complete");
});