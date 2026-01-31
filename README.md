# 📌 Decentralized Document Processing System

A **distributed document processing platform** that intelligently routes uploaded documents to specialized nodes (PDF or TXT processors), extracts text and keywords, and indexes content for **fast and efficient search**—designed to assist researchers handling large volumes of documents.

---

## 🚀 Key Features

### 🔗 Decentralized Architecture
- No central server dependency  
- Multiple peer nodes collaborate over the network  

### 🧩 Specialized Processing Nodes
- **PDF Nodes** → PDF text extraction  
- **TXT Nodes** → Text extraction + keyword extraction  
- Tasks are routed **only to capable nodes**

### ⚖️ Intelligent Task Distribution
- Automatic peer discovery using **UDP broadcast**
- **Load-aware** task assignment
- Failover to **local processing** if no peers are available

### 📡 Real-time Monitoring Dashboard
- Live node discovery
- Job distribution and success rates
- System statistics via **WebSockets**

### 🔍 Search & Indexing
- Extracted text and keywords are indexed
- Enables fast document search for research use cases

---

## 🏗️ System Architecture

The system follows a decentralized, peer-to-peer architecture where each node can act as both a coordinator and a worker.

### Architecture Flow

+----------------+
| Client |
| (Web Browser) |
+--------+-------+
|
| Upload Document
v
+----------------------+
| Coordinator Node |
| (Flask Application) |
+----------+-----------+
|
| Task Assignment
v
+----------------------+
| Task Manager |
| (Load & Capability |
| Based Routing) |
+----------+-----------+
|
| Distributed Tasks
v
+-------------------------------+

Specialized Peer Nodes
PDF Node
(PDF)
+-------------------------------+
       |
       | Processing Results
       v
+----------------------+
| Search Index |
| (Text & Keywords) |
+----------+-----------+
|
| Search Queries
v
+----------------------+
| Client UI |
| (Search & Dashboard)|
+----------------------+

---

## ⚙️ Technologies Used

### 🖥️ Backend
- Python
- Flask
- Flask-SocketIO
- UDP Sockets (Peer Discovery & Task Distribution)
- Multithreading

### 🌐 Frontend
- HTML
- JavaScript
- Bootstrap
- Chart.js

### 📄 Processing & Storage
- PyPDF2 (PDF parsing)
- Custom keyword extraction
- In-memory search indexing

---

## 🔄 How It Works

1. User uploads documents (PDF / TXT)
2. System identifies required processing capability
3. Available peer nodes broadcast their capabilities
4. Task Manager selects the **least-loaded capable node**
5. Node processes the document and returns results
6. Extracted text and keywords are indexed
7. Users can search documents through the UI

---

## 📊 Dashboard Features

- View active nodes and their specializations
- Monitor job completion and load distribution
- Real-time updates using WebSockets
- Visual analytics using interactive charts

---

## 🎯 Use Case

Designed for **research paper writers and analysts** who need to:

- Process large numbers of documents
- Extract searchable text and keywords
- Efficiently search across multiple document sources

---
