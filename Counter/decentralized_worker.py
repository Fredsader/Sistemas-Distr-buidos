from flask import Flask, jsonify, request
import requests
import hashlib
import time
import threading
import logging
import os
from typing import Dict, Set, Deque, Optional
from collections import deque
import ctypes

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuração Windows
try:
    ctypes.windll.kernel32.SetHandleCount(8192)
    logger.info("Configuração de handles para Windows aplicada")
except AttributeError:
    pass

# Estado Global
class NetworkState:
    def __init__(self):
        self.file_registry: Dict[str, Dict] = {}
        self.chunk_queue: Deque[str] = deque()
        self.processed_chunks: Dict[str, int] = {}
        self.known_peers: Set[str] = set()
        self.my_url: str = ""
        self.lock = threading.Lock()

state = NetworkState()

def content_defined_chunking(data: bytes, max_size: int = 1024*1024) -> list:
    chunks = []
    start = 0
    while start < len(data):
        end = min(start + max_size, len(data))
        while end < len(data) and not (data[end] in (32, 10, 13)):  # Space, LF, CR
            end += 1
        if end == start:
            end = min(start + max_size, len(data))
        chunks.append(data[start:end])
        start = end
    return chunks


def local_file_discovery_worker(file_id: str):
    while True:
        with state.lock:
            file_info = state.file_registry.get(file_id)
            if not file_info or file_info['next_offset'] >= file_info['size']:
                break
            
            try:
                with open(file_info['path'], 'rb') as f:
                    f.seek(file_info['next_offset'])
                    data = f.read(1024*1024)  # Lê 1MB
                    chunks = content_defined_chunking(data)
                    
                    for chunk in chunks:
                        chunk_id = hashlib.sha256(chunk).hexdigest()
                        if chunk_id not in file_info['chunks']:
                            file_info['chunks'][chunk_id] = {
                                'offset': file_info['next_offset'],
                                'length': len(chunk),
                                'processed': False
                            }
                            state.chunk_queue.append(chunk_id)
                            file_info['next_offset'] += len(chunk)
            except Exception as e:
                logger.error(f"Erro no discovery: {str(e)}")
        
        time.sleep(1)


@app.route('/register_local_file', methods=['POST'])
def register_local_file():
    file_path = request.json['path']
    file_id = hashlib.sha256(file_path.encode()).hexdigest()
    
    with state.lock:
        if file_id not in state.file_registry:
            try:
                file_size = os.path.getsize(file_path)
                
                state.file_registry[file_id] = {
                    'path': file_path,
                    'size': file_size,
                    'chunks': {},
                    'next_offset': 0
                }
                logger.info(f"Arquivo local registrado: {file_path}")
                threading.Thread(target=local_file_discovery_worker, args=(file_id,)).start()
            except Exception as e:
                logger.error(f"Falha ao registrar arquivo: {str(e)}")
    
    return jsonify({'status': 'processing', 'file_id': file_id})

@app.route('/register_file', methods=['POST'])
def register_file():
    file_url = request.json['url']
    file_id = hashlib.sha256(file_url.encode()).hexdigest()
    
    with state.lock:
        if file_id not in state.file_registry:
            try:
                head = requests.head(file_url, timeout=5)
                file_size = int(head.headers.get('Content-Length', 0))
                
                state.file_registry[file_id] = {
                    'url': file_url,
                    'size': file_size,
                    'chunks': {},
                    'next_offset': 0
                }
                logger.info(f"Novo arquivo registrado: {file_id[:8]}")
                threading.Thread(target=chunk_discovery_worker, args=(file_id,)).start()
            except Exception as e:
                logger.error(f"Falha ao registrar arquivo: {str(e)}")
    
    return jsonify({'status': 'processing', 'file_id': file_id})

def chunk_discovery_worker(file_id: str):
    while True:
        with state.lock:
            file_info = state.file_registry.get(file_id)
            if not file_info or file_info['next_offset'] >= file_info['size']:
                break
            
            try:
                range_header = f"bytes={file_info['next_offset']}-{file_info['next_offset'] + 1024*1024}"
                response = requests.get(file_info['url'], headers={'Range': range_header}, timeout=10)
                chunks = content_defined_chunking(response.content)
                
                for chunk in chunks:
                    chunk_id = hashlib.sha256(chunk).hexdigest()
                    if chunk_id not in file_info['chunks']:
                        file_info['chunks'][chunk_id] = {
                            'offset': file_info['next_offset'],
                            'length': len(chunk),
                            'processed': False
                        }
                        state.chunk_queue.append(chunk_id)
                        file_info['next_offset'] += len(chunk)
            except Exception as e:
                logger.error(f"Erro no discovery: {str(e)}")
        
        time.sleep(1)

@app.route('/get_work', methods=['GET'])
def get_work():
    with state.lock:
        if state.chunk_queue:
            chunk_id = state.chunk_queue.popleft()
            file_id = next(iter(state.file_registry.keys()))
            chunk_info = state.file_registry[file_id]['chunks'][chunk_id]
            
            return jsonify({
                'chunk_id': chunk_id,
                'file_url': state.file_registry[file_id]['url'],
                'range': f"bytes={chunk_info['offset']}-{chunk_info['offset'] + chunk_info['length'] - 1}"
            })
        return jsonify({'status': 'no_work'})

@app.route('/submit_work', methods=['POST'])
def submit_work():
    data = request.json
    chunk_id = data['chunk_id']
    count = data['count']
    
    with state.lock:
        if chunk_id not in state.processed_chunks:
            state.processed_chunks[chunk_id] = count
            propagate_result(chunk_id, count)
            return jsonify({'status': 'accepted'})
        return jsonify({'status': 'duplicate'})

def propagate_result(chunk_id: str, count: int):
    for peer in state.known_peers:
        try:
            requests.post(
                f"{peer}/update_result",
                json={'chunk_id': chunk_id, 'count': count},
                timeout=1
            )
        except:
            continue

@app.route('/update_result', methods=['POST'])
def update_result():
    data = request.json
    with state.lock:
        if data['chunk_id'] not in state.processed_chunks:
            state.processed_chunks[data['chunk_id']] = data['count']
    return jsonify({'status': 'updated'})

@app.route('/total', methods=['GET'])
def get_total():
    with state.lock:
        return jsonify({'total': sum(state.processed_chunks.values())})

@app.route('/ping', methods=['GET'])
def ping():
    return jsonify({
        'status': 'online',
        'url': state.my_url,
        'files': len(state.file_registry),
        'peers': list(state.known_peers)
    })

def peer_discovery():
    while True:
        time.sleep(5)
        current_peers = set()
        
        with state.lock:
            peers_to_check = list(state.known_peers)
        
        for peer in peers_to_check:
            try:
                response = requests.get(f"{peer}/ping", timeout=2)
                if response.status_code == 200:
                    data = response.json()
                    current_peers.add(data['url'])
                    current_peers.update(data['peers'])
            except:
                continue
        
        with state.lock:
            state.known_peers = current_peers - {state.my_url}

def start_worker(port: int):
    state.my_url = f"http://localhost:{port}"
    
    # Registro inicial
    for initial_port in range(5000, 5010):
        if initial_port != port:
            try:
                requests.post(
                    f"http://localhost:{initial_port}/register_peer",
                    json={'url': state.my_url},
                    timeout=0.5
                )
            except:
                continue
    
    threading.Thread(target=peer_discovery, daemon=True).start()
    app.run(host='0.0.0.0', port=port)

@app.route('/register_peer', methods=['POST'])
def register_peer():
    new_peer = request.json.get('url')
    if new_peer and new_peer != state.my_url:
        with state.lock:
            state.known_peers.add(new_peer)
        return jsonify({'status': 'registered'})
    return jsonify({'status': 'invalid'})

if __name__ == '__main__':
    import sys
    start_worker(int(sys.argv[1]))