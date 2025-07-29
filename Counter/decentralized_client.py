import requests
import sys
import logging
import hashlib
import time
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def discover_peers():
    """Descobre peers ativos na rede local"""
    peers = set()
    for port in range(5000, 5010):  # Varre portas 5000-5009
        try:
            response = requests.get(
                f"http://localhost:{port}/ping",
                timeout=1
            )
            if response.status_code == 200:
                peers.add(response.json()['url'])
        except requests.exceptions.RequestException:
            continue
    return list(peers)

def main(file_input: str):
    # Passo 1: Descobrir peers disponíveis
    peers = discover_peers()
    if not peers:
        logger.error("Nenhum peer disponível na rede!")
        return
    
    logger.info(f"Encontrados {len(peers)} peers ativos")

    # Passo 2: Registrar o arquivo na rede
    try:
        if os.path.exists(file_input):
            # Caso 1: Arquivo local
            abs_path = os.path.abspath(file_input)
            logger.info(f"Registrando arquivo local: {abs_path}")
            response = requests.post(
                f"{peers[0]}/register_local_file",
                json={'path': abs_path},
                timeout=5
            )
        else:
            # Caso 2: URL remota
            logger.info(f"Registrando URL remota: {file_input}")
            response = requests.post(
                f"{peers[0]}/register_file",
                json={'url': file_input},
                timeout=5
            )
        
        if response.status_code != 200:
            logger.error("Falha ao registrar arquivo na rede")
            return

    except Exception as e:
        logger.error(f"Erro na comunicação com o peer: {str(e)}")
        return

    # Passo 3: Monitorar progresso
    logger.info("Processamento iniciado. Aguardando resultados...")
    last_total = 0
    start_time = time.time()
    
    try:
        while True:
            # Pega o total atual de qualquer peer disponível
            current_peers = discover_peers()
            if not current_peers:
                logger.error("Todos os peers desconectaram!")
                break
                
            try:
                response = requests.get(
                    f"{current_peers[0]}/total",
                    timeout=2
                )
                data = response.json()
                current_total = data.get('total', 0)
                
                # Log de progresso
                if current_total != last_total:
                    elapsed = time.time() - start_time
                    logger.info(
                        f"Progresso: {current_total} palavras "
                        f"| Tempo: {elapsed:.1f}s"
                    )
                    last_total = current_total
                
                # Verificação final
                if time.time() - start_time > 3600:  # Timeout de 1 hora
                    logger.error("Tempo limite excedido")
                    break
                    
            except requests.exceptions.RequestException:
                logger.warning("Peer temporariamente indisponível")
            
            time.sleep(5)  # Verifica a cada 5 segundos

    except KeyboardInterrupt:
        logger.info("\nProcesso interrompido pelo usuário")
    
    # Passo 4: Resultado final
    try:
        response = requests.get(f"{current_peers[0]}/total", timeout=2)
        final_total = response.json().get('total', 0)
        logger.info("\n=== RESULTADO FINAL ===")
        logger.info(f"Total de palavras: {final_total}")
        logger.info(f"Tempo total: {time.time() - start_time:.2f} segundos")
    except:
        logger.error("Não foi possível obter o resultado final")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Uso: python decentralized_client.py <arquivo_local_ou_url>")
        sys.exit(1)
    
    main(sys.argv[1])