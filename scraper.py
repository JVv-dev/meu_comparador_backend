import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
import os
import time
from urllib.parse import urlparse

# --- Imports do Selenium ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# --- LISTA DE ALVOS (Completa) ---
LISTA_DE_PRODUTOS = [
    {
        "nome_base": "RX 6600",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/235984/placa-de-video-rx-6600-cld-8g-asrock-amd-radeon-8gb-gddr6-90-ga2rzz-00uanf",
            "Pichau": "https://pichau.com.br/placa-de-video-asrock-radeon-rx-6600-challenger-d-8gb-gddr6-128-bit-90-ga2rzz-00uanf",
            "Terabyte": "https://www.terabyteshop.com.br/produto/19808/placa-de-video-asrock-radeon-rx-6600-challenger-d-8gb-gddr6-fsr-ray-tracing-90-ga2rzz-00uanf"
        }
    },
    {
        "nome_base": "RTX 4070",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/461699/placa-de-video-rtx-4070-windforce-oc-gigabyte-geforce-12gb-gddr6x-dlss-ray-tracing-gv-n4070wf3oc-12gd",
            "Pichau": "https://pichau.com.br/placa-de-video-gigabyte-geforce-rtx-4070-super-windforce-oc-12gb-gddr6x-192-bit-gv-n407swf3oc-12gd",
            "Terabyte": "https://www.terabyteshop.com.br/produto/24479/placa-de-video-gigabyte-geforce-rtx-4070-eagle-oc-12gb-gddr6x-dlss-ray-tracing-gv-n4070eagle-oc-12gd"
        }
    },
]

#ARQUIVO_CSV = "precos.csv"
# Verifique se este é o caminho correto no seu PC
ARQUIVO_CSV = r"C:\Users\joaov\OneDrive\Documentos\meu_comparador\frontend\public\precos.csv"

# --- HEADERS (Para Kabum e Selenium) ---
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
}

# --- Configuração do requests.Session (para Kabum) ---
s = requests.Session()
s.headers.update(HEADERS)

# --- Configuração do Selenium (para Pichau e Terabyte) ---
print("Iniciando o navegador Selenium (headless)...")
chrome_options = Options()
chrome_options.add_argument("--headless") # Roda o Chrome sem abrir janela
chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("window-size=1920x1080")

try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    print("Navegador Selenium iniciado com sucesso.")
except Exception as e:
    print(f"ERRO: Falha ao iniciar o Selenium/WebDriver.")
    print(f"Verifique sua conexão ou instalação do Chrome. Erro: {e}")
    exit()

def limpar_preco(texto_preco):
    """Limpa o texto do preço e transforma em número float."""
    if not texto_preco: return None
    preco_limpo = re.sub(r'[^\d,]', '', texto_preco)
    preco_limpo = preco_limpo.replace(',', '.')
    try:
        return float(preco_limpo)
    except ValueError:
        return None

# --- FUNÇÃO DE SCRAPING DA KABUM (OK) ---
def buscar_dados_kabum(url, soup):
    nome_produto, preco_produto, imagem_url = None, None, None
    try:
        # Nome
        tag_nome = soup.find('h1', class_="text-black-800")
        if tag_nome: nome_produto = tag_nome.text.strip()
        else: print("  -> [Kabum] Tag <h1> do nome não encontrada.")

        # Preço / Esgotado
        tag_preco = soup.find('h4', class_="text-secondary-500")
        if tag_preco is None: tag_preco = soup.find('b', class_="text-secondary-500")

        if tag_preco:
            preco_produto = limpar_preco(tag_preco.text)
            print(f"  -> [Kabum] Status: Disponível! Preço: R$ {preco_produto}")
        else:
            tag_esgotado = soup.find('span', class_="text-secondary-400")
            if tag_esgotado and "esgotado" in tag_esgotado.text.lower():
                print("  -> [Kabum] Status: Produto Esgotado")
                preco_produto = 0.0
            else: print("  -> [Kabum] ALERTA: Preço/Esgotado não encontrado.")

        # Imagem (Padrão 1 e 2)
        tag_imagem = soup.select_one('img[src*="/produtos/fotos/"][src$="_gg.jpg"]')
        if tag_imagem is None:
            print("  -> [Kabum] Padrão 1 de imagem falhou. Tentando Padrão 2...")
            tag_imagem = soup.select_one('img[src*="/produtos/fotos/sync_mirakl/"][src*="/xlarge/"]')

        if tag_imagem:
            imagem_url = tag_imagem.get('src')
            if imagem_url: print("  -> [Kabum] Imagem encontrada!")
            else: print("  -> [Kabum] Tag <img> encontrada, mas sem 'src'.")
        else: print("  -> [Kabum] Imagem principal não encontrada (Padrões 1 e 2 falharam).")

    except Exception as e:
        print(f"  -> [Kabum] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url

# --- FUNÇÃO DE SCRAPING DA PICHAU (OK) ---
def buscar_dados_pichau(url, soup):
    nome_produto, preco_produto, imagem_url = None, None, None
    try:
        # Nome
        tag_nome = soup.find('h1', class_="mui-1ri6pu6-product_info_title")
        if tag_nome:
             nome_produto = tag_nome.text.strip()
        else:
            print("  -> [Pichau] Tag <h1> do nome não encontrada.")

        # Preço
        tag_preco = soup.find('div', class_="mui-1jk88bq-price_vista-extraSpacePriceVista")
        if tag_preco:
            preco_produto = limpar_preco(tag_preco.text)
            print(f"  -> [Pichau] Status: Disponível! Preço: R$ {preco_produto}")
        else:
            # Esgotado (lógica atualizada)
            tag_esgotado_span = soup.find('span', class_="mui-1nlpwp-availability-outOfStock")
            if tag_esgotado_span:
                print(f"  -> [Pichau] Status: Produto Esgotado (Encontrado: {tag_esgotado_span.text})")
                preco_produto = 0.0
            else:
                print("  -> [Pichau] ALERTA: Preço/Esgotado não encontrado.")

        # Imagem
        tag_imagem = soup.find('img', class_="iiz__img")
        if tag_imagem:
            imagem_url = tag_imagem.get('src')
            if imagem_url: print("  -> [Pichau] Imagem encontrada!")
            else: print("  -> [Pichau] Tag <img> encontrada, mas sem 'src'.")
        else: print("  -> [Pichau] Imagem principal não encontrada (class='iiz__img').")

    except Exception as e:
        print(f"  -> [Pichau] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url

# --- NOVO: FUNÇÃO DE SCRAPING DA TERABYTE (COMPLETA) ---
def buscar_dados_terabyte(url, soup):
    """Extrai nome, preço e imagem de uma página da Terabyte (já baixada)."""
    nome_produto, preco_produto, imagem_url = None, None, None
    try:
        # Nome (Selector que você encontrou)
        tag_nome = soup.find('h1', class_="tit-prod")
        if tag_nome:
            nome_produto = tag_nome.text.strip()
        else:
            print("  -> [Terabyte] Tag <h1> do nome não encontrada (class='tit-prod').")

        # Preço (Selector que você encontrou)
        tag_preco = soup.find('p', id="valVista")
        if tag_preco:
            preco_produto = limpar_preco(tag_preco.text)
            print(f"  -> [Terabyte] Status: Disponível! Preço: R$ {preco_produto}")
        else:
            # Esgotado (Selector que você encontrou)
            # Procura por um h2 que contenha "Produto Indisponível"
            tag_esgotado = soup.find(lambda tag: tag.name == 'h2' and 'Produto Indisponível' in tag.text)
            
            if tag_esgotado:
                print("  -> [Terabyte] Status: Produto Esgotado.")
                preco_produto = 0.0
            else:
                print("  -> [Terabyte] ALERTA: Preço/Esgotado não encontrado.")

        # Imagem (Selector que você encontrou)
        tag_imagem = soup.find('img', class_="zoomImg")
        if tag_imagem:
            imagem_url = tag_imagem.get('src')
            if imagem_url: print("  -> [Terabyte] Imagem encontrada!")
            else: print("  -> [Terabyte] Tag <img> encontrada, mas sem 'src'.")
        else: print("  -> [Terabyte] Imagem principal não encontrada (class='zoomImg').")

    except Exception as e:
        print(f"  -> [Terabyte] Exceção ao extrair dados: {e}")
    
    return nome_produto, preco_produto, imagem_url
# --- FIM DA FUNÇÃO TERABYTE ---

# --- Função para buscar HTML com Selenium ---
def get_selenium_soup(url):
    """Usa o driver Selenium para carregar a página, esperar o JS, e retornar o Soup."""
    try:
        driver.get(url)
        print("  -> [Selenium] Página carregada. Aguardando JavaScript (5s)...")
        time.sleep(5) 
        
        html = driver.page_source
        if "403 Forbidden" in html or "Access denied" in html:
             print("  -> [Selenium] ALERTA: Página bloqueada (detectou 403 no HTML).")
             return None

        soup = BeautifulSoup(html, 'lxml')
        return soup
    
    except Exception as e:
        print(f"  -> [Selenium] Erro ao carregar a página {url}: {e}")
        return None

# --- FUNÇÃO PRINCIPAL DE BUSCA (Híbrida v8.2 - FINAL) ---
def buscar_dados_loja(url, loja):
    """Baixa o HTML e chama a função de scraping apropriada para a loja."""
    print(f"  Acessando {loja}: {url[:50]}...")
    
    try:
        if loja == "Kabum":
            # Kabum: Usa requests (rápido)
            response = s.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            return buscar_dados_kabum(url, soup)
        
        # --- ALTERAÇÃO: Terabyte usa Selenium agora ---
        elif loja == "Pichau" or loja == "Terabyte":
            # Pichau e Terabyte: Usam Selenium (lento, mas potente)
            soup = get_selenium_soup(url)
            
            if not soup:
                print(f"  -> Falha ao obter HTML do Selenium para {loja}.")
                return None, None, None
            
            # Chama o parser correto para a loja
            if loja == "Pichau":
                return buscar_dados_pichau(url, soup)
            elif loja == "Terabyte":
                return buscar_dados_terabyte(url, soup) # Chama a função COMPLETA
        # --- FIM DA ALTERAÇÃO ---
            
        else:
            print(f"  -> Loja '{loja}' não suportada.")
            return None, None, None

    except requests.exceptions.HTTPError as err:
        # Erros do requests (SÓ PARA KABUM AGORA)
        print(f"  -> Erro HTTP [requests] ao acessar {loja}: {err}")
        return None, None, None
    except requests.exceptions.RequestException as e:
        # Erros do requests (SÓ PARA KABUM AGORA)
        print(f"  -> Erro de conexão [requests] ao acessar {loja}: {e}")
        return None, None, None
    except Exception as e:
        # Erros gerais (Selenium ou outros)
        print(f"  -> Exceção GERAL ao processar {loja} ({url[:50]}...): {e}")
        return None, None, None

# --- O PROGRAMA PRINCIPAL (v8.2) ---
print(f"--- INICIANDO MONITOR DE PREÇOS (v8.2 - Híbrido - 3 Lojas) ---")

resultados_de_hoje = []
timestamp_agora = datetime.now()

# Loop pelos PRODUTOS BASE
for produto_base_info in LISTA_DE_PRODUTOS:
    nome_base = produto_base_info["nome_base"]
    print(f"\nBuscando: {nome_base}...")

    # Loop pelas LOJAS desse produto
    for loja, url_loja in produto_base_info["urls"].items():
        print(f" Tentando loja: {loja}")

        nome_raspado, preco_raspado, imagem_raspada = buscar_dados_loja(url_loja, loja)

        if nome_raspado and preco_raspado is not None:
            resultados_de_hoje.append({
                "timestamp": timestamp_agora,
                "produto_base": nome_base, 
                "nome_completo_raspado": nome_raspado,
                "preco": preco_raspado,
                "imagem_url": imagem_raspada if imagem_raspada else "",
                "loja": loja, 
                "url": url_loja
            })
        else:
            print(f"  -> Falha ao salvar dados de {nome_base} na loja {loja}.")
        
        print("  Pausando por 10 segundos...\n")
        time.sleep(10)

print("\nBusca concluída.")

if resultados_de_hoje:
    df_hoje = pd.DataFrame(resultados_de_hoje)
    colunas_ordenadas = ["timestamp", "produto_base", "nome_completo_raspado", "preco", "imagem_url", "loja", "url"]
    df_hoje = df_hoje.reindex(columns=colunas_ordenadas)

    if os.path.exists(ARQUIVO_CSV):
        df_hoje.to_csv(ARQUIVO_CSV, mode='a', header=False, index=False, sep=';')
        print(f"Resultados adicionados ao '{ARQUIVO_CSV}'")
    else:
        df_hoje.to_csv(ARQUIVO_CSV, mode='w', header=True, index=False, sep=';')
        print(f"Novo arquivo '{ARQUIVO_CSV}' criado com os resultados.")
else: print("Nenhum dado foi coletado hoje.")


print("\nFechando o navegador Selenium...")
driver.quit() 

print("--- FIM DA EXECUÇÃO ---")