# meu_comparador_backend/scraper.py (v10.0 - Salvando no PostgreSQL)

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

# --- Imports para o Banco de Dados ---
from sqlalchemy import create_engine
from dotenv import load_dotenv # Para ler o arquivo .env

# --- Carrega as variáveis do arquivo .env (DATABASE_URL) ---
load_dotenv()

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

# --- REMOVIDO: ARQUIVO_CSV não é mais necessário ---

# --- HEADERS (Para Kabum e Selenium) ---
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
}

s = requests.Session()
s.headers.update(HEADERS)

print("Iniciando o navegador Selenium (headless)...")
chrome_options = Options()
chrome_options.add_argument("--headless") 
chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("window-size=1920x1080")
# Para rodar localmente, não precisamos do --no-sandbox

try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    print("Navegador Selenium iniciado com sucesso.")
except Exception as e:
    print(f"ERRO: Falha ao iniciar o Selenium/WebDriver.")
    print(f"Verifique sua conexão ou instalação do Chrome. Erro: {e}")
    exit()

def limpar_preco(texto_preco):
    if not texto_preco: return None
    preco_limpo = re.sub(r'[^\d,]', '', texto_preco)
    preco_limpo = preco_limpo.replace(',', '.')
    try:
        return float(preco_limpo)
    except ValueError:
        return None

def buscar_dados_kabum(url, soup):
    nome_produto, preco_produto, imagem_url = None, None, None
    try:
        tag_nome = soup.find('h1', class_="text-black-800")
        if tag_nome: nome_produto = tag_nome.text.strip()
        else: print("  -> [Kabum] Tag <h1> do nome não encontrada.")
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
        tag_imagem = soup.select_one('img[src*="/produtos/fotos/"][src$="_gg.jpg"]')
        if tag_imagem is None:
            tag_imagem = soup.select_one('img[src*="/produtos/fotos/sync_mirakl/"][src*="/xlarge/"]')
        if tag_imagem:
            imagem_url = tag_imagem.get('src')
            if imagem_url: print("  -> [Kabum] Imagem encontrada!")
    except Exception as e:
        print(f"  -> [Kabum] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url

def buscar_dados_pichau(url, soup):
    nome_produto, preco_produto, imagem_url = None, None, None
    try:
        tag_nome = soup.find('h1', class_="mui-1ri6pu6-product_info_title")
        if tag_nome:
             nome_produto = tag_nome.text.strip()
        else:
            print("  -> [Pichau] Tag <h1> do nome não encontrada.")
        tag_preco = soup.find('div', class_="mui-1jk88bq-price_vista-extraSpacePriceVista")
        if tag_preco:
            preco_produto = limpar_preco(tag_preco.text)
            print(f"  -> [Pichau] Status: Disponível! Preço: R$ {preco_produto}")
        else:
            tag_esgotado_span = soup.find('span', class_="mui-1nlpwp-availability-outOfStock")
            if tag_esgotado_span:
                print(f"  -> [Pichau] Status: Produto Esgotado (Encontrado: {tag_esgotado_span.text})")
                preco_produto = 0.0
            else:
                print("  -> [Pichau] ALERTA: Preço/Esgotado não encontrado.")
        tag_imagem = soup.find('img', class_="iiz__img")
        if tag_imagem:
            imagem_url = tag_imagem.get('src')
            if imagem_url: print("  -> [Pichau] Imagem encontrada!")
    except Exception as e:
        print(f"  -> [Pichau] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url

def buscar_dados_terabyte(url, soup):
    nome_produto, preco_produto, imagem_url = None, None, None
    try:
        tag_nome = soup.find('h1', class_="tit-prod")
        if tag_nome:
            nome_produto = tag_nome.text.strip()
        else:
            print("  -> [Terabyte] Tag <h1> do nome não encontrada (class='tit-prod').")
        tag_preco = soup.find('p', id="valVista")
        if tag_preco:
            preco_produto = limpar_preco(tag_preco.text)
            print(f"  -> [Terabyte] Status: Disponível! Preço: R$ {preco_produto}")
        else:
            tag_esgotado = soup.find(lambda tag: tag.name == 'h2' and 'Produto Indisponível' in tag.text)
            if tag_esgotado:
                print("  -> [Terabyte] Status: Produto Esgotado.")
                preco_produto = 0.0
            else:
                print("  -> [Terabyte] ALERTA: Preço/Esgotado não encontrado.")
        tag_imagem = soup.find('img', class_="zoomImg")
        if tag_imagem:
            imagem_url = tag_imagem.get('src')
            if imagem_url: print("  -> [Terabyte] Imagem encontrada!")
    except Exception as e:
        print(f"  -> [Terabyte] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url

def get_selenium_soup(url):
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

def buscar_dados_loja(url, loja):
    print(f"  Acessando {loja}: {url[:50]}...")
    try:
        if loja == "Kabum":
            response = s.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            return buscar_dados_kabum(url, soup)
        
        elif loja == "Pichau" or loja == "Terabyte":
            soup = get_selenium_soup(url)
            if not soup:
                print(f"  -> Falha ao obter HTML do Selenium para {loja}.")
                return None, None, None
            if loja == "Pichau":
                return buscar_dados_pichau(url, soup)
            elif loja == "Terabyte":
                return buscar_dados_terabyte(url, soup)
        else:
            print(f"  -> Loja '{loja}' não suportada.")
            return None, None, None
    except requests.exceptions.HTTPError as err:
        print(f"  -> Erro HTTP [requests] ao acessar {loja}: {err}")
    except requests.exceptions.RequestException as e:
        print(f"  -> Erro de conexão [requests] ao acessar {loja}: {e}")
    except Exception as e:
        print(f"  -> Exceção GERAL ao processar {loja} ({url[:50]}...): {e}")
    return None, None, None

# --- O PROGRAMA PRINCIPAL (v10.0 - Salvando no DB) ---
print(f"--- INICIANDO MONITOR DE PREÇOS (v10.0 - Salvando no DB) ---")

resultados_de_hoje = []
timestamp_agora = datetime.now()

for produto_base_info in LISTA_DE_PRODUTOS:
    nome_base = produto_base_info["nome_base"]
    print(f"\nBuscando: {nome_base}...")
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

# --- NOVO: LÓGICA PARA SALVAR NO BANCO DE DADOS ---
if resultados_de_hoje:
    try:
        df_hoje = pd.DataFrame(resultados_de_hoje)
        colunas_ordenadas = ["timestamp", "produto_base", "nome_completo_raspado", "preco", "imagem_url", "loja", "url"]
        df_hoje = df_hoje.reindex(columns=colunas_ordenadas)

        # 1. Obter a URL de conexão do Ambiente (do arquivo .env)
        DATABASE_URL = os.environ.get('DATABASE_URL')
        
        if not DATABASE_URL:
            print("ERRO CRÍTICO: A variável de ambiente 'DATABASE_URL' não foi encontrada.")
            print("Verifique se você criou o arquivo .env e colocou a URL lá.")
        else:
            print("Conectando ao banco de dados...")
            if DATABASE_URL.startswith("postgres://"):
                DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
                
            engine = create_engine(DATABASE_URL)
            
            # 2. Envia o DataFrame para a tabela 'precos'
            #    if_exists='append' -> Adiciona as novas linhas, mantendo as antigas
            df_hoje.to_sql('precos', con=engine, if_exists='append', index=False)
            
            print(f"Sucesso! {len(df_hoje)} registros foram salvos no banco de dados na tabela 'precos'.")

    except Exception as e:
        print(f"ERRO ao salvar dados no banco de dados: {e}")
        import traceback
        traceback.print_exc()

else:
    print("Nenhum dado foi coletado hoje.")


print("\nFechando o navegador Selenium...")
driver.quit() 

print("--- FIM DA EXECUÇÃO ---")