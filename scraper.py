# meu_comparador_backend/scraper.py (v9.1 - Com melhorias de erro)

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
import os
import time
import traceback # NOVO

# --- Imports do Selenium ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

# --- Imports para o Banco de Dados ---
from sqlalchemy import create_engine

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

# --- HEADERS (Para Kabum e Selenium) ---
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
}

# --- Configuração do requests.Session (para Kabum) ---
s = requests.Session()
s.headers.update(HEADERS)

def limpar_preco(texto_preco):
    if not texto_preco: return None
    preco_limpo = re.sub(r'[^\d,]', '', texto_preco)
    preco_limpo = preco_limpo.replace(',', '.')
    try:
        return float(preco_limpo)
    except ValueError:
        return None

# --- FUNÇÕES DE SCRAPING DAS LOJAS (sem alterações) ---
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
        else: print("  -> [Kabum] Imagem principal não encontrada.")
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
                print(f"  -> [Pichau] Status: Produto Esgotado")
                preco_produto = 0.0
            else:
                print("  -> [Pichau] ALERTA: Preço/Esgotado não encontrado.")
        tag_imagem = soup.find('img', class_="iiz__img")
        if tag_imagem:
            imagem_url = tag_imagem.get('src')
            if imagem_url: print("  -> [Pichau] Imagem encontrada!")
        else: print("  -> [Pichau] Imagem principal não encontrada.")
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
            print("  -> [Terabyte] Tag <h1> do nome não encontrada.")
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
        else: print("  -> [Terabyte] Imagem principal não encontrada.")
    except Exception as e:
        print(f"  -> [Terabyte] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url

# --- Função de setup do Selenium ---
def setup_driver():
    print("Iniciando o navegador Selenium (headless)...")
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("window-size=1920x1080")
    chrome_options.add_argument("--no-sandbox") 
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        print("Navegador Selenium iniciado com sucesso.")
        return driver
    except Exception as e:
        print(f"ERRO: Falha ao iniciar o Selenium/WebDriver.")
        print(f"Verifique sua conexão ou instalação do Chrome. Erro: {e}")
        traceback.print_exc()
        return None # Retorna None se falhar

def get_selenium_soup(driver, url):
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

def buscar_dados_loja(driver, url, loja):
    print(f"  Acessando {loja}: {url[:50]}...")
    try:
        if loja == "Kabum":
            response = s.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            return buscar_dados_kabum(url, soup)
        
        elif loja == "Pichau" or loja == "Terabyte":
            if driver is None: # Se o driver não iniciou, pula
                print(f"  -> [Selenium] Driver não está disponível, pulando {loja}.")
                return None, None, None
                
            soup = get_selenium_soup(driver, url)
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

    # --- ERRO HANDLING REFINADO ---
    # Erros de Request (Kabum)
    except requests.exceptions.HTTPError as err:
        print(f"  -> Erro HTTP [requests] ao acessar {loja}: {err}")
    except requests.exceptions.RequestException as e:
        print(f"  -> Erro de conexão [requests] ao acessar {loja}: {e}")
    # Erros Gerais (incluindo Selenium)
    except Exception as e:
        print(f"  -> Exceção GERAL ao processar {loja} ({url[:50]}...):")
        traceback.print_exc() # Imprime o stack trace
        
    # Se qualquer erro ocorrer, retorna None para todos
    return None, None, None


# --- O PROGRAMA PRINCIPAL ---
def main():
    print(f"--- INICIANDO MONITOR DE PREÇOS (v9.1 - PostgreSQL) ---")
    
    driver = setup_driver() # Inicia o Selenium
    
    # Se o Selenium falhar ao iniciar, o driver será None
    # O script continuará, mas pulará Pichau e Terabyte (veja em buscar_dados_loja)

    resultados_de_hoje = []
    timestamp_agora = datetime.now()
    erros_na_raspagem = False # Flag para saber se algo deu errado

    for produto_base_info in LISTA_DE_PRODUTOS:
        nome_base = produto_base_info["nome_base"]
        print(f"\nBuscando: {nome_base}...")
        for loja, url_loja in produto_base_info["urls"].items():
            print(f" Tentando loja: {loja}")
            
            nome_raspado, preco_raspado, imagem_raspada = buscar_dados_loja(driver, url_loja, loja)
            
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
                print(f"  -> FALHA ao salvar dados de {nome_base} na loja {loja}.")
                # Se não for a Kabum, é um erro de Selenium
                if loja in ["Pichau", "Terabyte"]:
                    erros_na_raspagem = True # Marca que tivemos um problema
            
            print("  Pausando por 10 segundos...\n")
            time.sleep(10)

    print("\nBusca concluída.")

    # --- Salvar no Banco de Dados ---
    if resultados_de_hoje:
        try:
            df_hoje = pd.DataFrame(resultados_de_hoje)
            colunas_ordenadas = ["timestamp", "produto_base", "nome_completo_raspado", "preco", "imagem_url", "loja", "url"]
            df_hoje = df_hoje.reindex(columns=colunas_ordenadas)

            DATABASE_URL = os.environ.get('DATABASE_URL')
            
            if not DATABASE_URL:
                print("ERRO CRÍTICO: 'DATABASE_URL' não foi encontrada.")
            else:
                print("Conectando ao banco de dados...")
                if DATABASE_URL.startswith("postgres://"):
                    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
                    
                engine = create_engine(DATABASE_URL)
                df_hoje.to_sql('precos', con=engine, if_exists='append', index=False)
                print(f"Sucesso! {len(df_hoje)} registros salvos no banco de dados 'precos'.")

        except Exception as e:
            print(f"ERRO ao salvar dados no banco de dados: {e}")
            traceback.print_exc()
            erros_na_raspagem = True # Se falhar o save, é um erro
    else:
        print("Nenhum dado foi coletado hoje.")
        erros_na_raspagem = True # Se não coletou nada, algo está errado

    if driver:
        print("\nFechando o navegador Selenium...")
        driver.quit() 
    
    # --- NOVO: Falha a Action se algum erro ocorreu ---
    if erros_na_raspagem:
        print("\n--- EXECUÇÃO CONCLUÍDA COM ERROS ---")
        # Isso fará a GitHub Action ficar vermelha (falhar)
        # exit(1) # Comentado por enquanto para permitir salvar dados parciais
    else:
        print("\n--- EXECUÇÃO CONCLUÍDA COM SUCESSO ---")

if __name__ == '__main__':
    main()