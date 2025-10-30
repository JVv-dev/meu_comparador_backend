# meu_comparador_backend/scraper.py (v9.3 - Anti-Bot com WebDriverWait)

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
# --- NOVOS IMPORTS (Paciência Inteligente) ---
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, NoSuchElementException

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

# --- Configuração do Selenium (com CAMUFLAGEM) ---
print("Iniciando o navegador Selenium (headless)...")
chrome_options = Options()
chrome_options.add_argument("--headless") 
chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
chrome_options.add_argument("--disable-gpu")
chrome_options.add_argument("window-size=1920x1080")
chrome_options.add_argument("--no-sandbox") 
chrome_options.add_argument("--disable-dev-shm-usage")
# --- NOVAS OPÇÕES DE CAMUFLAGEM ---
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)


try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    # Camuflagem extra
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
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

# --- FUNÇÃO DE SCRAPING DA KABUM (OK) ---
def buscar_dados_kabum(url):
    print(f"  Acessando [Requests] Kabum: {url[:50]}...")
    nome_produto, preco_produto, imagem_url = None, None, None
    try:
        response = s.get(url, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        
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
        
    except requests.exceptions.RequestException as e:
        print(f"  -> Erro de conexão [requests] ao acessar Kabum: {e}")
    except Exception as e:
        print(f"  -> [Kabum] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url

# --- FUNÇÃO DE SCRAPING DA PICHAU (COM WebDriverWait) ---
def buscar_dados_pichau(driver, url):
    print(f"  Acessando [Selenium] Pichau: {url[:50]}...")
    nome_produto, preco_produto, imagem_url = None, None, None
    try:
        driver.get(url)
        # Espera Inteligente de até 20 segundos
        wait = WebDriverWait(driver, 20)
        
        # Espera o NOME aparecer
        tag_nome = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "h1.mui-1ri6pu6-product_info_title")))
        nome_produto = tag_nome.text.strip()

        # Tenta encontrar o PREÇO
        try:
            tag_preco = driver.find_element(By.CSS_SELECTOR, "div.mui-1jk88bq-price_vista-extraSpacePriceVista")
            preco_produto = limpar_preco(tag_preco.text)
            print(f"  -> [Pichau] Status: Disponível! Preço: R$ {preco_produto}")
        except NoSuchElementException:
            # Se o preço não existe, procura por "esgotado"
            try:
                driver.find_element(By.CSS_SELECTOR, "span.mui-1nlpwp-availability-outOfStock")
                print("  -> [Pichau] Status: Produto Esgotado")
                preco_produto = 0.0
            except NoSuchElementException:
                print("  -> [Pichau] ALERTA: Preço/Esgotado não encontrado.")
                preco_produto = None # Falha em pegar o preço

        # Tenta encontrar a IMAGEM
        try:
            tag_imagem = driver.find_element(By.CSS_SELECTOR, "img.iiz__img")
            imagem_url = tag_imagem.get_attribute('src')
            if imagem_url: print("  -> [Pichau] Imagem encontrada!")
        except NoSuchElementException:
            print("  -> [Pichau] Imagem principal não encontrada (class='iiz__img').")

    except TimeoutException:
        print(f"  -> [Pichau] ERRO: Timeout. A página (ou o seletor do nome) não carregou em 20s. Provavelmente bloqueado.")
        print(f"  -> [Pichau DEBUG] Título da Página: {driver.title}")
    except Exception as e:
        print(f"  -> [Pichau] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url

# --- FUNÇÃO DE SCRAPING DA TERABYTE (COM WebDriverWait) ---
def buscar_dados_terabyte(driver, url):
    print(f"  Acessando [Selenium] Terabyte: {url[:50]}...")
    nome_produto, preco_produto, imagem_url = None, None, None
    try:
        driver.get(url)
        # Espera Inteligente de até 20 segundos
        wait = WebDriverWait(driver, 20)
        
        # Espera o NOME aparecer
        tag_nome = wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, "h1.tit-prod")))
        nome_produto = tag_nome.text.strip()

        # Tenta encontrar o PREÇO
        try:
            tag_preco = driver.find_element(By.ID, "valVista")
            preco_produto = limpar_preco(tag_preco.text)
            print(f"  -> [Terabyte] Status: Disponível! Preço: R$ {preco_produto}")
        except NoSuchElementException:
            # Se o preço não existe, procura por "esgotado"
            try:
                driver.find_element(By.XPATH, "//h2[contains(text(), 'Produto Indisponível')]")
                print("  -> [Terabyte] Status: Produto Esgotado.")
                preco_produto = 0.0
            except NoSuchElementException:
                print("  -> [Terabyte] ALERTA: Preço/Esgotado não encontrado.")
                preco_produto = None # Falha em pegar o preço

        # Tenta encontrar a IMAGEM
        try:
            tag_imagem = driver.find_element(By.CSS_SELECTOR, "img.zoomImg")
            imagem_url = tag_imagem.get_attribute('src')
            if imagem_url: print("  -> [Terabyte] Imagem encontrada!")
        except NoSuchElementException:
            print("  -> [Terabyte] Imagem principal não encontrada (class='zoomImg').")

    except TimeoutException:
        print(f"  -> [Terabyte] ERRO: Timeout. A página (ou o seletor do nome) não carregou em 20s. Provavelmente bloqueado.")
        print(f"  -> [Terabyte DEBUG] Título da Página: {driver.title}")
    except Exception as e:
        print(f"  -> [Terabyte] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url

# --- REMOVIDA: Função get_selenium_soup() ---

# --- FUNÇÃO PRINCIPAL DE BUSCA (ATUALIZADA) ---
def buscar_dados_loja(driver, url, loja):
    # Kabum usa requests (rápido e não usa o 'driver')
    if loja == "Kabum":
        return buscar_dados_kabum(url)
    
    # Pichau e Terabyte usam Selenium (o 'driver' compartilhado)
    elif loja == "Pichau":
        return buscar_dados_pichau(driver, url)
    elif loja == "Terabyte":
        return buscar_dados_terabyte(driver, url)
    else:
        print(f"  -> Loja '{loja}' não suportada.")
        return None, None, None

# --- O PROGRAMA PRINCIPAL ---
print(f"--- INICIANDO MONITOR DE PREÇOS (v9.3 - Anti-Bot com WebDriverWait) ---")

resultados_de_hoje = []
timestamp_agora = datetime.now()

for produto_base_info in LISTA_DE_PRODUTOS:
    nome_base = produto_base_info["nome_base"]
    print(f"\nBuscando: {nome_base}...")
    for loja, url_loja in produto_base_info["urls"].items():
        print(f" Tentando loja: {loja}")
        
        try:
            # Passa o 'driver' para a função principal
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
                print(f"  -> Falha ao extrair dados de {nome_base} na loja {loja}.")
        except Exception as e:
            print(f"  -> ERRO INESPERADO no loop da loja {loja}: {e}")
            import traceback
            traceback.print_exc()
        
        print("  Pausando por 5 segundos...\n") # Pausa menor entre lojas
        time.sleep(5)

print("\nBusca concluída.")

# --- LÓGICA PARA SALVAR NO BANCO DE DADOS (Sem mudanças) ---
if resultados_de_hoje:
    try:
        df_hoje = pd.DataFrame(resultados_de_hoje)
        colunas_ordenadas = ["timestamp", "produto_base", "nome_completo_raspado", "preco", "imagem_url", "loja", "url"]
        df_hoje = df_hoje.reindex(columns=colunas_ordenadas)

        DATABASE_URL = os.environ.get('DATABASE_URL')
        
        if not DATABASE_URL:
            print("ERRO CRÍTICO: A variável de ambiente 'DATABASE_URL' não foi encontrada.")
        else:
            print("Conectando ao banco de dados...")
            if DATABASE_URL.startswith("postgres://"):
                DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
                
            engine = create_engine(DATABASE_URL)
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