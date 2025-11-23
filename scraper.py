# meu_comparador_backend/scraper.py (v11.5 - FULL: Lista Completa + Specs Pichau)

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
import os 
import time
import traceback

# --- Imports do Selenium ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium_stealth import stealth
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

# --- FUNÇÃO DE LIMPEZA DE HTML ---
def corrigir_html_descricao(soup_element, url_base):
    if not soup_element: return ""
    
    # Remove tags inúteis
    for tag in soup_element.find_all(['script', 'iframe', 'link', 'button', 'input']):
        tag.decompose()

    # Corrige Imagens (Lazy Load e Links Relativos)
    for img in soup_element.find_all('img'):
        new_src = img.get('data-src') or img.get('data-srcset') or img.get('src')
        if new_src:
            new_src = new_src.split(' ')[0]
            if new_src.startswith('/'): new_src = url_base + new_src
            img['src'] = new_src
        
        # Limpa atributos que escondem a imagem
        for attr in ['loading', 'width', 'height', 'style', 'class']:
            if img.has_attr(attr): del img[attr]
        
        # Estilo padrão para responsividade
        img['style'] = "max-width: 100%; height: auto; display: block; margin: 10px auto;"

    # Corrige estilos de texto para ficarem legíveis (remove cores fixas escuras)
    for tag in soup_element.find_all(style=True):
        del tag['style'] # Remove estilos inline que podem conflitar com o tema dark/light

    return str(soup_element)

# --- LISTA DE ALVOS (SUA LISTA COMPLETA ORIGINAL) ---
LISTA_DE_PRODUTOS = [
    # --- Nvidia ---
    {
        "nome_base": "RTX 4070",
        "categoria": "Placa de Vídeo",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/461699/placa-de-video-rtx-4070-windforce-oc-gigabyte-geforce-12gb-gddr6x-dlss-ray-tracing-gv-n4070wf3oc-12gd",
            "Pichau": "https://pichau.com.br/placa-de-video-gigabyte-geforce-rtx-4070-super-windforce-oc-12gb-gddr6x-192-bit-gv-n407swf3oc-12gd",
            "Terabyte": "https://www.terabyteshop.com.br/produto/24479/placa-de-video-gigabyte-geforce-rtx-4070-eagle-oc-12gb-gddr6x-dlss-ray-tracing-gv-n4070eagle-oc-12gd"
        }
    },
    {
        "nome_base": "Palit GeForce RTX 5060 Ti Infinity",
        "categoria": "Placa de Vídeo",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/776931",
            "Pichau": "https://pichau.com.br/placa-de-video-palit-geforce-rtx-5060-ti-infinity-3-8gb-gddr7-128-bit-ne7506t019p1-gb2062s",
            "Terabyte": "https://www.terabyteshop.com.br/produto/36060/placa-de-video-palit-nvidia-geforce-rtx-5060-infinity-3-8gb-gddr7-dlss-ray-tracing-ne75060019p1-gb2063s"
        }
    },
    {
        "nome_base": "RTX 3060 12GB",
        "categoria": "Placa de Vídeo",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/153454",
            "Pichau": "https://pichau.com.br/placa-de-video-maxsun-geforce-rtx-3060-terminator-12gb-gddr6-192-bit-ms-geforce-rtx3060-tr-12g",
            "Terabyte": "https://www.terabyteshop.com.br/produto/21297/placa-de-video-msi-geforce-rtx-3060-ventus-2x-oc-lhr-12gb-gddr6-dlss-ray-tracing"
        }
    },
    # --- AMD ---
    {
        "nome_base": "RX 7600",
        "categoria": "Placa de Vídeo",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/475647",
            "Pichau": "https://pichau.com.br/placa-de-video-gigabyte-radeon-rx-7600-gaming-oc-8gb-gddr6-128-bit-gv-r76gaming-oc-8gd",
            "Terabyte": "https://www.terabyteshop.com.br/produto/25487/placa-de-video-gigabyte-amd-radeon-rx-7600-gaming-oc-8gb-gddr6-fsr-ray-tracing-gv-r76gaming-oc-8gd"
        }
    },
    {
        "nome_base": "RX 6600",
        "categoria": "Placa de Vídeo",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/235984/placa-de-video-rx-6600-cld-8g-asrock-amd-radeon-8gb-gddr6-90-ga2rzz-00uanf",
            "Pichau": "https://pichau.com.br/placa-de-video-asrock-radeon-rx-6600-challenger-d-8gb-gddr6-128-bit-90-ga2rzz-00uanf",
            "Terabyte": "https://www.terabyteshop.com.br/produto/19808/placa-de-video-asrock-radeon-rx-6600-challenger-d-8gb-gddr6-fsr-ray-tracing-90-ga2rzz-00uanf"
        }
    },
    # --- Processadores ---
    {
        "nome_base": "Ryzen 5 7600",
        "categoria": "Processador",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/420277/processador-amd-ryzen-5-7600-3-8ghz-5-1ghz-turbo-cache-32mb-hexa-core-12-threads-am5-wraith-stealth-100-100001015box",
            "Pichau": "https://www.pichau.com.br/processador-amd-ryzen-5-7600-6-core-12-threads-3-8ghz-5-1ghz-turbo-cache-38mb-am5-100-100001015box",
            "Terabyte": "https://www.terabyteshop.com.br/produto/23415/processador-amd-ryzen-5-7600-38ghz-51ghz-turbo-6-cores-12-threads-am5-com-cooler-amd-wraith-stealth-100-100001015box"
        }
    },
    {
        "nome_base": "Ryzen 7 7800X3D",
        "categoria": "Processador",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/426262/processador-amd-ryzen-7-7800x3d-5-0ghz-max-turbo-cache-104mb-am5-8-nucleos-video-integrado-100-100000910wof",
            "Pichau": "https://www.pichau.com.br/processador-amd-ryzen-7-7800x3d-8-core-16-threads-4-2ghz-5-0ghzturbo-cache-104mb-am5-100-100000910wof-br",
            "Terabyte": "https://www.terabyteshop.com.br/produto/24769/processador-amd-ryzen-7-7800x3d-42ghz-50ghz-turbo-8-cores-16-threads-am5-sem-cooler-100-100000910wof"
        }
    },
    # --- Outros ---
    {
        "nome_base": "Placa-Mãe B650M Gigabyte",
        "categoria": "Placa-Mãe",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/505794",
            "Pichau": "https://pichau.com.br/placa-mae-gigabyte-b650m-d3hp-ddr5-socket-am5-m-atx-chipset-amd-b650-b650m-d3hp",
            "Terabyte": "https://www.terabyteshop.com.br/produto/28919/placa-mae-gigabyte-b650m-d3hp-chipset-b650-amd-am5-matx-ddr5"
        }
    },
    {
        "nome_base": "Monitor Gamer LG UltraGear 24' 180Hz",
        "categoria": "Monitor",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/614879",
            "Pichau": "https://pichau.com.br/monitor-gamer-lg-24-pol-ips-fhd-1ms-180hz-freesync-g-sync-hdmi-dp-24gs60f-b-awzm",
            "Terabyte": "https://www.terabyteshop.com.br/produto/31035/monitor-gamer-lg-ultragear-24-pol-full-hd-180hz-ips-1ms-freesyncg-sync-hdmidp-24gs60f-bawzm"
        }
    },
    {
        "nome_base": "Fonte Cooler Master Mwe Gold 750 V3",
        "categoria": "Fonte de Alimentação",
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/923379",
            "Pichau": "https://pichau.com.br/fonte-cooler-master-mwe-gold-750-v3-750w-atx-3-1-80-plus-gold-preto-mpe-7506-acag-bbr",
        }
    },
]

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
}
s = requests.Session()
s.headers.update(HEADERS)

def limpar_preco(texto_preco):
    if not texto_preco: return None
    preco_limpo = re.sub(r'[^\d,]', '', texto_preco)
    preco_limpo = preco_limpo.replace(',', '.')
    try: return float(preco_limpo)
    except ValueError: return None

# --- FUNÇÕES DE BUSCA POR LOJA ---

def buscar_dados_kabum(url, soup):
    nome, preco, img, desc = None, None, None, None
    try:
        tag_nome = soup.find('h1')
        if tag_nome: nome = tag_nome.text.strip()
        
        tag_preco = soup.find('h4', class_="text-secondary-500") or soup.find('b', class_="text-secondary-500")
        if tag_preco: preco = limpar_preco(tag_preco.text)
        else: preco = 0.0

        tag_desc = soup.find('div', id='description')
        if tag_desc:
            desc = corrigir_html_descricao(tag_desc, "https://www.kabum.com.br")

        tag_img = soup.select_one('img[src*="/produtos/fotos/"][src$="_gg.jpg"]') or soup.select_one('img[src*="/produtos/fotos/sync_mirakl/"][src*="/xlarge/"]')
        if tag_img: img = tag_img.get('src')

    except Exception as e: print(f"  -> [Kabum] Erro: {e}")
    return nome, preco, img, desc

def buscar_dados_pichau(url, soup):
    nome, preco, img, desc = None, None, None, "" # Descrição começa vazia string
    try:
        # 1. Nome e Preço (Padrão)
        tag_nome = soup.find('h1')
        if tag_nome: nome = tag_nome.text.strip()
        
        tag_preco = soup.find(string=re.compile(r'R\$\s*[\d\.,]+.*vista'))
        if tag_preco and hasattr(tag_preco, 'parent'):
             preco = limpar_preco(tag_preco.parent.text)
        else:
             preco = 0.0

        # --- 2. DESCRIÇÃO "SOBRE" (Texto Rico) ---
        tag_sobre = soup.find('div', class_="description-rich-text-product")
        if tag_sobre:
            html_sobre = corrigir_html_descricao(tag_sobre, "https://www.pichau.com.br")
            desc += f'<div class="mb-8"> <h3 class="text-xl font-bold mb-4">Sobre o Produto</h3> {html_sobre} </div>'
            print("  -> [Pichau] Seção 'Sobre' encontrada.")

        # --- 3. DESCRIÇÃO "INFORMAÇÕES ADICIONAIS" (Especificações) ---
        # Procura pelo título "Informações Adicionais"
        header_specs = soup.find(lambda tag: tag.name in ['h2', 'h3', 'div'] and "INFORMAÇÕES ADICIONAIS" in tag.text.upper())
        
        if header_specs:
            # Pega o pai do header para tentar capturar a tabela
            parent_container = header_specs.find_parent('div')
            if parent_container:
                specs_html = corrigir_html_descricao(parent_container, "https://www.pichau.com.br")
                desc += f'<div class="mt-8 border-t pt-4"> {specs_html} </div>'
                print("  -> [Pichau] Seção 'Informações Adicionais' encontrada.")
        else:
            # Fallback: Tenta encontrar qualquer tabela grande
            possible_table = soup.find('div', class_=lambda x: x and ('attributes' in x or 'specifications' in x))
            if possible_table:
                 specs_html = corrigir_html_descricao(possible_table, "https://www.pichau.com.br")
                 desc += f'<div class="mt-8 border-t pt-4"><h3 class="text-xl font-bold mb-4">Especificações</h3> {specs_html} </div>'
                 print("  -> [Pichau] Tabela de especificações encontrada (fallback).")

        tag_img = soup.find('img', class_="iiz__img")
        if tag_img: img = tag_img.get('src')

    except Exception as e: print(f"  -> [Pichau] Erro: {e}")
    
    if not desc: desc = None
    return nome, preco, img, desc

def buscar_dados_terabyte(url, soup):
    nome, preco, img, desc = None, None, None, None
    try:
        tag_nome = soup.find('h1', class_="tit-prod") or soup.find('h1')
        if tag_nome: nome = tag_nome.text.strip()

        tag_preco = soup.find('p', id="valVista") or soup.find('div', class_="part-price")
        if tag_preco: preco = limpar_preco(tag_preco.text)
        else: preco = 0.0

        tag_desc = soup.find('div', class_='descricao')
        if tag_desc:
            for clear in tag_desc.find_all('div', class_='clear'): clear.decompose()
            desc = corrigir_html_descricao(tag_desc, "https://www.terabyteshop.com.br")

        tag_img = soup.select_one("img.zoomImg") or soup.select_one("#carousel-product-images img")
        if tag_img: img = tag_img.get('src')

    except Exception as e: print(f"  -> [Terabyte] Erro: {e}")
    return nome, preco, img, desc

# --- SELENIUM ---
def get_selenium_soup(url, loja):
    driver = None
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage") 
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
        
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        stealth(driver, languages=["pt-BR", "pt"], vendor="Google Inc.", platform="Win32", webgl_vendor="Intel Inc.", renderer="Intel Iris OpenGL Engine", fix_hairline=True)

        driver.get(url)
        
        # Espera a página carregar (H1 é crucial)
        WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "h1")))
        time.sleep(3) 
        
        return BeautifulSoup(driver.page_source, 'lxml')

    except Exception as e:
        print(f"  -> [Selenium] Erro em {loja}: {e}")
        return None
    finally:
        if driver: driver.quit()

# --- LOOP PRINCIPAL ---
def buscar_dados_loja(url, loja):
    print(f"  Acessando {loja}...")
    if loja == "Kabum":
        try:
            resp = s.get(url, timeout=15)
            if resp.status_code == 200: return buscar_dados_kabum(url, BeautifulSoup(resp.content, 'lxml'))
        except: pass
    elif loja in ["Pichau", "Terabyte"]:
        soup = get_selenium_soup(url, loja)
        if soup:
            if loja == "Pichau": return buscar_dados_pichau(url, soup)
            if loja == "Terabyte": return buscar_dados_terabyte(url, soup)
    return None, None, None, None

print(f"--- INICIANDO MONITOR DE PREÇOS (v11.5 - Pichau Full + Lista Completa) ---")
resultados = []
now = datetime.now()

for item in LISTA_DE_PRODUTOS:
    base = item["nome_base"].strip()
    print(f"\n>>> Produto: {base}")
    
    for loja, url in item["urls"].items():
        if not url: continue
        nome, preco, img, desc = buscar_dados_loja(url, loja)
        
        if nome and preco is not None:
            resultados.append({
                "timestamp": now, "produto_base": base, "categoria": item["categoria"],
                "nome_completo_raspado": nome, "preco": preco, "imagem_url": img or "",
                "loja": loja, "url": url, "descricao": desc or ""
            })
            print(f"  -> SUCESSO: {loja} salvo.")
        else:
            print(f"  -> FALHA: {loja} não coletado.")
            
    time.sleep(2)

if resultados:
    try:
        df = pd.DataFrame(resultados)
        # Garante ordem das colunas
        cols = ["timestamp", "produto_base", "categoria", "nome_completo_raspado", "preco", "imagem_url", "loja", "url", "descricao"]
        # Preenche colunas faltantes
        for col in cols:
            if col not in df.columns: df[col] = ""
        df = df[cols]
        
        db_url = os.environ.get('DATABASE_URL')
        if db_url and db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
            
        engine = create_engine(db_url)
        df.to_sql('precos', con=engine, if_exists='append', index=False)
        print(f"\n=== DB ATUALIZADO: {len(df)} registros ===")
    except Exception as e: print(f"Erro SQL: {e}")
else: print("\nNenhum dado coletado.")