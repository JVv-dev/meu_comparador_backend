# meu_comparador_backend/scraper.py (v11.17 - Fix Duplicação e Ordem)

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

# --- NOVO: EXTRATOR DE IMAGENS CSS ---
def extrair_imagens_css(soup_element):
    css_map = {}
    for style in soup_element.find_all('style'):
        if style.string:
            matches = re.findall(r'--([\w-]+)\s*:\s*url\([\'"]?([^\'"\)]+)[\'"]?\)', style.string)
            for var_name, url in matches:
                css_map[var_name] = url
    
    if css_map:
        for tag in soup_element.find_all(class_=True):
            classes = tag.get('class', [])
            for cls in classes:
                if cls in css_map:
                    if tag.name == 'img':
                        tag['src'] = css_map[cls]
                    else:
                        # Mantém background-image para divs
                        tag['style'] = f"background-image: url('{css_map[cls]}'); background-size: cover; background-position: center; min-height: 250px;"
                    break
    return soup_element

# --- TRADUTOR INTELIGENTE (MDL -> TAILWIND) ---
def traduzir_para_tailwind(soup_element, url_base):
    if not soup_element: return ""
    
    soup_element = extrair_imagens_css(soup_element)

    # 1. REMOVE DUPLICATAS MOBILE/DESKTOP (O Fix das "Duas Imagens")
    # Remove elementos exclusivos de mobile para não duplicar com o desktop
    for mobile_elem in soup_element.find_all(class_='mobile'):
        mobile_elem.decompose()
    
    # Remove classes 'desktop' que sobraram para não atrapalhar, mas mantém o elemento
    for desktop_elem in soup_element.find_all(class_='desktop'):
        desktop_elem['class'] = [c for c in desktop_elem.get('class', []) if c != 'desktop']

    # 2. LIMPEZA GERAL
    for tag in soup_element.find_all(['script', 'iframe', 'link', 'button', 'input', 'object', 'style']):
        tag.decompose()
    for video in soup_element.find_all('section', id='videos'):
        video.decompose()

    # 3. TABELAS
    for table in soup_element.find_all('table'):
        table['class'] = "w-full text-sm border-collapse my-6 overflow-hidden rounded-lg border border-border"
        table['style'] = ""
        for th in table.find_all('th'):
            th['class'] = "bg-muted/50 p-3 text-left font-medium border-b border-border text-foreground"
            th['style'] = ""
        for td in table.find_all('td'):
            td['class'] = "p-3 border-b border-border text-muted-foreground"
            td['style'] = ""

    # 4. GRID SYSTEM (12 Colunas + Reordenamento)
    for grid in soup_element.find_all(class_='mdl-grid'):
        # Grid flexível: Mobile (coluna única) -> Desktop (12 colunas)
        grid['class'] = "grid grid-cols-1 md:grid-cols-12 gap-6 my-8 items-center w-full"
        grid['style'] = ""
    
    for cell in soup_element.find_all(class_='mdl-cell'):
        classes = cell.get('class', [])
        
        # Classes base do Tailwind
        tailwind_classes = ["flex flex-col gap-4"] 
        
        # Traduz largura (col-span)
        col_span = "md:col-span-12" 
        if 'mdl-cell--6-col' in classes: col_span = "md:col-span-6"
        elif 'mdl-cell--4-col' in classes: col_span = "md:col-span-4"
        elif 'mdl-cell--3-col' in classes: col_span = "md:col-span-3"
        elif 'mdl-cell--8-col' in classes: col_span = "md:col-span-8"
        
        tailwind_classes.append(col_span)

        # Traduz ORDEM (Fix do "Texto em cima da imagem")
        # Se tiver 'order-1-phone', joga para o final no mobile (order-last)
        # No desktop (md:), volta ao normal (order-none)
        if 'mdl-cell--order-1-phone' in classes:
            tailwind_classes.append("order-last md:order-none")
        
        cell['class'] = " ".join(tailwind_classes)
        cell['style'] = ""

    # 5. IMAGENS
    for img in soup_element.find_all('img'):
        if not img.get('src'):
            new_src = img.get('data-src') or img.get('data-srcset')
            if new_src: img['src'] = new_src.split(' ')[0]

        src = img.get('src')
        if src and src.startswith('/'):
            img['src'] = url_base + src
        
        if not img.get('src') or 'data:image' in img.get('src', ''):
            # Se não tem src e não é background (tratado antes), remove
            if not img.has_attr('style') or 'url' not in img['style']: 
                img.decompose()
            continue

        img['class'] = "w-full h-auto rounded-lg shadow-sm mx-auto block object-contain max-h-[500px]"
        for attr in ['loading', 'width', 'height']:
            if img.has_attr(attr): del img[attr]
        
        if img.has_attr('style') and 'background-image' not in img['style']:
             del img['style']

    # 6. TIPOGRAFIA
    for tag in soup_element.find_all(['h2', 'h3', 'h4', 'h5']):
        tag['class'] = "text-2xl font-bold mt-8 mb-4 tracking-tight text-foreground"
        tag['style'] = ""
    for p in soup_element.find_all('p'):
        p['class'] = "leading-7 mb-4 text-muted-foreground text-lg"
        p['style'] = ""

    return soup_element.prettify()

# --- LISTA DE ALVOS ---
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
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
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

# --- FUNÇÃO KABUM ---
def buscar_dados_kabum(url, soup):
    nome, preco, img, desc = None, None, None, None
    try:
        tag_nome = soup.find('h1')
        if tag_nome: nome = tag_nome.text.strip()
        tag_preco = soup.find('h4', class_="text-secondary-500") or soup.find('b', class_="text-secondary-500")
        if tag_preco: preco = limpar_preco(tag_preco.text)
        else: preco = 0.0
        desc = None 
        tag_img = soup.select_one('img[src*="/produtos/fotos/"][src$="_gg.jpg"]') or soup.select_one('img[src*="/produtos/fotos/sync_mirakl/"][src*="/xlarge/"]')
        if tag_img: img = tag_img.get('src')
    except Exception as e: print(f"  -> [Kabum] Erro: {e}")
    return nome, preco, img, desc

# --- FUNÇÃO PICHAU ---
def buscar_dados_pichau(url, soup):
    nome, preco, img, desc = None, None, None, "" 
    try:
        tag_nome = soup.find('h1')
        if tag_nome: nome = tag_nome.text.strip()
        tag_preco = soup.find(string=re.compile(r'R\$\s*[\d\.,]+.*vista'))
        if tag_preco and hasattr(tag_preco, 'parent'):
             preco = limpar_preco(tag_preco.parent.text)
        else:
             tag_preco_class = soup.find('div', class_=lambda x: x and 'price_vista' in x)
             if tag_preco_class: preco = limpar_preco(tag_preco_class.text)
             else: preco = 0.0
        
        if preco > 0: print(f"  -> [Pichau] Preço: R$ {preco}")
        else: print("  -> [Pichau] Sem preço ou esgotado.")

        # --- CAPTURA INTELIGENTE COM NOVO TRADUTOR ---
        divs_desc = soup.find_all('div', class_="description-rich-text-product")
        unique_divs = []
        for div in divs_desc:
            is_nested = False
            for parent in divs_desc:
                if div != parent and parent in div.parents:
                    is_nested = True
                    break
            if not is_nested: unique_divs.append(div)

        html_sobre = ""
        html_specs = ""

        for div in unique_divs:
            if div.find('table'):
                print("  -> [Pichau] Specs encontradas.")
                html_specs += traduzir_para_tailwind(div, "https://www.pichau.com.br")
            else:
                print("  -> [Pichau] Visual encontrado.")
                html_sobre += traduzir_para_tailwind(div, "https://www.pichau.com.br")
        
        if html_sobre:
            desc += f'<div class="mb-8 pichau-visual">{html_sobre}</div>'
        if html_specs:
            desc += f'<div class="mt-8 border-t border-border pt-6 pichau-specs"><h3 class="text-2xl font-bold mb-4 text-foreground">Especificações Técnicas</h3>{html_specs}</div>'

        tag_img = soup.find('img', class_="iiz__img")
        if tag_img: img = tag_img.get('src')

    except Exception as e: print(f"  -> [Pichau] Erro: {e}")
    if not desc: desc = None
    return nome, preco, img, desc

# --- FUNÇÃO TERABYTE ---
def buscar_dados_terabyte(url, soup):
    nome, preco, img, desc = None, None, None, None
    try:
        tag_nome = soup.find('h1', class_="tit-prod") or soup.find('h1')
        if tag_nome: nome = tag_nome.text.strip()
        tag_preco = soup.find('p', id="valVista") or soup.find('div', class_="part-price")
        if tag_preco: 
            preco = limpar_preco(tag_preco.text)
            print(f"  -> [Terabyte] Preço: R$ {preco}")
        else: preco = 0.0

        tag_desc = soup.find('div', class_='descricao')
        if tag_desc:
            for clear in tag_desc.find_all('div', class_='clear'): clear.decompose()
            desc = traduzir_para_tailwind(tag_desc, "https://www.terabyteshop.com.br")
            print("  -> [Terabyte] Descrição OK")

        tag_img = soup.select_one("img.zoomImg") or soup.select_one("#carousel-product-images img")
        if tag_img: img = tag_img.get('src')

    except Exception as e: print(f"  -> [Terabyte] Erro: {e}")
    return nome, preco, img, desc

# --- SELENIUM ---
def get_selenium_soup(url, loja):
    driver = None
    try:
        print("  -> [Selenium] Iniciando...")
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage") 
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("window-size=1920x1080")
        chrome_options.add_argument(f"user-agent={HEADERS['User-Agent']}")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=chrome_options)
        stealth(driver, languages=["pt-BR", "pt"], vendor="Google Inc.", platform="Win32", webgl_vendor="Intel Inc.", renderer="Intel Iris OpenGL Engine", fix_hairline=True)
        
        driver.get(url)
        wait_for_popup = 3
        wait_for_content = 10 

        if loja == "Terabyte" or loja == "Pichau": 
            try:
                popup_close_button = WebDriverWait(driver, wait_for_popup).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "button.close, button.modal__close, button[aria-label='Close'], .close-modal"))
                )
                if popup_close_button:
                    driver.execute_script("arguments[0].click();", popup_close_button)
                    time.sleep(1)
            except: pass 

            if loja == "Terabyte":
                WebDriverWait(driver, wait_for_content).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#valVista, #avise-me-container, #btn-avise-me, .price-payment-slip, .part-price"))
                )
            elif loja == "Pichau":
                WebDriverWait(driver, wait_for_content).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='price_vista'], span[class*='availability-outOfStock'], h1"))
                )
        else: time.sleep(5)
            
        html = driver.page_source
        if "403 Forbidden" in html: return None
        return BeautifulSoup(html, 'lxml')
    except Exception as e:
        print(f"  -> [Selenium] Erro: {e}")
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

print(f"--- INICIANDO MONITOR (v11.17 - Duplication & Order Fix) ---")
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
        else: print(f"  -> FALHA: {loja} não coletado.")
    time.sleep(2)

if resultados:
    try:
        df = pd.DataFrame(resultados)
        cols = ["timestamp", "produto_base", "categoria", "nome_completo_raspado", "preco", "imagem_url", "loja", "url", "descricao"]
        for col in cols:
            if col not in df.columns: df[col] = ""
        df = df[cols]
        
        db_url = os.environ.get('DATABASE_URL')
        if db_url and db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
            
        engine = create_engine(db_url)
        df.to_sql('precos', con=engine, if_exists='append', index=False)
        print(f"\n=== SUCESSO: {len(df)} registros salvos no banco ===")
    except Exception as e: print(f"Erro SQL: {e}")
else: print("\nNenhum dado coletado.")