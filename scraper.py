# meu_comparador_backend/scraper.py (v10.25 - Correção Imagens Pichau)

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
from datetime import datetime
import os 
import time
from urllib.parse import urlparse
import traceback

# --- Imports do Selenium ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
# --- NOVOS IMPORTS (STEALTH) ---
from selenium_stealth import stealth
# --- FIM DOS NOVOS IMPORTS ---
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


# --- Imports para o Banco de Dados ---
from sqlalchemy import create_engine
from dotenv import load_dotenv # Para ler o arquivo .env

# --- Carrega as variáveis do arquivo .env (DATABASE_URL) ---
load_dotenv()

# --- LISTA DE ALVOS (COM .strip()) ---
LISTA_DE_PRODUTOS = [
    
    # --- NOVOS PRODUTOS (GPUs) ---
    # --- Nvidia ---
    # Não Afiliado
    {
        "nome_base": "RTX 4070".strip(),
        "categoria": "Placa de Vídeo".strip(),  
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/461699/placa-de-video-rtx-4070-windforce-oc-gigabyte-geforce-12gb-gddr6x-dlss-ray-tracing-gv-n4070wf3oc-12gd",
            "Pichau": "https://pichau.com.br/placa-de-video-gigabyte-geforce-rtx-4070-super-windforce-oc-12gb-gddr6x-192-bit-gv-n407swf3oc-12gd",
            "Terabyte": "https://www.terabyteshop.com.br/produto/24479/placa-de-video-gigabyte-geforce-rtx-4070-eagle-oc-12gb-gddr6x-dlss-ray-tracing-gv-n4070eagle-oc-12gd"
        }
    },
    # Não Afiliado
    {
        "nome_base": " Palit GeForce RTX 5060 Ti Infinity".strip(),
        "categoria": "Placa de Vídeo".strip(), 
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/776931",
            "Pichau": "https://pichau.com.br/placa-de-video-palit-geforce-rtx-5060-ti-infinity-3-8gb-gddr7-128-bit-ne7506t019p1-gb2062s",
            "Terabyte": "https://www.terabyteshop.com.br/produto/36060/placa-de-video-palit-nvidia-geforce-rtx-5060-infinity-3-8gb-gddr7-dlss-ray-tracing-ne75060019p1-gb2063s"
        }
    },
    # Não Afiliado
    {
        "nome_base": " RTX 3060 12GB ".strip(),
        "categoria": "Placa de Vídeo".strip(), 
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/153454",
            "Pichau": "https://pichau.com.br/placa-de-video-maxsun-geforce-rtx-3060-terminator-12gb-gddr6-192-bit-ms-geforce-rtx3060-tr-12g",
            "Terabyte": "https://www.terabyteshop.com.br/produto/21297/placa-de-video-msi-geforce-rtx-3060-ventus-2x-oc-lhr-12gb-gddr6-dlss-ray-tracing"
        }
    },

    # --- A M D ---
    # Não Afiliado
    {
        "nome_base": "RX 7600".strip(),
        "categoria": "Placa de Vídeo".strip(), 
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/475647",
            "Pichau": "https://pichau.com.br/placa-de-video-gigabyte-radeon-rx-7600-gaming-oc-8gb-gddr6-128-bit-gv-r76gaming-oc-8gd",
            "Terabyte": "https://www.terabyteshop.com.br/produto/25487/placa-de-video-gigabyte-amd-radeon-rx-7600-gaming-oc-8gb-gddr6-fsr-ray-tracing-gv-r76gaming-oc-8gd"
        }
    },
    # Não Afiliado
    {
        "nome_base": "RX 6600".strip(),
        "categoria": "Placa de Vídeo".strip(), 
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/235984/placa-de-video-rx-6600-cld-8g-asrock-amd-radeon-8gb-gddr6-90-ga2rzz-00uanf",
            "Pichau": "https://pichau.com.br/placa-de-video-asrock-radeon-rx-6600-challenger-d-8gb-gddr6-128-bit-90-ga2rzz-00uanf",
            "Terabyte": "https://www.terabyteshop.com.br/produto/19808/placa-de-video-asrock-radeon-rx-6600-challenger-d-8gb-gddr6-fsr-ray-tracing-90-ga2rzz-00uanf"
        }
    },

    # --- NOVOS PRODUTOS (Processadores) ---
    # --- A M D ---
    # Não afiliado
    {
        "nome_base": "Ryzen 5 7600".strip(),
        "categoria": "Processador".strip(),
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/420277/processador-amd-ryzen-5-7600-3-8ghz-5-1ghz-turbo-cache-32mb-hexa-core-12-threads-am5-wraith-stealth-100-100001015box",
            "Pichau": "https://www.pichau.com.br/processador-amd-ryzen-5-7600-6-core-12-threads-3-8ghz-5-1ghz-turbo-cache-38mb-am5-100-100001015box",
            "Terabyte": "https://www.terabyteshop.com.br/produto/23415/processador-amd-ryzen-5-7600-38ghz-51ghz-turbo-6-cores-12-threads-am5-com-cooler-amd-wraith-stealth-100-100001015box"
        }
    },
    # Não afiliado
    {
        "nome_base": "Ryzen 7 7800X3D".strip(),
        "categoria": "Processador".strip(),
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/426262/processador-amd-ryzen-7-7800x3d-5-0ghz-max-turbo-cache-104mb-am5-8-nucleos-video-integrado-100-100000910wof",
            "Pichau": "https://www.pichau.com.br/processador-amd-ryzen-7-7800x3d-8-core-16-threads-4-2ghz-5-0ghzturbo-cache-104mb-am5-100-100000910wof-br",
            "Terabyte": "https://www.terabyteshop.com.br/produto/24769/processador-amd-ryzen-7-7800x3d-42ghz-50ghz-turbo-8-cores-16-threads-am5-sem-cooler-100-100000910wof"
        }
    },
    # --- Intel ---


    # --- NOVOS PRODUTOS (Placas-mãe) ---
    # Não afiliado
    {
        "nome_base": "Placa-Mãe B650M Gigabyte".strip(),
        "categoria": "Placa-Mãe".strip(),
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/505794",
            "Pichau": "https://pichau.com.br/placa-mae-gigabyte-b650m-d3hp-ddr5-socket-am5-m-atx-chipset-amd-b650-b650m-d3hp",
            "Terabyte": "https://www.terabyteshop.com.br/produto/28919/placa-mae-gigabyte-b650m-d3hp-chipset-b650-amd-am5-matx-ddr5"
        }
    },

    # --- NOVOS PRODUTOS (Monitores) ---
    # Não afiliado
    {
        "nome_base": "Monitor Gamer LG UltraGear 24' 180Hz".strip(),
        "categoria": "Monitor".strip(),
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/614879",
            "Pichau": "https://pichau.com.br/monitor-gamer-lg-24-pol-ips-fhd-1ms-180hz-freesync-g-sync-hdmi-dp-24gs60f-b-awzm",
            "Terabyte": "https://www.terabyteshop.com.br/produto/31035/monitor-gamer-lg-ultragear-24-pol-full-hd-180hz-ips-1ms-freesyncg-sync-hdmidp-24gs60f-bawzm"
        }
    },

    # --- NOVOS PRODUTOS (Fontes / PSU) ---
    # Não afiliado
    {
        "nome_base": "Fonte Cooler Master Mwe Gold 750 V3 Atx 3.1 80 Plus Gold".strip(),
        "categoria": "Fonte de Alimentação".strip(),
        "urls": {
            "Kabum": "https://www.kabum.com.br/produto/923379",
            "Pichau": "https://pichau.com.br/fonte-cooler-master-mwe-gold-750-v3-750w-atx-3-1-80-plus-gold-preto-mpe-7506-acag-bbr",
            #"Terabyte": " " nao possui o item
        }
    },
]

# --- HEADERS ---
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
}
s = requests.Session()
s.headers.update(HEADERS)

# --- DRIVER GLOBAL REMOVIDO ---

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
    descricao_html = None # <-- NOVO
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
            
        # --- LÓGICA DA DESCRIÇÃO (v10.22 - Mantida, pois funcionou) ---
        tag_descricao = soup.find('div', id='description')
        if tag_descricao:
            descricao_html = str(tag_descricao) # Salva o HTML bruto
            print("  -> [Kabum] Descrição encontrada!")
        else:
            print("  -> [Kabum] ALERTA: Descrição não encontrada.")
        # --- FIM DA LÓGICA ---

        tag_imagem = soup.select_one('img[src*="/produtos/fotos/"][src$="_gg.jpg"]')
        if tag_imagem is None:
            tag_imagem = soup.select_one('img[src*="/produtos/fotos/sync_mirakl/"][src*="/xlarge/"]')
        if tag_imagem:
            imagem_url = tag_imagem.get('src')
            if imagem_url: print("  -> [Kabum] Imagem encontrada!")
    except Exception as e:
        print(f"  -> [Kabum] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url, descricao_html # <-- MUDANÇA

def buscar_dados_pichau(url, soup):
    nome_produto, preco_produto, imagem_url = None, None, None
    descricao_html = None # <-- NOVO
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
                
        # --- LÓGICA DA DESCRIÇÃO (v10.25 - CORREÇÃO PICHAU) ---
        # Usa o seletor de classe que você encontrou:
        tag_descricao = soup.find('div', class_="description-rich-text-product")
        
        if tag_descricao:
            # MUDANÇA: SÓ remover o <link>, mas MANTER o <style>
            for link_tag in tag_descricao.find_all('link'):
                link_tag.decompose()
                
            descricao_html = str(tag_descricao) # Salva o HTML bruto
            print("  -> [Pichau] Descrição encontrada (com styles)!")
        else:
            print("  -> [Pichau] ALERTA: Descrição não encontrada.")
        # --- FIM DA LÓGICA ---
                
        tag_imagem = soup.find('img', class_="iiz__img")
        if tag_imagem:
            imagem_url = tag_imagem.get('src')
            if imagem_url: print("  -> [Pichau] Imagem encontrada!")
    except Exception as e:
        print(f"  -> [Pichau] Exceção ao extrair dados: {e}")
    return nome_produto, preco_produto, imagem_url, descricao_html # <-- MUDANÇA

def buscar_dados_terabyte(url, soup):
    nome_produto, preco_produto, imagem_url = None, None, None
    descricao_html = None # <-- NOVO
    try:
        # --- 1. TENTATIVA DE NOME ---
        tag_nome = soup.find('h1', class_="tit-prod") 
        if not tag_nome:
            tag_nome = soup.find('h1') 
            if tag_nome:
                print("  -> [Terabyte] AVISO: Seletor 'tit-prod' falhou. Usando tag <h1> genérica.")
            else:
                print("  -> [Terabyte] ALERTA: Tag <h1> do nome não encontrada.")
        if tag_nome:
            nome_produto = tag_nome.text.strip()
            
        # --- 2. TENTATIVA DE ESGOTADO ---
        tag_esgotado = soup.find(lambda tag: tag.name == 'h2' and 'Produto Indisponível' in tag.text)
        if not tag_esgotado:
            tag_avise_me = soup.find('div', id="avise-me-container")
            if tag_avise_me and (not tag_avise_me.get('style') or 'display: none' not in tag_avise_me.get('style')):
                 print("  -> [Terabyte] AVISO: Usando 'avise-me-container' para Esgotado.")
                 tag_esgotado = tag_avise_me
        if not tag_esgotado:
             tag_avise_me_btn = soup.find('button', id="btn-avise-me")
             if tag_avise_me_btn:
                 print("  -> [Terabyte] AVISO: Usando 'btn-avise-me' para Esgotado.")
                 tag_esgotado = tag_avise_me_btn
        if not tag_esgotado:
             tag_notify_me = soup.find('div', class_='notify-me-wrapper')
             if tag_notify_me:
                 print("  -> [Terabyte] AVISO: Usando 'notify-me-wrapper' para Esgotado.")
                 tag_esgotado = tag_notify_me

        if tag_esgotado:
            print("  -> [Terabyte] Status: Produto Esgotado.")
            preco_produto = 0.0
        else:
            # --- 3. TENTATIVA DE PREÇO ---
            tag_preco = soup.find('p', id="valVista")
            if not tag_preco:
                print("  -> [Terabyte] AVISO: Seletor 'valVista' não encontrado. Tentando fallback 'span.price'...")
                bloco_part_price = soup.find('div', class_="part-price")
                if bloco_part_price and 'à vista' in bloco_part_price.text.lower():
                    tag_preco = bloco_part_price.find('span', class_='price')
                    if tag_preco:
                        print("  -> [Terabyte] Sucesso! Encontrado preço 'à vista' no fallback 1.")
            if not tag_preco:
                print("  -> [Terabyte] AVISO: Fallback 1 falhou. Tentando fallback 'product-price'...")
                tag_preco = soup.find('span', class_='product-price')
                if tag_preco:
                     print("  -> [Terabyte] Sucesso! Encontrado preço no fallback 2.")
            if not tag_preco:
                print("  -> [Terabyte] AVISO: Fallback 2 falhou. Tentando fallback 'price-payment-slip'...")
                tag_preco = soup.find('span', class_='price-payment-slip')
                if tag_preco:
                     print("  -> [Terabyte] Sucesso! Encontrado preço no fallback 3 (monitores).")

            if tag_preco:
                preco_produto = limpar_preco(tag_preco.text)
                if preco_produto == 0.0:
                    print(f"  -> [Terabyte] Status: Preço R$ 0,00 encontrado, tratando como Esgotado.")
                    preco_produto = 0.0
                else:
                    print(f"  -> [Terabyte] Status: Disponível! Preço: R$ {preco_produto}")
            else:
                print("  -> [Terabyte] ALERTA: Preço/Esgotado não encontrado. (Página pode ter mudado)")

        # --- LÓGICA DA DESCRIÇÃO (v10.24 - CORREÇÃO TERABYTE) ---
        # Usa o seletor de classe que você encontrou:
        tag_descricao = soup.find('div', class_='descricao')
        
        if tag_descricao:
            # Limpa os divs "clear" que não precisamos
            for clear_tag in tag_descricao.find_all('div', class_='clear'):
                clear_tag.decompose()

            descricao_html = str(tag_descricao) # Salva o HTML bruto
            print("  -> [Terabyte] Descrição encontrada!")
        else:
            print("  -> [Terabyte] ALERTA: Descrição não encontrada.")
        # --- FIM DA LÓGICA ---

        # --- 4. TENTATIVA DE IMAGEM ---
        tag_imagem = soup.find('img', class_="zoomImg") 
        if not tag_imagem:
            tag_imagem = soup.select_one("div.easyzoom img") 
        if not tag_imagem:
             tag_imagem = soup.select_one("img#pImg") 
        if not tag_imagem:
            img_container = soup.find('div', class_='product-image')
            if img_container:
                tag_imagem = img_container.find('img')
                if tag_imagem:
                     print("  -> [Terabyte] AVISO: Usando 'product-image' de fallback para imagem.")
        if not tag_imagem:
            img_container = soup.find('figure', class_='mz-figure')
            if img_container:
                tag_imagem = img_container.find('img')
                if tag_imagem:
                     print("  -> [Terabyte] AVISO: Usando 'mz-figure' de fallback para imagem.")
        
        if tag_imagem:
            imagem_url = tag_imagem.get('src')
            if imagem_url: print("  -> [Terabyte] Imagem encontrada!")
        else:
             print("  -> [Terabyte] ALERTA: Imagem não encontrada.")

    except Exception as e:
        print(f"  -> [Terabyte] Exceção ao extrair dados: {e}")
        traceback.print_exc() 
        
    return nome_produto, preco_produto, imagem_url, descricao_html # <-- MUDANÇA
# --- FIM DA FUNÇÃO ---

# --- INÍCIO DA FUNÇÃO (v10.21 - Otimizado) ---
def get_selenium_soup(url, loja):
    
    driver = None # Inicia o driver como Nulo
    
    try:
        # --- 1. CRIAR O NAVEGADOR ---
        print("  -> [Selenium] Iniciando nova sessão de navegador (Stealth)...")
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
        
        print("  -> [Selenium] Aplicando patches 'stealth'...")
        stealth(driver,
              languages=["pt-BR", "pt"],
              vendor="Google Inc.",
              platform="Win32",
              webgl_vendor="Intel Inc.",
              renderer="Intel Iris OpenGL Engine",
              fix_hairline=True,
              )
        print("  -> [Selenium] Patches 'stealth' aplicados.")
        # --- FIM DA CRIAÇÃO ---


        # --- 2. CARREGAR A PÁGINA E APLICAR LÓGICA ---
        driver.get(url)
        print(f"  -> [Selenium] Página carregada para {loja}. Iniciando lógica...")
        
        # --- SUA OTIMIZAÇÃO (3s / 10s) ---
        wait_for_popup = 3
        wait_for_content = 10 
        # --- FIM DA OTIMIZAÇÃO ---


        if loja == "Terabyte" or loja == "Pichau": # Aplicar a lógica para ambas
            # --- ESTÁGIO 1: MATADOR DE POPUP (Sua observação) ---
            try:
                print(f"  -> [Selenium] Estágio 1/2: Tentando fechar popup (espera máx {wait_for_popup}s)...")
                popup_close_button = WebDriverWait(driver, wait_for_popup).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, 
                        "button.close, button.modal__close, button[aria-label='Close'], button[aria-label='Fechar'], .close-modal, .btn-close, .fancybox-close, .modal-close"
                    ))
                )
                if popup_close_button:
                    print("  -> [Selenium] Popup encontrado! Clicando para fechar.")
                    driver.execute_script("arguments[0].click();", popup_close_button) # Click via JS (mais confiável)
                    time.sleep(2) # Espera 2s para o popup fechar
            except Exception as e:
                print(f"  -> [Selenium] AVISO: Nenhum popup encontrado ou não foi possível fechá-lo (Timeout de {wait_for_popup}s).")
            # --- FIM DO ESTÁGIO 1 ---

            # --- ESTÁGIO 2: ESPERA INTELIGENTE OTIMIZADA ---
            print(f"  -> [Selenium] Estágio 2/2: Esperando pelo conteúdo final da página (máx {wait_for_content}s)...")
            
            if loja == "Terabyte":
                WebDriverWait(driver, wait_for_content).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 
                        "#valVista, #avise-me-container, #btn-avise-me, .price-payment-slip, .notify-me-wrapper"
                    ))
                )
                print("  -> [Selenium] Elemento da Terabyte (preço ou esgotado) encontrado.")
            
            elif loja == "Pichau":
                WebDriverWait(driver, wait_for_content).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, 
                        ".mui-1jk88bq-price_vista-extraSpacePriceVista, .mui-1nlpwp-availability-outOfStock"
                    ))
                )
                print("  -> [Selenium] Elemento da Pichau (preço ou esgotado) encontrado.")
            # --- FIM DO ESTÁGIO 2 ---
        
        else:
            time.sleep(5)
            
        
        html = driver.page_source
        if "403 Forbidden" in html or "Access denied" in html:
             print("  -> [Selenium] ALERTA: Página bloqueada (detectou 403 no HTML).")
             return None
        soup = BeautifulSoup(html, 'lxml')
        return soup
    
    except Exception as e:
        # Se der timeout (elemento não apareceu), vai cair aqui
        print(f"  -> [Selenium] Erro/Timeout ao esperar pelo conteúdo da página {url}. Erro: {e}")
        return None
        
    finally:
        # --- 3. DESTRUIR O NAVEGADOR ---
        if driver:
            print("  -> [Selenium] Fechando sessão do navegador...")
            driver.quit()
        # --- FIM DA DESTRUIÇÃO ---
# --- FIM DA FUNÇÃO CORRIGIDA ---

def buscar_dados_loja(url, loja):
    print(f"  Acessando {loja}: {url[:50]}...")
    try:
        if loja == "Kabum":
            response = s.get(url, timeout=15)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'lxml')
            return buscar_dados_kabum(url, soup) # Retorna 4 valores
        
        elif loja == "Pichau" or loja == "Terabyte":
            soup = get_selenium_soup(url, loja) # Esta função agora abre e fecha o driver
            
            if not soup:
                print(f"  -> Falha ao obter HTML do Selenium para {loja}.")
                return None, None, None, None # <-- MUDANÇA
            if loja == "Pichau":
                return buscar_dados_pichau(url, soup) # Retorna 4 valores
            elif loja == "Terabyte":
                return buscar_dados_terabyte(url, soup) # Retorna 4 valores
        else:
            print(f"  -> Loja '{loja}' não suportada.")
            return None, None, None, None # <-- MUDANÇA
    except requests.exceptions.HTTPError as err:
        print(f"  -> Erro HTTP [requests] ao acessar {loja}: {err}")
    except requests.exceptions.RequestException as e:
        print(f"  -> Erro de conexão [requests] ao acessar {loja}: {e}")
    except Exception as e:
        print(f"  -> Exceção GERAL ao processar {loja} ({url[:50]}...): {e}")
    return None, None, None, None # <-- MUDANÇA

# --- O PROGRAMA PRINCIPAL (v10.25) ---
print(f"--- INICIANDO MONITOR DE PREÇOS (v10.25 - Correção Imagens Pichau) ---")

resultados_de_hoje = []
timestamp_agora = datetime.now()

for produto_base_info in LISTA_DE_PRODUTOS:
    nome_base = produto_base_info["nome_base"].strip()
    categoria = produto_base_info["categoria"].strip() 
    print(f"\nBuscando: {nome_base} (Categoria: {categoria})")
    
    for loja, url_loja in produto_base_info["urls"].items():
        if not url_loja or url_loja.strip() == "":
            print(f" Pulando loja: {loja} (URL não fornecida)")
            continue
            
        print(f" Tentando loja: {loja}")
        # --- MUDANÇA AQUI ---
        nome_raspado, preco_raspado, imagem_raspada, descricao_raspada = buscar_dados_loja(url_loja, loja)
        # --- FIM DA MUDANÇA ---
        
        if nome_raspado and preco_raspado is not None:
            resultados_de_hoje.append({
                "timestamp": timestamp_agora,
                "produto_base": nome_base, 
                "categoria": categoria, 
                "nome_completo_raspado": nome_raspado,
                "preco": preco_raspado,
                "imagem_url": imagem_raspada if imagem_raspada else "",
                "loja": loja, 
                "url": url_loja,
                "descricao": descricao_raspada if descricao_raspada else "" # <-- NOVO
            })
        else:
            print(f"  -> Falha ao salvar dados de {nome_base} na loja {loja}.")
        
        print("  Pausando por 10 segundos...\n")
        time.sleep(10)

print("\nBusca concluída.")

if resultados_de_hoje:
    try:
        df_hoje = pd.DataFrame(resultados_de_hoje)
        # --- MUDANÇA AQUI ---
        colunas_ordenadas = ["timestamp", "produto_base", "categoria", "nome_completo_raspado", "preco", "imagem_url", "loja", "url", "descricao"]
        # --- FIM DA MUDANÇA ---
        df_hoje = df_hoje.reindex(columns=colunas_ordenadas)

        DATABASE_URL = os.environ.get('DATABASE_URL')
        
        if not DATABASE_URL:
            print("ERRO CRÍTICO: A variável de ambiente 'DATABASE_URL' não foi encontrada.")
            print("Verifique se você criou o arquivo .env e colocou a URL lá.")
        else:
            print("Conectando ao banco de dados...")
            if DATABASE_URL.startswith("postgres://"):
                DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
                
            engine = create_engine(DATABASE_URL)
            
            df_hoje.to_sql('precos', con=engine, if_exists='append', index=False)
            
            print(f"Sucesso! {len(df_hoje)} registros foram salvos no banco de dados na tabela 'precos'.")

    except Exception as e:
        print(f"ERRO ao salvar dados no banco de dados: {e}")
        traceback.print_exc()

else:
    print("Nenhum dado foi coletado hoje.")


print("\n--- FIM DA EXECUÇÃO ---")