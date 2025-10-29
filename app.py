# meu_comparador_backend/app.py

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
from datetime import datetime
import requests
import io
import traceback # Importar traceback para depuração mais detalhada

app = Flask(__name__)
# Permitir CORS de qualquer origem para dep
CORS(app)

# --- URL RAW DO SEU CSV NO GITHUB ---
URL_CSV_GITHUB = "https://raw.githubusercontent.com/JVv-dev/meu_comparador_backend/master/precos.csv"

DADOS_CACHE = None

def carregar_dados_csv():
    global DADOS_CACHE
    print(f"Tentando carregar CSV de: {URL_CSV_GITHUB}")
    try:
        response = requests.get(URL_CSV_GITHUB, timeout=10) # Adiciona timeout
        response.raise_for_status() # Levanta HTTPError para status 4xx/5xx

        csv_content = io.StringIO(response.text)
        df = pd.read_csv(csv_content, sep=';')

        colunas_necessarias = ['timestamp', 'preco', 'produto_base', 'loja', 'url', 'nome_completo_raspado']
        if not all(coluna in df.columns for coluna in colunas_necessarias):
            print(f"Erro: CSV baixado não contém todas as colunas necessárias: {colunas_necessarias}. Colunas encontradas: {df.columns.tolist()}")
            return None

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce').fillna(0.0)

        if 'imagem_url' not in df.columns:
            df['imagem_url'] = ''
        df['imagem_url'] = df['imagem_url'].fillna('')

        print("CSV carregado e processado com sucesso do GitHub.")
        DADOS_CACHE = df
        return DADOS_CACHE
    except requests.exceptions.Timeout:
        print(f"Erro: Tempo limite excedido ao baixar o CSV de {URL_CSV_GITHUB}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Erro de requisição ao baixar o CSV do GitHub: {e}")
        return None
    except Exception as e:
        print(f"Erro inesperado ao processar o CSV baixado:")
        traceback.print_exc() # Imprime o stack trace completo
        return None

# Carrega os dados na inicialização da API
# Isso será feito uma vez quando o Gunicorn iniciar o processo de trabalho
DADOS_CACHE = carregar_dados_csv()

# Rota de teste simples para verificar se a API está respondendo
@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "API de Comparador de Produtos está funcionando!", "csv_loaded": DADOS_CACHE is not None}), 200

# Rota de saúde para verificar se a API está funcionando
@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "healthy", "csv_loaded": DADOS_CACHE is not None, "products_count": len(DADOS_CACHE['produto_base'].unique()) if DADOS_CACHE is not None else 0}), 200

# --- SUA ROTA /api/products, USANDO DADOS_CACHE ---
@app.route('/api/products', methods=['GET'])
def get_products():
    global DADOS_CACHE
    if DADOS_CACHE is None or DADOS_CACHE.empty:
        # Tenta recarregar se o cache estiver vazio ou se o DataFrame for vazio
        print("Cache vazio ou DataFrame vazio para /api/products, tentando recarregar CSV...")
        DADOS_CACHE = carregar_dados_csv()
        if DADOS_CACHE is None or DADOS_CACHE.empty:
            print("Nenhum dado válido carregado do CSV para /api/products após recarga.")
            return jsonify({"error": "Não foi possível carregar os dados dos produtos ou o CSV está vazio."}), 500

    produtos_formatados = []
    try:
        for nome_base, group in DADOS_CACHE.groupby('produto_base'):
            try:
                produto_recente = group.sort_values(by='timestamp', ascending=False).iloc[0]

                lojas = []
                df_lojas_recentes = group.loc[group.groupby('loja')['timestamp'].idxmax()]

                for _, loja_info in df_lojas_recentes.iterrows():
                    lojas.append({
                        "name": loja_info['loja'],
                        "price": float(loja_info['preco']),
                        "originalPrice": None,
                        "shipping": "Consultar",
                        "rating": 0,
                        "reviews": 0,
                        "affiliateLink": loja_info['url'],
                        "inStock": loja_info['preco'] > 0 and not pd.isna(loja_info['preco'])
                    })

                historico_df = group.sort_values('timestamp')[['timestamp', 'preco', 'loja']].drop_duplicates()
                historico_formatado = []
                for _, row in historico_df.iterrows():
                    historico_formatado.append({
                        "date": row['timestamp'].strftime('%Y-%m-%d'),
                        "price": float(row['preco']),
                        "loja": row['loja']
                    })

                produtos_formatados.append({
                    "id": str(nome_base),
                    "name": produto_recente['nome_completo_raspado'],
                    "image": produto_recente['imagem_url'] if produto_recente['imagem_url'] else "/placeholder.svg",
                    "category": "Eletrônicos",
                    "stores": lojas,
                    "priceHistory": historico_formatado
                })
            except Exception as e:
                print(f"Erro detalhado ao processar produto '{nome_base}': {e}")
                traceback.print_exc()
                continue
    except Exception as e:
        print(f"Erro geral ao iterar sobre grupos de produtos: {e}")
        traceback.print_exc()
        return jsonify({"error": "Erro interno ao processar produtos"}), 500

    print(f"Retornando {len(produtos_formatados)} produtos via API.")
    return jsonify(produtos_formatados)


# Endpoint de histórico
@app.route('/api/products/<product_id>/history', methods=['GET'])
def get_product_history(product_id):
    global DADOS_CACHE
    if DADOS_CACHE is None or DADOS_CACHE.empty:
        print("Cache vazio ou DataFrame vazio para histórico, tentando recarregar CSV...")
        DADOS_CACHE = carregar_dados_csv()
        if DADOS_CACHE is None or DADOS_CACHE.empty:
            return jsonify({"error": "Dados não encontrados ou CSV vazio"}), 404

    df_produto = DADOS_CACHE[DADOS_CACHE['produto_base'] == product_id].copy()

    if df_produto.empty:
        return jsonify({"error": "Produto não encontrado ou sem histórico"}), 404

    df_historico = df_produto.sort_values('timestamp')[['timestamp', 'preco', 'loja']].drop_duplicates()

    historico_formatado = []
    for _, row in df_historico.iterrows():
        historico_formatado.append({
            "date": row['timestamp'].strftime('%Y-%m-%d'),
            "price": float(row['preco']),
            "store": row['loja']
        })

    return jsonify(historico_formatado)

# Gunicorn usará 'app:app' para iniciar a aplicação, então este bloco não é executado em produção.
# Ele é útil apenas para testar localmente com 'python app.py'.
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    print(f"Rodando Flask localmente na porta {port}...")
    app.run(debug=True, host='0.0.0.0', port=port)