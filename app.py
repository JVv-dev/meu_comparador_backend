# meu_comparador_backend/app.py

from flask import Flask, jsonify, request # Adicionado 'request' aqui se for usar em outras rotas
from flask_cors import CORS
import pandas as pd
import os
from datetime import datetime
import requests # Importe a biblioteca requests
import io       # Importe a biblioteca io

app = Flask(__name__)
CORS(app) # Habilita CORS para todas as rotas por padrão

# --- URL RAW DO SEU CSV NO GITHUB ---
# ATENÇÃO: Verifique se esta URL está CORRETA para o seu repositório.
# Ela deve apontar para o arquivo 'precos.csv' dentro do seu repositório 'meu_comparador'
# na branch 'main', na pasta 'frontend/public'.
URL_CSV_GITHUB = "https://raw.githubusercontent.com/JVv-dev/meu_comparador/refs/heads/main/frontend/public/precos.csv"

# Variável global para armazenar os dados (cache)
DADOS_CACHE = None

# Função auxiliar para carregar os dados do CSV (AGORA DA URL DO GITHUB)
def carregar_dados_csv():
    global DADOS_CACHE # Declara que queremos modificar a variável global
    print(f"Tentando carregar CSV de: {URL_CSV_GITHUB}")
    try:
        response = requests.get(URL_CSV_GITHUB)
        response.raise_for_status() # Lança um erro para status HTTP 4xx/5xx

        # Usa io.StringIO para tratar o texto da resposta como um arquivo
        csv_content = io.StringIO(response.text)

        df = pd.read_csv(csv_content, sep=';')

        # Verifica se colunas essenciais existem
        colunas_necessarias = ['timestamp', 'preco', 'produto_base', 'loja', 'url', 'nome_completo_raspado']
        if not all(coluna in df.columns for coluna in colunas_necessarias):
            print(f"Erro: CSV baixado não contém todas as colunas necessárias: {colunas_necessarias}")
            return None

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce').fillna(0.0)

        # Garante que a coluna 'imagem_url' existe e não tem NaNs
        if 'imagem_url' not in df.columns:
            df['imagem_url'] = ''
        df['imagem_url'] = df['imagem_url'].fillna('')

        print("CSV carregado e processado com sucesso do GitHub.")
        DADOS_CACHE = df # Armazena no cache
        return DADOS_CACHE
    except requests.exceptions.RequestException as e:
        print(f"Erro ao baixar o CSV do GitHub: {e}")
        DADOS_CACHE = None # Limpa o cache em caso de erro
        return None
    except Exception as e:
        print(f"Erro ao processar o CSV baixado: {e}")
        DADOS_CACHE = None # Limpa o cache em caso de erro
        return None

# Carrega os dados na inicialização da API
# Isso será feito uma vez quando o Render iniciar o serviço
DADOS_CACHE = carregar_dados_csv()

# --- SUA ROTA /api/products ORIGINAL, AGORA USANDO DADOS_CACHE ---
@app.route('/api/products')
def get_products():
    global DADOS_CACHE
    if DADOS_CACHE is None:
        # Se o cache estiver vazio (ex: falha no carregamento inicial ou API reiniciou)
        # Tenta recarregar os dados
        print("Cache vazio, tentando recarregar CSV...")
        DADOS_CACHE = carregar_dados_csv()
        if DADOS_CACHE is None:
            print("Nenhum dado válido carregado do CSV para /api/products após recarga.")
            return jsonify({"error": "Não foi possível carregar os dados dos produtos."}), 500

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
                        "originalPrice": None, # Manter como None se não tiver
                        "shipping": "Consultar",
                        "rating": 0,
                        "reviews": 0,
                        "affiliateLink": loja_info['url'],
                        "inStock": loja_info['preco'] > 0 and not pd.isna(loja_info['preco'])
                    })

                # --- Lógica do histórico multi-loja ---
                historico_df = group.sort_values('timestamp')[['timestamp', 'preco', 'loja']].drop_duplicates()
                historico_formatado = []
                for _, row in historico_df.iterrows():
                    historico_formatado.append({
                        "date": row['timestamp'].strftime('%Y-%m-%d'),
                        "price": float(row['preco']),
                        "loja": row['loja'] # <-- Mantém o nome da coluna como 'loja'
                    })
                # --- FIM da lógica do histórico ---

                produtos_formatados.append({
                    "id": str(nome_base),
                    "name": produto_recente['nome_completo_raspado'],
                    "image": produto_recente['imagem_url'] if produto_recente['imagem_url'] else "/placeholder.svg",
                    "category": "Eletrônicos",
                    "stores": lojas,
                    "priceHistory": historico_formatado
                })
            except Exception as e:
                import traceback
                print(f"Erro detalhado ao processar produto '{nome_base}':")
                traceback.print_exc()
                continue
    except Exception as e:
        import traceback
        print(f"Erro geral ao iterar sobre grupos de produtos:")
        traceback.print_exc()
        return jsonify({"error": "Erro interno ao processar produtos"}), 500

    print(f"Retornando {len(produtos_formatados)} produtos via API.")
    return jsonify(produtos_formatados)


# Endpoint de histórico (sua rota original)
@app.route('/api/products/<product_id>/history')
def get_product_history(product_id):
    global DADOS_CACHE
    if DADOS_CACHE is None:
        print("Cache vazio, tentando recarregar CSV para histórico...")
        DADOS_CACHE = carregar_dados_csv()
        if DADOS_CACHE is None:
            return jsonify({"error": "Dados não encontrados"}), 404

    df_produto = DADOS_CACHE[DADOS_CACHE['produto_base'] == product_id].copy()

    if df_produto.empty:
        return jsonify({"error": "Produto não encontrado"}), 404

    df_historico = df_produto.sort_values('timestamp')[['timestamp', 'preco', 'loja']].drop_duplicates()

    historico_formatado = []
    for _, row in df_historico.iterrows():
        historico_formatado.append({
            "date": row['timestamp'].strftime('%Y-%m-%d'),
            "price": float(row['preco']),
            "store": row['loja'] # Mantive 'store' aqui para compatibilidade com o que você tinha
        })

    return jsonify(historico_formatado)


if __name__ == '__main__':
    # Usar host='0.0.0.0' para que a API seja acessível externamente (necessário para o Render)
    # Definir debug=False em ambiente de produção (Render já faz isso)
    # A porta será definida pelo ambiente do Render (geralmente via variável de ambiente PORT)
    app.run(debug=True, host='0.0.0.0', port=os.environ.get('PORT', 5001))