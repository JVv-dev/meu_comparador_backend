# Conteúdo para app.py
from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd
import os
from datetime import datetime

app = Flask(__name__)
CORS(app) 
ARQUIVO_CSV = r"C:\Users\joaov\OneDrive\Documentos\meu_comparador\frontend\public\precos.csv"

# Função auxiliar para ler e preparar o DataFrame base
def carregar_dados_csv():
    if not os.path.exists(ARQUIVO_CSV):
        print(f"Aviso: Arquivo {ARQUIVO_CSV} não encontrado.")
        return None
    try:
        df = pd.read_csv(ARQUIVO_CSV, sep=';')
        colunas_necessarias = ['timestamp', 'preco', 'produto_base', 'loja', 'url', 'nome_completo_raspado']
        if not all(coluna in df.columns for coluna in colunas_necessarias):
            print(f"Erro: Arquivo CSV não contém todas as colunas necessárias: {colunas_necessarias}")
            return None

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce').fillna(0.0) 

        if 'imagem_url' not in df.columns:
            df['imagem_url'] = '' 
        df['imagem_url'] = df['imagem_url'].fillna('') 

        return df
    except Exception as e:
        print(f"Erro ao carregar ou processar o CSV: {e}")
        return None

# --- ESTA É A ÚNICA VERSÃO DA ROTA /api/products ---
@app.route('/api/products')
def get_products():
    df = carregar_dados_csv()
    if df is None:
        print("Nenhum dado válido carregado do CSV para /api/products.")
        return jsonify([]) 

    produtos_formatados = []
    try:
        for nome_base, group in df.groupby('produto_base'):
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

                # --- INÍCIO DA CORREÇÃO (PARA HISTÓRICO MULTI-LOJA) ---
                # 1. Seleciona as colunas corretas ('loja' incluída)
                historico_df = group.sort_values('timestamp')[['timestamp', 'preco', 'loja']].drop_duplicates()

                historico_formatado = []
                for _, row in historico_df.iterrows():
                     historico_formatado.append({
                        "date": row['timestamp'].strftime('%Y-m-%d'),
                        "price": float(row['preco']), # Garante float
                        "loja": row['loja'] # <-- 2. ADICIONA A LOJA AQUI
                    })
                # --- FIM DA CORREÇÃO ---


                produtos_formatados.append({
                    "id": str(nome_base),
                    "name": produto_recente['nome_completo_raspado'],
                    "image": produto_recente['imagem_url'] if produto_recente['imagem_url'] else "/placeholder.svg",
                    "category": "Eletrônicos",
                    "stores": lojas,
                    "priceHistory": historico_formatado # <-- Agora o histórico está correto
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


# Endpoint de histórico (pode remover se não for usar diretamente)
@app.route('/api/products/<product_id>/history')
def get_product_history(product_id):
    df = carregar_dados_csv()
    if df is None:
        return jsonify({"error": "Dados não encontrados"}), 404

    df_produto = df[df['produto_base'] == product_id].copy() 

    if df_produto.empty:
        return jsonify({"error": "Produto não encontrado"}), 404

    df_historico = df_produto.sort_values('timestamp')[['timestamp', 'preco', 'loja']].drop_duplicates()

    historico_formatado = []
    for _, row in df_historico.iterrows():
         historico_formatado.append({
            "date": row['timestamp'].strftime('%Y-%m-%d'),
            "price": float(row['preco']), 
            "store": row['loja'] # 'store' ou 'loja', decida o padrão
        })

    return jsonify(historico_formatado)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5001)