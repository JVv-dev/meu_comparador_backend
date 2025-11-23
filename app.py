# meu_comparador_backend/app.py (v12.1 - Prioridade Visual Pichau)

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
import traceback
from sqlalchemy import create_engine 
import numpy as np

app = Flask(__name__)
CORS(app)

def get_dados_do_db():
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        if not DATABASE_URL: return None
        if DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        engine = create_engine(DATABASE_URL)
        df = pd.read_sql("SELECT * FROM precos", engine)
        if df.empty: return None

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce').fillna(0.0)
        if 'imagem_url' not in df.columns: df['imagem_url'] = ''
        df['imagem_url'] = df['imagem_url'].fillna('')
        if 'categoria' not in df.columns: df['categoria'] = 'Eletrônicos'
        df['categoria'] = df['categoria'].fillna('Eletrônicos')
        if 'descricao' not in df.columns: df['descricao'] = ''
        df['descricao'] = df['descricao'].fillna('')
        df['produto_base'] = df['produto_base'].str.strip()
        df['categoria'] = df['categoria'].str.strip()
        
        return df
    except: return None

@app.route('/', methods=['GET'])
def home(): return jsonify({"message": "API Online"}), 200

@app.route('/api/products', methods=['GET'])
def get_products():
    df_dados = get_dados_do_db()
    if df_dados is None: return jsonify({"error": "Sem dados"}), 500

    produtos_formatados = []
    try:
        for nome_base, group in df_dados.groupby('produto_base'):
            try:
                df_recentes = group.loc[group.groupby('loja')['timestamp'].idxmax()]
                group_valido = df_recentes[df_recentes['preco'] > 0]
                
                if not group_valido.empty:
                    principal = group_valido.loc[group_valido['preco'].idxmin()]
                else:
                    principal = df_recentes.sort_values(by='timestamp', ascending=False).iloc[0]

                precos_hist = group[group['preco'] > 0]['preco']
                p_min = float(precos_hist.min()) if not precos_hist.empty else 0.0
                p_med = float(precos_hist.mean()) if not precos_hist.empty else 0.0

                lojas = []
                for _, row in df_recentes.iterrows():
                    lojas.append({
                        "name": row['loja'],
                        "price": float(row['preco']),
                        "affiliateLink": row['url'],
                        "inStock": row['preco'] > 0
                    })

                produtos_formatados.append({
                    "id": str(nome_base), 
                    "name": principal['nome_completo_raspado'],
                    "image": principal['imagem_url'],
                    "category": principal['categoria'], 
                    "stores": lojas,
                    "priceHistory": [],
                    "precoMinimoHistorico": p_min, 
                    "precoMedioHistorico": p_med
                })
            except: continue
    except Exception as e: return jsonify({"error": str(e)}), 500
    return jsonify(produtos_formatados)

# --- ROTA DE PRODUTO ÚNICO (PRIORIDADE PICHAU) ---
@app.route('/api/product/<path:product_base_name>', methods=['GET'])
def get_single_product(product_base_name):
    product_name_limpo = product_base_name.strip()
    print(f"Buscando: '{product_name_limpo}'")

    df_dados = get_dados_do_db()
    if df_dados is None: return jsonify({"error": "Erro DB"}), 500

    group = df_dados[df_dados['produto_base'] == product_name_limpo].copy()
    if group.empty: return jsonify({"error": "Não encontrado"}), 404

    try:
        # 1. Recentes de cada loja
        df_recentes = group.loc[group.groupby('loja')['timestamp'].idxmax()]
        
        # 2. Vencedor do Preço (Capa)
        group_valido = df_recentes[df_recentes['preco'] > 0]
        if not group_valido.empty:
            principal = group_valido.loc[group_valido['preco'].idxmin()]
        else:
            principal = df_recentes.sort_values(by='timestamp', ascending=False).iloc[0]

        # 3. --- LÓGICA DA DESCRIÇÃO (HIERARQUIA RÍGIDA) ---
        descricao_final = ""
        
        # Tenta Pichau PRIMEIRO (independente de preço)
        try:
            pichau_row = df_recentes[df_recentes['loja'] == 'Pichau']
            if not pichau_row.empty:
                desc = pichau_row.iloc[0]['descricao']
                if desc and len(str(desc).strip()) > 10: 
                    descricao_final = desc
                    print("  -> [Descrição] Usando Pichau (Prioridade Visual).")
        except: pass

        # Se não achou Pichau, tenta Terabyte
        if not descricao_final:
            try:
                tera_row = df_recentes[df_recentes['loja'] == 'Terabyte']
                if not tera_row.empty:
                    desc = tera_row.iloc[0]['descricao']
                    if desc and len(str(desc).strip()) > 10:
                        descricao_final = desc
                        print("  -> [Descrição] Usando Terabyte (Visual Secundário).")
            except: pass

        # Se ainda não achou, usa a do vencedor do preço
        if not descricao_final:
            descricao_final = principal.get('descricao', '')
            print(f"  -> [Descrição] Usando Vencedor ({principal['loja']}).")

        # Último recurso: Kabum
        if not descricao_final:
             try:
                kabum_row = df_recentes[df_recentes['loja'] == 'Kabum']
                if not kabum_row.empty:
                    descricao_final = kabum_row.iloc[0]['descricao']
                    print("  -> [Descrição] Fallback Kabum.")
             except: pass
        # ------------------------------------------------

        # 4. Monta Lojas
        lojas = []
        for _, row in df_recentes.iterrows():
            lojas.append({
                "name": row['loja'],
                "price": float(row['preco']),
                "originalPrice": None,
                "shipping": "Consultar",
                "rating": 0, "reviews": 0,
                "affiliateLink": row['url'],
                "inStock": row['preco'] > 0
            })
            
        # 5. Histórico
        historico_df = group.sort_values('timestamp')[['timestamp', 'preco', 'loja']].drop_duplicates()
        historico_formatado = [{"date": r['timestamp'].strftime('%Y-%m-%d'), "price": float(r['preco']), "loja": r['loja']} for _, r in historico_df.iterrows()]

        validos_hist = group[group['preco'] > 0]['preco']
        
        return jsonify({
            "id": str(product_name_limpo), 
            "name": principal['nome_completo_raspado'],
            "image": principal['imagem_url'],
            "category": principal['categoria'],
            "stores": lojas,
            "priceHistory": historico_formatado,
            "precoMinimoHistorico": float(validos_hist.min()) if not validos_hist.empty else 0.0,
            "precoMedioHistorico": float(validos_hist.mean()) if not validos_hist.empty else 0.0,
            "descricao": descricao_final
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=True, host='0.0.0.0', port=port)