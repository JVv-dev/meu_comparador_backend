# meu_comparador_backend/app.py (v13.4 - Rota de Cupons Validada)

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
import traceback
from sqlalchemy import create_engine 
import numpy as np

app = Flask(__name__)
CORS(app)

# --- FUNÇÕES AUXILIARES ---
def get_db_engine():
    """Cria a conexão com o banco de dados."""
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        if not DATABASE_URL: return None
        if DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        return create_engine(DATABASE_URL)
    except: return None

def get_dados_do_db():
    """Busca os produtos do banco."""
    try:
        engine = get_db_engine()
        if not engine: return None
        
        df = pd.read_sql("SELECT * FROM precos", engine)
        if df.empty: return None

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce').fillna(0.0)
        
        for col in ['imagem_url', 'descricao', 'categoria', 'produto_base']:
             if col not in df.columns: df[col] = ''
        
        df['imagem_url'] = df['imagem_url'].fillna('')
        df['descricao'] = df['descricao'].fillna('')
        df['categoria'] = df['categoria'].fillna('Eletrônicos').str.strip()
        df['produto_base'] = df['produto_base'].str.strip()
        
        return df
    except Exception as e: 
        print(f"Erro DB: {e}")
        return None

# --- ROTAS ---

@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "API V3.0 Online (Com Cupons)"}), 200

# --- ROTA DE CUPONS (CRUCIAL) ---
@app.route('/api/coupons', methods=['GET'])
def get_coupons():
    """Retorna a lista de cupons ativos no banco."""
    try:
        engine = get_db_engine()
        if not engine: 
            print("Erro: Sem conexão com o banco para buscar cupons.")
            return jsonify([])
        
        # Busca os cupons
        df = pd.read_sql("SELECT * FROM cupons ORDER BY id DESC", engine)
        
        if df.empty: 
            return jsonify([]) # Lista vazia se não houver cupons
        
        # Converte para lista de dicionários
        cupons = df.to_dict(orient='records')
        return jsonify(cupons)
        
    except Exception as e:
        print(f"Erro ao buscar cupons: {e}")
        # Retorna lista vazia em vez de erro 500 para não quebrar o front
        return jsonify([])

# --- ROTA DE PRODUTOS (LISTAGEM) ---
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

# --- ROTA DE PRODUTO ÚNICO (DETALHES) ---
@app.route('/api/product/<path:product_base_name>', methods=['GET'])
def get_single_product(product_base_name):
    product_name_limpo = product_base_name.strip()
    
    df_dados = get_dados_do_db()
    if df_dados is None: return jsonify({"error": "Erro DB"}), 500

    group = df_dados[df_dados['produto_base'] == product_name_limpo].copy()
    if group.empty: return jsonify({"error": "Não encontrado"}), 404

    try:
        df_recentes = group.loc[group.groupby('loja')['timestamp'].idxmax()]
        
        group_valido = df_recentes[df_recentes['preco'] > 0]
        if not group_valido.empty:
            principal = group_valido.loc[group_valido['preco'].idxmin()]
        else:
            principal = df_recentes.sort_values(by='timestamp', ascending=False).iloc[0]

        # Lógica de Descrição (Pichau > Terabyte > Vencedor)
        descricao_final = ""
        try:
            pichau_row = df_recentes[df_recentes['loja'] == 'Pichau']
            if not pichau_row.empty:
                desc = pichau_row.iloc[0]['descricao']
                if desc and len(str(desc).strip()) > 10:
                    descricao_final = desc
        except: pass

        if not descricao_final:
            try:
                tera_row = df_recentes[df_recentes['loja'] == 'Terabyte']
                if not tera_row.empty:
                    desc = tera_row.iloc[0]['descricao']
                    if desc and len(str(desc).strip()) > 10:
                        descricao_final = desc
            except: pass

        if not descricao_final:
            desc_vencedor = principal.get('descricao', '')
            if desc_vencedor and len(str(desc_vencedor).strip()) > 10:
                 descricao_final = desc_vencedor

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
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    app.run(debug=True, host='0.0.0.0', port=port)

    # Forçando atualização do Render v13.5