# meu_comparador_backend/app.py (v9.1 - Corrigido)

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
import traceback
from sqlalchemy import create_engine

app = Flask(__name__)
CORS(app)

def get_dados_do_db():
    """
    Busca os dados mais recentes diretamente do banco de dados PostgreSQL.
    """
    print("Tentando buscar dados do banco de dados...")
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        if not DATABASE_URL:
            print("ERRO CRÍTICO: Variável de ambiente 'DATABASE_URL' não encontrada.")
            return None

        if DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
            
        engine = create_engine(DATABASE_URL)
        
        df = pd.read_sql("SELECT * FROM precos", engine)
        
        if df.empty:
            print("A tabela 'precos' está vazia.")
            return None

        colunas_necessarias = ['timestamp', 'preco', 'produto_base', 'loja', 'url', 'nome_completo_raspado']
        if not all(coluna in df.columns for coluna in colunas_necessarias):
            print(f"Erro: Tabela 'precos' não contém todas as colunas necessárias.")
            return None

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce').fillna(0.0)

        if 'imagem_url' not in df.columns:
            df['imagem_url'] = ''
        df['imagem_url'] = df['imagem_url'].fillna('')
        
        print(f"Sucesso! {len(df)} registros lidos do banco de dados.")
        return df
        
    except Exception as e:
        print(f"Erro ao conectar ou ler do banco de dados:")
        traceback.print_exc()
        return None

# ... (rotas / e /health permanecem iguais) ...
@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "API de Comparador de Produtos está funcionando!"}), 200

@app.route('/health', methods=['GET'])
def health_check():
    df = get_dados_do_db()
    db_accessible = df is not None
    products_count = len(df['produto_base'].unique()) if db_accessible and not df.empty else 0
    return jsonify({
        "status": "healthy", 
        "database_accessible": db_accessible, 
        "products_count": products_count
    }), 200

# --- ROTA /api/products, AGORA LENDO DO DB A CADA CHAMADA ---
@app.route('/api/products', methods=['GET'])
def get_products():
    df_dados = get_dados_do_db()
    
    if df_dados is None or df_dados.empty:
        print("Nenhum dado válido carregado do banco de dados para /api/products.")
        return jsonify({"error": "Não foi possível carregar os dados dos produtos."}), 500

    produtos_formatados = []
    try:
        for nome_base, group in df_dados.groupby('produto_base'):
            try:
                # --- LÓGICA DE NOME/IMAGEM CORRIGIDA ---
                # 1. Encontra a linha com o menor preço válido (em estoque)
                group_valido = group[group['preco'] > 0]
                if not group_valido.empty:
                    # Pega o nome e imagem do produto com menor preço
                    produto_principal = group_valido.loc[group_valido['preco'].idxmin()]
                else:
                    # Se todos estiverem 0.0 (sem estoque), pega o mais recente
                    produto_principal = group.sort_values(by='timestamp', ascending=False).iloc[0]
                # --- FIM DA CORREÇÃO ---


                # Esta lógica (que você tinha) está CORRETA e sempre esteve
                # Ela pega o preço mais recente de CADA loja
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
                    # --- USA O NOME/IMAGEM CORRIGIDOS ---
                    "name": produto_principal['nome_completo_raspado'],
                    "image": produto_principal['imagem_url'] if produto_principal['imagem_url'] else "/placeholder.svg",
                    "category": "Eletrônicos",
                    "stores": lojas, # 'lojas' (plural) contém TODAS as lojas
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

    print(f"Retornando {len(produtos_formatados)} produtos via API (lidos do DB).")
    return jsonify(produtos_formatados)


# --- Endpoint de histórico (CORRIGIDO) ---
@app.route('/api/products/<product_id>/history', methods=['GET'])
def get_product_history(product_id):
    df_dados = get_dados_do_db()

    if df_dados is None or df_dados.empty:
        return jsonify({"error": "Dados não encontrados"}), 404

    df_produto = df_dados[df_dados['produto_base'] == product_id].copy()

    if df_produto.empty:
        return jsonify({"error": "Produto não encontrado ou sem histórico"}), 404

    df_historico = df_produto.sort_values('timestamp')[['timestamp', 'preco', 'loja']].drop_duplicates()

    historico_formatado = []
    for _, row in df_historico.iterrows():
        historico_formatado.append({
            "date": row['timestamp'].strftime('%Y-%m-%d'),
            "price": float(row['preco']),
            "loja": row['loja'] # <-- CORRIGIDO (era 'store' e o modal esperava 'loja')
        })

    return jsonify(historico_formatado)


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    print(f"Rodando Flask localmente na porta {port} (lendo do DB)...")
    app.run(debug=True, host='0.0.0.0', port=port)