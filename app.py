# meu_comparador_backend/app.py (v9.2 - CORRIGIDO)

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
import traceback
from sqlalchemy import create_engine

app = Flask(__name__)
CORS(app)

def get_dados_do_db():
    print("Tentando buscar dados do banco de dados...")
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        if not DATABASE_URL:
            print("ERRO CRÍTICO: Variável de ambiente 'DATABASE_URL' não encontrada.")
            return None

        if DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
            
        engine = create_engine(DATABASE_URL)
        
        # --- NOVO: Query para pegar APENAS os dados mais recentes de CADA loja/produto ---
        # Esta query é mais inteligente. Ela busca no DB e já retorna
        # apenas a entrada mais recente (último timestamp) para cada combo "produto_base" + "loja".
        query = """
        WITH RankedPrices AS (
            SELECT 
                *,
                ROW_NUMBER() OVER(
                    PARTITION BY "produto_base", "loja" 
                    ORDER BY "timestamp" DESC
                ) as rn
            FROM precos
        )
        SELECT * FROM RankedPrices
        WHERE rn = 1;
        """
        df = pd.read_sql(query, engine)
        
        if df.empty:
            print("A tabela 'precos' está vazia ou a query não retornou dados.")
            return None

        # Processamento de Tipos (simplificado, pois já pegamos os dados mais recentes)
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce').fillna(0.0)
        if 'imagem_url' not in df.columns:
            df['imagem_url'] = ''
        df['imagem_url'] = df['imagem_url'].fillna('')
        
        print(f"Sucesso! {len(df)} registros (mais recentes por loja) lidos do DB.")
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

# --- ROTA /api/products (AGORA MUITO MAIS SIMPLES) ---
@app.route('/api/products', methods=['GET'])
def get_products():
    # 1. Busca os dados JÁ FILTRADOS (só os mais recentes de cada loja)
    df_dados_recentes = get_dados_do_db()
    
    if df_dados_recentes is None or df_dados_recentes.empty:
        print("Nenhum dado válido carregado do banco de dados para /api/products.")
        return jsonify({"error": "Não foi possível carregar os dados dos produtos."}), 500

    produtos_formatados = []
    try:
        # 2. Agrupa por produto_base (ex: "RTX 4070")
        for nome_base, group in df_dados_recentes.groupby('produto_base'):
            
            # --- LÓGICA DE NOME/IMAGEM CORRIGIDA ---
            # Pega o nome/imagem da loja com o menor preço (ou o primeiro, se todos sem estoque)
            produto_principal = group.sort_values(by='preco', ascending=True).iloc[0]
            if produto_principal['preco'] == 0: # Se o mais barato for 0, pega o primeiro
                 produto_principal = group.iloc[0]

            lojas = []
            # 3. Itera sobre o 'group'. Como o group JÁ CONTÉM
            #    apenas a entrada mais recente de cada loja, podemos iterar direto!
            for _, loja_info in group.iterrows():
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

            # --- BUSCAR HISTÓRICO (ainda é necessário) ---
            # Precisamos de uma nova conexão para pegar o histórico COMPLETO
            DATABASE_URL = os.environ.get('DATABASE_URL')
            if DATABASE_URL.startswith("postgres://"):
                DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
            engine = create_engine(DATABASE_URL)
            
            # Query específica para o histórico deste produto
            query_hist = f"SELECT timestamp, preco, loja FROM precos WHERE produto_base = '{nome_base}' ORDER BY timestamp"
            df_hist = pd.read_sql(query_hist, engine)
            
            historico_formatado = []
            if not df_hist.empty:
                df_hist['preco'] = pd.to_numeric(df_hist['preco'], errors='coerce').fillna(0.0)
                df_hist['timestamp'] = pd.to_datetime(df_hist['timestamp'])
                
                # Otimização: remove duplicatas de preço no mesmo dia pela mesma loja
                df_hist = df_hist.drop_duplicates(subset=['timestamp', 'preco', 'loja'])
                
                for _, row in df_hist.iterrows():
                    historico_formatado.append({
                        "date": row['timestamp'].strftime('%Y-%m-%d'),
                        "price": float(row['preco']),
                        "loja": row['loja']
                    })

            produtos_formatados.append({
                "id": str(nome_base),
                "name": produto_principal['nome_completo_raspado'],
                "image": produto_principal['imagem_url'] if produto_principal['imagem_url'] else "/placeholder.svg",
                "category": "Eletrônicos",
                "stores": lojas,
                "priceHistory": historico_formatado
            })
            
    except Exception as e:
        print(f"Erro geral ao iterar sobre grupos de produtos: {e}")
        traceback.print_exc()
        return jsonify({"error": "Erro interno ao processar produtos"}), 500

    print(f"Retornando {len(produtos_formatados)} produtos via API (lidos do DB).")
    return jsonify(produtos_formatados)


# --- ROTA DE HISTÓRICO (Não é mais usada pelo frontend, mas mantida) ---
@app.route('/api/products/<product_id>/history', methods=['GET'])
def get_product_history(product_id):
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        if not DATABASE_URL:
            return jsonify({"error": "Configuração do DB não encontrada"}), 500
            
        if DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        engine = create_engine(DATABASE_URL)
        
        query_hist = f"SELECT timestamp, preco, loja FROM precos WHERE produto_base = '{product_id}' ORDER BY timestamp"
        df_hist = pd.read_sql(query_hist, engine)

        if df_hist.empty:
            return jsonify({"error": "Produto não encontrado ou sem histórico"}), 404

        df_hist['preco'] = pd.to_numeric(df_hist['preco'], errors='coerce').fillna(0.0)
        df_hist['timestamp'] = pd.to_datetime(df_hist['timestamp'])
        df_hist = df_hist.drop_duplicates(subset=['timestamp', 'preco', 'loja'])

        historico_formatado = []
        for _, row in df_hist.iterrows():
            historico_formatado.append({
                "date": row['timestamp'].strftime('%Y-%m-%d'),
                "price": float(row['preco']),
                "loja": row['loja'] # Corrigido de 'store' para 'loja'
            })

        return jsonify(historico_formatado)
    except Exception as e:
        print(f"Erro na rota de histórico: {e}")
        return jsonify({"error": "Erro interno ao buscar histórico"}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    print(f"Rodando Flask localmente na porta {port} (lendo do DB)...")
    app.run(debug=True, host='0.0.0.0', port=port)