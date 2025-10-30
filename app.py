# meu_comparador_backend/app.py (v9.0 - Lendo do PostgreSQL)

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
import os
import traceback
from sqlalchemy import create_engine # NOVO: Para conectar ao DB

app = Flask(__name__)
CORS(app)

# --- REMOVIDO: URL_CSV_GITHUB e DADOS_CACHE não são mais usados ---

def get_dados_do_db():
    """
    Busca os dados mais recentes diretamente do banco de dados PostgreSQL.
    Esta função é chamada a cada requisição para garantir dados frescos.
    """
    print("Tentando buscar dados do banco de dados...")
    try:
        DATABASE_URL = os.environ.get('DATABASE_URL')
        if not DATABASE_URL:
            print("ERRO CRÍTICO: Variável de ambiente 'DATABASE_URL' não encontrada.")
            return None

        # Substitui 'postgres://' por 'postgresql://' para compatibilidade com SQLAlchemy
        if DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
            
        engine = create_engine(DATABASE_URL)
        
        # Lê a tabela 'precos' inteira para um DataFrame
        # A lógica de processamento/agrupamento será feita no Python
        df = pd.read_sql("SELECT * FROM precos", engine)
        
        if df.empty:
            print("A tabela 'precos' está vazia.")
            return None

        # --- Processamento de Tipos (igual ao que fazíamos com o CSV) ---
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

# --- REMOVIDA: Função carregar_dados_csv() ---

# Rota de teste simples para verificar se a API está respondendo
@app.route('/', methods=['GET'])
def home():
    # Não temos mais DADOS_CACHE, então removemos a verificação
    return jsonify({"message": "API de Comparador de Produtos está funcionando!"}), 200

# Rota de saúde (simplificada)
@app.route('/health', methods=['GET'])
def health_check():
    # A verificação de saúde agora tenta ler o DB
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
    # Busca dados frescos do DB em CADA requisição
    df_dados = get_dados_do_db()
    
    if df_dados is None or df_dados.empty:
        print("Nenhum dado válido carregado do banco de dados para /api/products.")
        return jsonify({"error": "Não foi possível carregar os dados dos produtos."}), 500

    produtos_formatados = []
    try:
        # Usa o DataFrame 'df_dados' (em vez de DADOS_CACHE)
        for nome_base, group in df_dados.groupby('produto_base'):
            try:
                # O resto da sua lógica de agrupamento funciona perfeitamente
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

    print(f"Retornando {len(produtos_formatados)} produtos via API (lidos do DB).")
    return jsonify(produtos_formatados)


# --- Endpoint de histórico, AGORA LENDO DO DB ---
@app.route('/api/products/<product_id>/history', methods=['GET'])
def get_product_history(product_id):
    # Busca dados frescos do DB em CADA requisição
    df_dados = get_dados_do_db()

    if df_dados is None or df_dados.empty:
        return jsonify({"error": "Dados não encontrados"}), 404

    # Usa o DataFrame 'df_dados' (em vez de DADOS_CACHE)
    df_produto = df_dados[df_dados['produto_base'] == product_id].copy()

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


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    print(f"Rodando Flask localmente na porta {port} (lendo do DB)...")
    app.run(debug=True, host='0.0.0.0', port=port)