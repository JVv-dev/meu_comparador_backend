# meu_comparador_backend/app.py (v11.2 - Lógica de Descrição Corrigida)

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

        # --- Processamento de Tipos ---
        colunas_necessarias = ['timestamp', 'preco', 'produto_base', 'loja', 'url', 'nome_completo_raspado']
        if not all(coluna in df.columns for coluna in colunas_necessarias):
            print(f"Erro: Tabela 'precos' não contém todas as colunas necessárias.")
            return None

        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['preco'] = pd.to_numeric(df['preco'], errors='coerce').fillna(0.0)

        # Fallbacks para colunas novas
        if 'imagem_url' not in df.columns:
            df['imagem_url'] = ''
        df['imagem_url'] = df['imagem_url'].fillna('')
        
        if 'categoria' not in df.columns:
            df['categoria'] = 'Eletrônicos'
        df['categoria'] = df['categoria'].fillna('Eletrônicos')

        # --- MUDANÇA: Limpa os dados do banco na leitura ---
        df['produto_base'] = df['produto_base'].str.strip()
        df['categoria'] = df['categoria'].str.strip()
        if 'descricao' not in df.columns:
            df['descricao'] = ''
        df['descricao'] = df['descricao'].fillna('')
        # --- FIM DA MUDANÇA ---
        
        print(f"Sucesso! {len(df)} registros lidos do banco de dados.")
        return df
        
    except Exception as e:
        print(f"Erro ao conectar ou ler do banco de dados:")
        traceback.print_exc()
        return None

# Rota de teste
@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "API de Comparador de Produtos (v11.2 - Lógica de Descrição Corrigida) está funcionando!"}), 200

# Rota de saúde
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

# Rota de Produtos (Principal)
@app.route('/api/products', methods=['GET'])
def get_products():
    df_dados = get_dados_do_db()
    
    if df_dados is None or df_dados.empty:
        return jsonify({"error": "Não foi possível carregar os dados dos produtos."}), 500

    produtos_formatados = []
    try:
        # Agrupa pelos nomes de produto já limpos
        for nome_base, group in df_dados.groupby('produto_base'):
            try:
                group_valido = group[group['preco'] > 0]
                if not group_valido.empty:
                    produto_principal = group_valido.loc[group_valido['preco'].idxmin()]
                else:
                    produto_principal = group.sort_values(by='timestamp', ascending=False).iloc[0]

                precos_historicos_validos = group[group['preco'] > 0]['preco']
                preco_min_historico = 0.0
                preco_medio_historico = 0.0

                if not precos_historicos_validos.empty:
                    preco_min_historico = float(precos_historicos_validos.min())
                    preco_medio_historico = float(precos_historicos_validos.mean())

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
                    "name": produto_principal['nome_completo_raspado'],
                    "image": produto_principal['imagem_url'],
                    "category": produto_principal['categoria'], 
                    "stores": lojas,
                    "priceHistory": historico_formatado,
                    "precoMinimoHistorico": preco_min_historico, 
                    "precoMedioHistorico": preco_medio_historico
                })

            except Exception as e:
                print(f"Erro detalhado ao processar produto '{nome_base}': {e}")
                traceback.print_exc()
                continue
    except Exception as e:
        print(f"Erro geral ao iterar sobre grupos de produtos: {e}")
        traceback.print_exc()
        return jsonify({"error": "Erro interno ao processar produtos"}), 500

    return jsonify(produtos_formatados)


# Rota de Histórico (Também limpa o ID)
@app.route('/api/products/<product_id>/history', methods=['GET'])
def get_product_history(product_id):
    product_id_limpo = product_id.strip() # Limpa o ID
    
    df_dados = get_dados_do_db()
    if df_dados is None or df_dados.empty:
        return jsonify({"error": "Dados não encontrados"}), 404
        
    df_produto = df_dados[df_dados['produto_base'] == product_id_limpo].copy() # Usa ID limpo
    
    if df_produto.empty:
        return jsonify({"error": "Produto não encontrado ou sem histórico"}), 404
    df_historico = df_produto.sort_values('timestamp')[['timestamp', 'preco', 'loja']].drop_duplicates()
    historico_formatado = []
    for _, row in df_historico.iterrows():
        historico_formatado.append({
            "date": row['timestamp'].strftime('%Y-%m-%d'),
            "price": float(row['preco']),
            "loja": row['loja']
        })
    return jsonify(historico_formatado)

# ---
# --- NOVA ROTA DE PRODUTO ÚNICO ---
# ---
@app.route('/api/product/<path:product_base_name>', methods=['GET'])
def get_single_product(product_base_name):
    
    # Limpa o nome do produto que vem da URL
    product_name_limpo = product_base_name.strip()
    print(f"Buscando dados para produto único: '{product_name_limpo}'")

    df_dados = get_dados_do_db()
    
    if df_dados is None or df_dados.empty:
        print("Falha ao carregar dados do DB para produto único.")
        return jsonify({"error": "Não foi possível carregar os dados."}), 500

    # Filtra o DataFrame (coluna 'produto_base' já foi limpa pelo get_dados_do_db)
    df_produto = df_dados[df_dados['produto_base'] == product_name_limpo].copy()

    if df_produto.empty:
        print(f"Produto '{product_name_limpo}' não encontrado no banco de dados.")
        return jsonify({"error": "Produto não encontrado"}), 404

    # --- Reutiliza a lógica de formatação de '/api/products' ---
    try:
        group = df_produto 
        
        group_valido = group[group['preco'] > 0]
        if not group_valido.empty:
            produto_principal = group_valido.loc[group_valido['preco'].idxmin()]
        else:
            produto_principal = group.sort_values(by='timestamp', ascending=False).iloc[0]

        precos_historicos_validos = group[group['preco'] > 0]['preco']
        preco_min_historico = 0.0
        preco_medio_historico = 0.0

        if not precos_historicos_validos.empty:
            preco_min_historico = float(precos_historicos_validos.min())
            preco_medio_historico = float(precos_historicos_validos.mean())

        lojas = []
        df_lojas_recentes = group.loc[group.groupby('loja')['timestamp'].idxmax()]

        # --- INÍCIO DA CORREÇÃO (v11.2 - Lógica da Descrição) ---
        
        # 1. Pega a descrição da loja principal (menor preço)
        # .get() é mais seguro caso a coluna 'descricao' não exista por algum motivo
        descricao_final = produto_principal.get('descricao', '')

        # 2. Se a loja principal (ex: Pichau) não tiver descrição,
        #    tenta pegar a da Kabum (que sempre tem).
        if not descricao_final or not descricao_final.strip():
            print(f"AVISO: Produto '{product_name_limpo}' está sem descrição na loja principal ({produto_principal['loja']}). Procurando fallback...")
            
            # Itera em todas as lojas desse produto
            for _, loja_row in df_lojas_recentes.iterrows():
                if loja_row['loja'] == 'Kabum' and loja_row.get('descricao'):
                    descricao_final = loja_row['descricao']
                    print("  -> Usando descrição da Kabum como fallback.")
                    break
            
            # 3. Se ainda não achou, pega qualquer uma
            if not descricao_final or not descricao_final.strip():
                 for desc in group['descricao']:
                    if desc and desc.strip():    
                        descricao_final = desc
                        print("  -> Usando a primeira descrição não-nula encontrada como fallback.")
                        break
        
        # --- FIM DA CORREÇÃO ---

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

        produto_formatado = {
            "id": str(product_name_limpo), 
            "name": produto_principal['nome_completo_raspado'],
            "image": produto_principal['imagem_url'],
            "category": produto_principal['categoria'],
            "stores": lojas,
            "priceHistory": historico_formatado,
            "precoMinimoHistorico": preco_min_historico, 
            "precoMedioHistorico": preco_medio_historico,
            "descricao": descricao_final # <-- Variável corrigida
        }
        
        print(f"Retornando dados formatados para: {product_name_limpo}")
        return jsonify(produto_formatado) # Retorna um único objeto

    except Exception as e:
        print(f"Erro geral ao processar produto único '{product_name_limpo}': {e}")
        traceback.print_exc()
        return jsonify({"error": "Erro interno ao processar produto"}), 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    print(f"Rodando Flask localmente na porta {port} (lendo do DB)...")
    app.run(debug=True, host='0.0.0.0', port=port)