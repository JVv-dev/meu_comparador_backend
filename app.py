# meu_comparador_backend/app.py (v11.4 - Versão Final Corrigida)

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
        
        # Lê a tabela completa
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

        # Fallbacks e Limpeza
        if 'imagem_url' not in df.columns: df['imagem_url'] = ''
        df['imagem_url'] = df['imagem_url'].fillna('')
        
        if 'categoria' not in df.columns: df['categoria'] = 'Eletrônicos'
        df['categoria'] = df['categoria'].fillna('Eletrônicos')

        if 'descricao' not in df.columns: df['descricao'] = ''
        df['descricao'] = df['descricao'].fillna('')

        # Limpa espaços em branco extras nos nomes
        df['produto_base'] = df['produto_base'].str.strip()
        df['categoria'] = df['categoria'].str.strip()
        
        print(f"Sucesso! {len(df)} registros lidos do banco de dados.")
        return df
        
    except Exception as e:
        print(f"Erro ao conectar ou ler do banco de dados:")
        traceback.print_exc()
        return None

# Rota de teste
@app.route('/', methods=['GET'])
def home():
    return jsonify({"message": "API de Comparador de Produtos (v11.4 Final) está funcionando!"}), 200

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

# Rota de Listagem de Produtos
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
                # Pega apenas os dados mais recentes de cada loja
                df_lojas_recentes = group.loc[group.groupby('loja')['timestamp'].idxmax()]

                group_valido = df_lojas_recentes[df_lojas_recentes['preco'] > 0]
                
                if not group_valido.empty:
                    # Menor preço define a "capa" do produto
                    produto_principal = group_valido.loc[group_valido['preco'].idxmin()]
                else:
                    produto_principal = df_lojas_recentes.sort_values(by='timestamp', ascending=False).iloc[0]

                precos_historicos_validos = group[group['preco'] > 0]['preco']
                preco_min_historico = float(precos_historicos_validos.min()) if not precos_historicos_validos.empty else 0.0
                preco_medio_historico = float(precos_historicos_validos.mean()) if not precos_historicos_validos.empty else 0.0

                lojas = []
                for _, loja_info in df_lojas_recentes.iterrows():
                    lojas.append({
                        "name": loja_info['loja'],
                        "price": float(loja_info['preco']),
                        "originalPrice": None,
                        "shipping": "Consultar",
                        "rating": 0, "reviews": 0,
                        "affiliateLink": loja_info['url'],
                        "inStock": loja_info['preco'] > 0 and not pd.isna(loja_info['preco'])
                    })

                # Histórico simplificado para a listagem (pode ser vazio para economizar)
                historico_formatado = [] 

                produtos_formatados.append({
                    "id": str(nome_base), 
                    "name": produto_principal['nome_completo_raspado'],
                    "image": produto_principal['imagem_url'],
                    "category": produto_principal['categoria'], 
                    "stores": lojas,
                    "priceHistory": historico_formatado,
                    "precoMinimoHistorico": preco_min_historico, 
                    "precoMedioHistorico": preco_medio_historico
                    # Descrição não enviada na listagem para economizar banda
                })

            except Exception as e:
                print(f"Erro ao processar item '{nome_base}': {e}")
                continue
    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": "Erro interno ao processar produtos"}), 500

    return jsonify(produtos_formatados)


# Rota de Histórico Específico
@app.route('/api/products/<product_id>/history', methods=['GET'])
def get_product_history(product_id):
    product_id_limpo = product_id.strip()
    
    df_dados = get_dados_do_db()
    if df_dados is None or df_dados.empty:
        return jsonify({"error": "Dados não encontrados"}), 404
        
    df_produto = df_dados[df_dados['produto_base'] == product_id_limpo].copy()
    
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
# --- ROTA DE PRODUTO ÚNICO (V12 - Prioridade Visual CORRIGIDA) ---
@app.route('/api/product/<path:product_base_name>', methods=['GET'])
def get_single_product(product_base_name):
    product_name_limpo = product_base_name.strip()
    print(f"Buscando dados para: '{product_name_limpo}'")

    df_dados = get_dados_do_db()
    
    if df_dados is None or df_dados.empty:
        return jsonify({"error": "Erro no DB"}), 500

    group = df_dados[df_dados['produto_base'] == product_name_limpo].copy()

    if group.empty:
        return jsonify({"error": "Produto não encontrado"}), 404

    try:
        # 1. Recentes de cada loja (Variável unificada: df_recentes)
        df_recentes = group.loc[group.groupby('loja')['timestamp'].idxmax()]
        
        # 2. Define o Vencedor do Preço (Loja Principal)
        group_valido = df_recentes[df_recentes['preco'] > 0]
        if not group_valido.empty:
            produto_principal_row = group_valido.loc[group_valido['preco'].idxmin()]
        else:
            produto_principal_row = df_recentes.sort_values(by='timestamp', ascending=False).iloc[0]

        # 3. --- LÓGICA DE DESCRIÇÃO TURBINADA (V12) ---
        # Define a prioridade: Pichau > Terabyte > Vencedor > Kabum
        descricao_final = ""
        prioridade_lojas = ['Pichau', 'Terabyte'] 
        
        # A. Tenta as lojas com descrições visuais (Rich HTML)
        for loja_top in prioridade_lojas:
            try:
                # CORREÇÃO: Usando df_recentes aqui
                row = df_recentes[df_recentes['loja'] == loja_top]
                if not row.empty:
                    desc = row.iloc[0]['descricao']
                    # Verifica se tem conteúdo real (não só tags vazias)
                    if desc and len(str(desc).strip()) > 50: 
                        descricao_final = desc
                        print(f"  -> [Descrição] Usando versão rica da {loja_top}.")
                        break
            except: pass
            
        # B. Se não achou nas ricas, usa a do vencedor do preço
        if not descricao_final:
            desc_vencedor = produto_principal_row.get('descricao', '')
            if desc_vencedor and str(desc_vencedor).strip():
                descricao_final = desc_vencedor
                print(f"  -> [Descrição] Usando versão do vencedor ({produto_principal_row['loja']}).")

        # C. Fallback final para Kabum (se o vencedor não for Kabum e estiver sem desc)
        if not descricao_final:
             try:
                # CORREÇÃO: Usando df_recentes aqui também
                row_k = df_recentes[df_recentes['loja'] == 'Kabum']
                if not row_k.empty:
                    descricao_final = row_k.iloc[0]['descricao']
                    print("  -> [Descrição] Fallback final para Kabum.")
             except: pass
        # ------------------------------------------------

        # 4. Monta Lojas
        lojas = []
        for _, row in df_recentes.iterrows(): # CORREÇÃO: Usando df_recentes
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
        validos_hist = group[group['preco'] > 0]['preco']
        historico_df = group.sort_values('timestamp')[['timestamp', 'preco', 'loja']].drop_duplicates()
        historico_formatado = []
        for _, row in historico_df.iterrows():
            historico_formatado.append({
                "date": row['timestamp'].strftime('%Y-%m-%d'),
                "price": float(row['preco']),
                "loja": row['loja']
            })

        return jsonify({
            "id": str(product_name_limpo), 
            "name": produto_principal_row['nome_completo_raspado'],
            "image": produto_principal_row['imagem_url'],
            "category": produto_principal_row['categoria'],
            "stores": lojas,
            "priceHistory": historico_formatado,
            "precoMinimoHistorico": float(validos_hist.min()) if not validos_hist.empty else 0.0,
            "precoMedioHistorico": float(validos_hist.mean()) if not validos_hist.empty else 0.0,
            "descricao": descricao_final # <-- Agora com a melhor descrição disponível!
        })

    except Exception as e:
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500
if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    print(f"Rodando Flask localmente na porta {port}...")
    app.run(debug=True, host='0.0.0.0', port=port)