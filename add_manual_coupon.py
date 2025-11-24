import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Carrega variáveis de ambiente
load_dotenv()

def adicionar_cupom_manual():
    DATABASE_URL = os.environ.get('DATABASE_URL')
    
    if not DATABASE_URL:
        print("ERRO: DATABASE_URL não configurada no .env")
        return

    # Correção para o Render (postgres:// -> postgresql://)
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    try:
        engine = create_engine(DATABASE_URL)
        
        # Dados do Cupom (Edite aqui se quiser mudar)
        cupom = {
            "codigo": "TESTE10",
            "descricao": "Cupom de teste manual adicionado pelo Admin (10% OFF)",
            "validade": "31/12/2025",
            "loja": "Kabum",
            "link": "https://www.kabum.com.br"
        }

        print(f"Tentando inserir cupom: {cupom['codigo']}...")

        with engine.connect() as conn:
            # Query SQL de inserção
            query = text("""
                INSERT INTO cupons (codigo, descricao, validade, loja, link, timestamp)
                VALUES (:codigo, :descricao, :validade, :loja, :link, NOW())
            """)
            
            conn.execute(query, cupom)
            conn.commit()
            
        print("✅ SUCESSO! Cupom adicionado manualmente.")
        print("Agora atualize a página de Cupons no seu site.")

    except Exception as e:
        print(f"❌ Erro ao inserir: {e}")

if __name__ == "__main__":
    adicionar_cupom_manual()