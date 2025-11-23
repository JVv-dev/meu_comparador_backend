import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

print("Carregando .env...")
load_dotenv()

DATABASE_URL = os.environ.get('DATABASE_URL')

if not DATABASE_URL:
    print("ERRO: DATABASE_URL não encontrada.")
else:
    try:
        if DATABASE_URL.startswith("postgres://"):
            DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        
        engine = create_engine(DATABASE_URL)
        
        with engine.connect() as conn:
            print("Criando tabela 'cupons'...")
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS cupons (
                    id SERIAL PRIMARY KEY,
                    codigo VARCHAR(100),
                    descricao TEXT,
                    validade VARCHAR(100),
                    loja VARCHAR(50),
                    link TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """))
            conn.commit()
        
        print("Sucesso! Tabela 'cupons' criada.")

    except Exception as e:
        print(f"Erro: {e}")