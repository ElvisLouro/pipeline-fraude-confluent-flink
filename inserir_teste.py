import os
import psycopg2
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

# Conecta ao PostgreSQL no Neon usando a DATABASE_URL
conn = psycopg2.connect(os.getenv("DATABASE_URL"))
cur = conn.cursor()

# Insere um novo registro de teste na tabela contas
cur.execute(
    "INSERT INTO contas (conta_id, cliente_id, nome_cliente) VALUES (%s, %s, %s);",
    ('ACC-003', 'CLI-103', 'Carlos Eduardo')
)

conn.commit()
print("[SUCESSO] Registro inserido no Neon com sucesso!")

cur.close()
conn.close()
