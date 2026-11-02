import os
import psycopg2
from dotenv import load_dotenv

# Carrega as variáveis do arquivo .env
load_dotenv()

DB_URI = os.getenv("DB_URI")

try:
    print("[INFO] Conectando ao PostgreSQL no Aiven...")
    conn = psycopg2.connect(DB_URI)
    conn.autocommit = True
    cursor = conn.cursor()

    print("[INFO] Forçando sessão para modo de escrita...")
    cursor.execute("SET TRANSACTION READ WRITE;")
    cursor.execute("SET default_transaction_read_only = off;")

    print("[INFO] Executando TRUNCATE...")
    cursor.execute("TRUNCATE TABLE pagamentos;")

    print("[SUCESSO] Tabela esvaziada com sucesso!")

    cursor.close()
    conn.close()

except Exception as e:
    print(f"[ERRO] Falha ao executar o comando: {e}")
