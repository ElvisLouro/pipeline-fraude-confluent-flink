import os
import random
import time
import uuid
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def gerar_transacoes_limitadas():
    conn = psycopg2.connect(os.getenv("DATABASE_URL"))
    cur = conn.cursor()

    print("[INFO] Gerando lote seguro de 10 transações para teste...")

    for i in range(1, 11):
        transacao_id = f"TRX-{uuid.uuid4().hex[:8]}"
        cartao_id = "CARD-9988"
        conta_id = "ACC-003"

        # Garante a geracao de transacoes Normais, Amarelas e Vermelhas
        if i in [3, 7]:
            valor = round(random.uniform(3200.0, 6800.0), 2)  # Alerta Amarelo
        elif i in [5, 9]:
            valor = round(random.uniform(7200.0, 12000.0),
                          2)  # Alerta Vermelho
        else:
            valor = round(random.uniform(100.0, 2500.0), 2)   # Normal

        timestamp_transacao = datetime.now()

        query = """
            INSERT INTO transacoes (transacao_id, cartao_id, conta_id, valor, timestamp_transacao)
            VALUES (%s, %s, %s, %s, %s);
        """

        cur.execute(query, (transacao_id, cartao_id,
                    conta_id, valor, timestamp_transacao))
        conn.commit()

        print(f"[{i}/10] Transação {transacao_id} enviada -> R$ {valor:.2f}")
        time.sleep(1)  # Intervalo de 1 segundo entre envios

    cur.close()
    conn.close()
    print("[SUCESSO] Lote de 10 transações concluído com segurança!")


if __name__ == "__main__":
    gerar_transacoes_limitadas()
