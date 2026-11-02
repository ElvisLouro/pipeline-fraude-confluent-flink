import base64
import json
import os
import struct
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

conf = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP_SERVERS'),
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': os.getenv('KAFKA_API_KEY'),
    'sasl.password': os.getenv('KAFKA_API_SECRET'),
    'group.id': 'grupo-fraude-python',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['postgres.public.transacoes', 'postgres.public.contas'])

print("[INFO] Sistema de Detecção de Fraudes Ativo... Processando eventos do tópico.")


def converter_valor(valor_raw):
    """Converte o valor numérico enviado pelo Debezium (float, int ou Base64)."""
    if isinstance(valor_raw, (int, float)):
        return float(valor_raw)

    if isinstance(valor_raw, str):
        try:
            # Tenta conversao direta se vier como string numerica ("4122.39")
            return float(valor_raw)
        except ValueError:
            # Se for Base64 enviado pelo Debezium para campos Decimal/Numeric
            raw_bytes = base64.b64decode(valor_raw)
            # Decodifica os bytes para inteiro e ajusta a escala de 2 casas decimais
            num = int.from_bytes(raw_bytes, byteorder='big', signed=True)
            return num / 100.0

    return 0.0


try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"[ERRO] {msg.error()}")
                break

        valor_payload = msg.value().decode('utf-8') if msg.value() else None

        if valor_payload:
            dados = json.loads(valor_payload)
            payload_after = dados.get('payload', {}).get(
                'after') if 'payload' in dados else dados.get('after')

            if payload_after and 'valor' in payload_after:
                valor_raw = payload_after.get('valor')
                valor = converter_valor(valor_raw)
                conta_id = payload_after.get('conta_id')
                trx_id = payload_after.get('transacao_id')

                print(
                    f"\n[NOVA TRANSAÇÃO] ID: {trx_id} | Conta: {conta_id} | Valor: R$ {valor:.2f}")

                # REGRAS DE NEGÓCIO PARA FRAUDE
                if valor > 7000.00:
                    print(
                        f"🚨 [ALERTA VERMELHO - SUSPEITA DE FRAUDE] Transação {trx_id} excede R$ 7.000,00! Valor: R$ {valor:.2f}")
                elif valor > 3000.00:
                    print(
                        f"⚠️ [ALERTA AMARELO - VALOR ALTO] Transação {trx_id} acima de R$ 3.000,00. Valor: R$ {valor:.2f}")
                else:
                    print(f"✅ [TRANSAÇÃO NORMAL] Processada com sucesso.")

except KeyboardInterrupt:
    print("\n[INFO] Encerrando consumidor...")
finally:
    consumer.close()
