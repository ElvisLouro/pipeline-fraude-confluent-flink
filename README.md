# 🚀 Pipeline de Detecção de Fraudes em Tempo Real

Este projeto consiste em um pipeline robusto de **Event Streaming** e **Stream Processing** para detecção contínua de comportamento financeiro suspeito em tempo real. O fluxo captura alterações em um banco de dados relacional via **Change Data Capture (CDC)**, transmite os eventos através do **Confluent Cloud Kafka** e processa agregados temporais no **Flink SQL**.

---

## 🛠️ Arquitetura e Decisões Técnicas

1. **Banco de Dados Origem (PostgreSQL Serverless no Neon):**
   - **Compatibilidade Nativa:** Opera exatamente como um PostgreSQL padrão, garantindo total suporte a instruções SQL e conectores CDC.
   - **Conectividade Cloud-to-Cloud:** Escolha estratégica de infraestrutura para permitir acesso via TLS/SSL seguro diretamente pelo Confluent Cloud, eliminando a necessidade de túneis locais (como Ngrok).
2. **Ingestão CDC (Debezium / Confluent Connector):**
   - Escuta mutações (`INSERT`) na tabela `public.transacoes` e publica mensagens em formato de stream binário/JSON no Kafka.
3. **Event Streaming (Confluent Cloud Kafka):**
   - Tópico `postgres.public.transacoes` responsável por manter o log de eventos imutável e distribuído.
4. **Stream Processing (Flink SQL):**
   - Agregação contínua em tempo real utilizando a sintaxe moderna **Table-Valued Functions (TVF)** com **Tumbling Windows** de 1 minuto.

---

## ⚙️ Regra de Negócio e Flink SQL

O motor Flink realiza a conversão dos dados binários para caractere (`CAST`), extrai os atributos internos do evento JSON (`JSON_VALUE`) e identifica contas que atingem ou ultrapassam **2 transações dentro do mesmo intervalo de 1 minuto**:

```sql
SELECT
    window_start AS inicio_janela,
    window_end AS fim_janela,
    JSON_VALUE(CAST(val AS STRING), '$.after.conta_id') AS id_conta,
    COUNT(*) AS total_transacoes
FROM TABLE(
    TUMBLE(
        TABLE `default`.`desafio-basic`.`postgres.public.transacoes`,
        DESCRIPTOR(`$rowtime`),
        INTERVAL '1' MINUTE
    )
)
GROUP BY
    window_start,
    window_end,
    JSON_VALUE(CAST(val AS STRING), '$.after.conta_id')
HAVING COUNT(*) >= 2;

pipeline-fraude-confluent-flink/
├── .gitignore                  # Arquivos desconsiderados pelo Git (ex: .env)
├── README.md                   # Documentação oficial do repositório
├── requisitos.txt              # Dependências das bibliotecas Python
├── consumidor_fraudes.py       # Script consumidor para alertas do Kafka
├── docker-compose.yml          # Definição de containers locais de suporte
├── inserir_transacao_teste.py  # Gerador automatizado de eventos para testes
├── limpar_banco.py             # Script de apoio para reset da base no PostgreSQL
└── SQL/
    ├── 01_schema_postgres.sql         # DDL inicial das tabelas relacionais
    └── 02_alertas_fraude_tumble.sql   # Query de agregação temporal do Flink SQL

git clone [https://github.com/ElvisLouro/pipeline-fraude-confluent-flink.git](https://github.com/ElvisLouro/pipeline-fraude-confluent-flink.git)
cd pipeline-fraude-confluent-flink

DB_URI="postgres://usuario:senha@host-neon.l.aivencloud.com:12559/defaultdb?sslmode=require"

pip install -r requisitos.txt

python inserir_transacao_teste.py

---

<Steps>
  <Step subtitle="Completar o arquivo" title="Colar o texto no VS Code">
    Clique na linha 8 no VS Code (logo abaixo de `## 🛠️ Arquitetura e Decisões Técnicas`) e cole[cite: 12]. Pressione `Ctrl + S` para salvar.
    *Verificação:* O número de linhas do arquivo vai subir de 7 para aproximadamente 100 no VS Code[cite: 12].
  </Step>

  <Step subtitle="Enviar para o GitHub" title="Fazer Push no GitHub Desktop">
    Abra o GitHub Desktop, faça o **Commit to main** da alteração e clique em **Push origin**.
    *Verificação:* O botão no topo do GitHub Desktop mudará para *Fetch origin*.
  </Step>

  <Step subtitle="Validação visual" title="Conferir no Navegador">
    Atualize a página do GitHub no navegador (`F5`).
    *Verificação:* Toda a documentação e blocos de código aparecerão completamente renderizados na página.
  </Step>
</Steps>

<ElicitationsGroup message="Se precisar de ajuda com a atualização:">
  <Elicitation label="Subiu tudo no GitHub" query="Colei o texto restante no VS Code, salvei e fiz o push pelo GitHub Desktop. Deu certo no site?"/>
</ElicitationsGroup>

```
