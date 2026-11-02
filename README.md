# 🚀 Pipeline de Detecção de Fraudes em Tempo Real

Este projeto consiste em um pipeline robusto de **Event Streaming** e **Stream Processing** para detecção contínua de comportamento financeiro suspeito em tempo real. O fluxo captura alterações em um banco de dados relacional via **Change Data Capture (CDC)**, transmite os eventos através do **Confluent Cloud Kafka** e processa agregados temporais no **Flink SQL**.

---

## 🛠️ Arquitetura e Decisões Técnicas

[ PostgreSQL (Neon) ] ──(CDC / Debezium)──> [ Confluent Cloud Kafka ] ──> [ Flink SQL Engine ] ──> [ Materialized Table / Alertas ]

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
💡 Comportamento do Flink SQL Workspace
Modo Interativo (SELECT): Utilizado durante o desenvolvimento e os testes dinâmicos para visualização dos blocos e janelas de tempo em tempo real no console do Workspace.

Modo Persistente (CREATE TABLE ... AS SELECT): Ao criar uma Materialized Table, o Flink passa a rodar um job de streaming contínuo em segundo plano (background) persistindo os resultados em um novo tópico Kafka de saída, sem atualizar a grade de pré-visualização imediata da célula.

📁 Estrutura do Repositório
Plaintext
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
🧪 Como Executar o Projeto Localmente
Clonar o Repositório:

Bash
git clone [https://github.com/ElvisLouro/pipeline-fraude-confluent-flink.git](https://github.com/ElvisLouro/pipeline-fraude-confluent-flink.git)
cd pipeline-fraude-confluent-flink
Configurar as Variáveis de Ambiente:
Crie um arquivo .env na raiz do projeto contendo suas credenciais (este arquivo é ignorado pelo Git para segurança):

Ini, TOML
DB_URI="postgres://usuario:senha@host-neon.l.aivencloud.com:12559/defaultdb?sslmode=require"
Instalar Dependências:

Bash
pip install -r requisitos.txt
Simular Transações:
Execute o script Python para injetar um lote de eventos no Neon e disparar o fluxo do CDC:

Bash
python inserir_transacao_teste.py
Monitorar no Flink SQL:
Acompanhe o processamento dinâmico das janelas diretamente no editor do Confluent Cloud Workspace.


2. No **VS Code**, coloque o cursor na linha 8 (abaixo de `## 🛠️ Arquitetura e Decisões Técnicas`).
3. Aperte **`Ctrl + V`** apenas uma vez.
4. Salve com **`Ctrl + S`**.

*Verificação:* O VS Code vai colar todo o resto instantaneamente de uma vez só! Depois disso é só fazer o commit e push pelo GitHub Desktop.

<FollowUp label="Quer conferir como ficou a página no GitHub após colar tudo e fazer o push?" query="Colei tudo de uma vez no VS Code, salvei e fiz o Push pelo GitHub Desktop. Como confirmo se a página no GitHub ficou completa?"/>
```
