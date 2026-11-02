-- Criando a tabela de contas de clientes
CREATE TABLE IF NOT EXISTS contas (
    conta_id VARCHAR(50) PRIMARY KEY,
    cliente_id VARCHAR(50) NOT NULL,
    nome_cliente VARCHAR(100) NOT NULL,
    criado_em TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-- Criando a tabela de transações
CREATE TABLE IF NOT EXISTS transacoes (
    transacao_id VARCHAR(50) PRIMARY KEY,
    cartao_id VARCHAR(50) NOT NULL,
    conta_id VARCHAR(50) NOT NULL,
    valor DECIMAL(10, 2) NOT NULL,
    timestamp_transacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_conta FOREIGN KEY (conta_id) REFERENCES contas(conta_id)
);
-- Inserindo dados sintéticos iniciais de teste
INSERT INTO contas (conta_id, cliente_id, nome_cliente)
VALUES ('ACC-001', 'CLI-101', 'Elvis Louro'),
    ('ACC-002', 'CLI-102', 'Maria Silva');