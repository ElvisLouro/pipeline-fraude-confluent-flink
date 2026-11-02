# 🚀 Pipeline de Detecção de Fraudes em Tempo Real

Este projeto consiste em um pipeline robusto de **Event Streaming** e **Stream Processing** para detecção contínua de comportamento financeiro suspeito em tempo real. O fluxo captura alterações em um banco de dados relacional via **Change Data Capture (CDC)**, transmite os eventos através do **Confluent Cloud Kafka** e processa agregados temporais no **Flink SQL**.

---

## 🛠️ Arquitetura e Decisões Técnicas