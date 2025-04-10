# ETL DW XD

## 🇧🇷 Leia em Português

Este repositório contém o processo completo de ETL para o Data Warehouse da empresa XD, construído com MySQL. O script extrai dados do ERP relacional via ODBC, realiza transformações com regras de negócio e carrega os dados em tabelas de dimensões e fatos.

---

## 📌 Sobre o Projeto

📌 **Objetivo:**
- Extrair dados do ERP relacional (MySQL 5.6 via ODBC)
- Transformar dados com regras de negócio
- Carregar em DW estruturado (MySQL 8)

📌 **Tecnologias:**
- Python (pandas, pyodbc)
- MySQL 5.6 (ERP)

📌 **Tabelas criadas:**
### 🔹 Dimensões (atualizadas completamente)
- `dim_filial`
- `dim_atividade`
- `dim_pessoa`
- `dim_colaborador`
- `dim_motdevolucao`
- `dim_produto`

### 🔸 Fato (atualização incremental)
- `fato_vendasitens`
`

---

## 🧠 Regras de Negócio

-  Inclui apenas pedidos a partir de 2025, status 6 e determinadas naturezas de operação
- Relaciona itens de pedido com possíveis devoluções através do código de motivo (`mtd_codigo`)
- Caso o item não possua motivo, será atribuído `999999 - SEM MOTIVO`

---

## ▶️ Execução

1. Instale os pacotes:
```bash
pip install -r requirements.txt
```

2. Execute o script Python (necessário ter DSN configurado para `xd` e `dw_xd`):
```bash
python etl_dw_industria.py
```

---

## 🇺🇸 English Version

This repository contains the complete ETL process for the **XD* company's Data Warehouse, built with MySQL. The script extracts data from the ERP system via ODBC, applies business rules, and loads it into a structured Data Warehouse.



### 📌 Project Overview
- Extract data from relational ERP (MySQL 5.6 via ODBC)
- Transform with business rules
- Load into structured DW (MySQL 8)

### 📌 Technologies
- Python (pandas, pyodbc)
- MySQL 5.6 (ERP) and MySQL 8 (DW)

### 🔹 Dimensions (fully refreshed)
- `dim_filial`
- `dim_atividade`
- `dim_pessoa`
- `dim_colaborador`
- `dim_motdevolucao`
- `dim_produto`

### 🔸 Fact (incremental load)
- `fato_vendasitens`


---

## 🧠 Business Rules
- Only orders from 2025 onwards with status = 6 and specific nature codes 
- Link Order items to return reasons (motives)
- If no reason is found, `999999 - SEM MOTIVO` is assigned

---

## ▶️ How to Run
```bash
pip install -r requirements.txt
python etl_dw_inustria.py
```

---

Mantenedor: [@GabrielDBConsultoria](https://github.com/GabrielDBConsultoria) 💼
