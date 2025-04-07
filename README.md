# .gitignore
__pycache__/
*.pyc
.env
.DS_Store
.ipynb_checkpoints
venv/
.idea/

# requirements.txt
pandas
pyodbc

# README.md
# ETL DW Xodó

Este repositório contém o processo completo de ETL para o Data Warehouse da empresa Xodó de Minas. 

## 📄 Sobre o Projeto
O objetivo é extrair dados do ERP relacional (MySQL 5.6 via ODBC), transformar os dados com regras de negócio, e carregar em um DW estruturado (MySQL 8). As tabelas estão divididas entre dimensões e fatos.

## 📊 Estrutura do Projeto

```
etl-dw-xodo/
├── README.md
├── requirements.txt
├── .gitignore
├── etl_dw_xodo.py               # Script principal do ETL
├── /sql
│   └── ddl_alter_tables.sql   # Scripts SQL para ajuste nas tabelas do DW
└── /doc
    └── etl_dw_documentacao.md # Documentação completa em Markdown
```

## ⚙️ Como executar
1. Crie os DSNs ODBC `xodo` (ERP) e `dw_xodo` (DW)
2. Instale as dependências:
```bash
pip install -r requirements.txt
```
3. Execute o script principal:
```bash
python etl_dw_xodo.py
```

## 🔍 O que o ETL faz?
- Atualiza todas as dimensões com `DELETE + INSERT`
- Atualiza apenas registros novos nas tabelas fato
- Gera chaves como `Id_Pedido` e `Id_ItemPedido`
- Une as tabelas de devolução para compor a dimensão de motivos

## 🔹 Checklist de tabelas

### Dimensões:
- [x] `dim_filial`
- [x] `dim_atividade`
- [x] `dim_pessoa`
- [x] `dim_colaborador`
- [x] `dim_motdevolucao`
- [x] `dim_produto`

### Fatos:
- [x] `fato_pedidos`
- [x] `fato_itenspedido`

## 📖 Documentação
A documentação completa do projeto está no arquivo [`doc/etl_dw_documentacao.md`](doc/etl_dw_documentacao.md).

## 🚀 Futuras melhorias
- Automatização com Apache Airflow ou agendador
- Criação de tabela de logs de execução
- Validação e teste de integridade com pytest

---

Desenvolvido com ❤️ para a Xodó de Minas por Gabriel com apoio do ChatGPT.
