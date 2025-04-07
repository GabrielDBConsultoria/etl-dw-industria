# 🗂️ Modelo Físico de Dados – DW Xodó

Esta pasta contém a estrutura física do Data Warehouse **dw_xodo**, incluindo o script SQL de criação das tabelas.

## 📄 Arquivo disponível

- `script_modelo_dados_devolucao.sql`: script completo para criação das tabelas de **dimensões** e **fatos**, com definição de:
  - Chaves primárias (`PRIMARY KEY`)
  - Chaves estrangeiras (`FOREIGN KEY`)
  - Tipos de dados e relacionamentos

## 🧱 Finalidade

O objetivo desta estrutura é servir como base para o processo de ETL desenvolvido em Python, garantindo consistência, rastreabilidade e desempenho para análises de dados.

## 📝 Observações

- As tabelas **dimensão** são completamente substituídas a cada carga.
- As tabelas **fato** são atualizadas de forma **incremental**, com lógica de verificação por `Id`.
- A modelagem segue o padrão dimensional (Kimball), com separação clara entre dimensões e fatos.

---

📌 Este modelo físico é parte do projeto de construção de um Data Warehouse voltado à análise de devoluções e vendas, desenvolvido por [Gabriel Delucca Barros](mailto:gabrieldbarrosconsultoria@gmail.com).
