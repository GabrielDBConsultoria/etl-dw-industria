# Documentação ETL - DW 

## ✨ Objetivo
Este processo ETL tem como objetivo extrair dados do ERP (base OLTP) e carregá-los de forma estruturada em um Data Warehouse MySQL chamado `dw_xd`, separando corretamente as tabelas de dimensão e de fatos para análises posteriores.

## 📂 Estrutura Geral

### Dimensões (atualizadas completamente a cada execução):
- `dim_filial`
- `dim_atividade`
- `dim_pessoa`
- `dim_colaborador`
- `dim_motdevolucao`
- `dim_produto`

### Fatos (atualização incremental):
- `fato_pedidos`
- `fato_itenspedido`

## ⚙️ Lógica das Dimensões
Todas as dimensões são atualizadas via `DELETE` total antes da inserção de novos dados. Isso garante sincronismo sem necessidade de controle incremental.

### Destaques:
- `dim_motdevolucao` inclui o motivo extra `999999 - SEM MOTIVO`, usado como fallback para registros sem correspondência.
- `dim_produto` considera produtos com `pro_grpcodigo = 1` **ou** códigos específicos `1801, 704, 1723`.

## 📊 Fato: `fato_pedidos`
- Chave: `Id_Pedido` gerado concatenando: `filcodigo` + `spvcodigo` + `numero`
- Atualização incremental: apenas novos registros são inseridos.
- Se o `Id_Pedido` já existir, é deletado antes da reinserção.
- Filtros:
  - `YEAR(ped_dtemissao) >= 2025`
  - `ped_stpcodigo = 6`
  - `ped_natcodigo IN ('VE','VTE','VIN','VTI','DEV','DTR','DV')`

## 📊 Fato: `fato_itenspedido`
- Chave: `Id_ItemPedido` gerado com: `Id_Pedido` + `ipv_numitem`
- Join com `movdevolucao` para capturar o `mtd_codigo`
- Se não houver motivo de devolução, assume-se `999999`
- Atualização incremental com `DELETE` por `Id_ItemPedido`
- Filtros seguem os mesmos de `fato_pedidos`
- Campos `ipv_filcodigo` e `ipv_spvcodigo` foram removidos pois já estão contemplados na chave `Id_Pedido` e presentes na tabela `fato_pedidos`

## ✅ Considerações
- Utiliza `pyodbc` e `pandas` para leitura dos dados em chunks (5000 registros por vez)
- Todos os comandos são `DELETE` + `INSERT` para manter a consistência
- A execução final é feita com a chamada sequencial das funções

## 🔎 Verificações de Integridade
Ao final da carga de `fato_itenspedido`, é feita uma verificação cruzada para mostrar quais motivos de devolução foram utilizados e quantas vezes.

## 📖 Sugestões de Expansão
- Criar tabelas de auditoria (log de execução do ETL)
- Automatizar execução com agendador ou Airflow
- Implementar flag de atualização nas dimensões para otimizar cargas futuras

---

Caso precise converter para PDF ou HTML para documentação oficial, o formato Markdown é compatível com diversas ferramentas como VSCode, Typora, Obsidian e o próprio GitHub.
