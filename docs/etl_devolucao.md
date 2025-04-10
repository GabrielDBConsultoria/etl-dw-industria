# Documentação ETL - DW (Modelo Unificado)

✨ **Objetivo**  
Este processo ETL extrai dados do ERP (base OLTP) e os carrega de forma estruturada em um Data Warehouse MySQL chamado **dw_xodo**. A principal mudança neste projeto é a unificação das tabelas de fato em uma única tabela, **fato_vendasitens**, que reúne os dados do cabeçalho (pedido) e dos itens para análises posteriores.

---

📂 **Estrutura Geral**

**Dimensões** (atualizadas completamente a cada execução):
- **dim_filial**
- **dim_atividade**
- **dim_pessoa**
- **dim_colaborador**
- **dim_motdevolucao**
- **dim_produto** (inclui o campo **pro_ativo**, onde 1 = ativo e 0 = inativo)

**Fato** (atualização incremental):
- **fato_vendasitens** – tabela unificada que reúne os dados dos pedidos e dos itens.

---

⚙️ **Lógica das Dimensões**  
- Todas as dimensões continuam sendo atualizadas via DELETE total antes da inserção de novos dados.  
- Isso garante o sincronismo entre o DW e o ERP sem a necessidade de controles incrementais complexos.

**Destaques:**
- **dim_motdevolucao** inclui o motivo extra 999999 – "SEM MOTIVO" como fallback.
- **dim_produto** carrega apenas produtos com `pro_grpcodigo = 1` ou produtos com códigos específicos (ex.: 1801, 704, 1723) e inclui o campo **pro_ativo**.

---

📊 **Fato: fato_vendasitens (Modelo Unificado)**

**Chave Primária:**  
- **Id_ItemPedido** – Gerada através da concatenação dos seguintes campos da tabela *itenspedido*:  
  - **ipv_filcodigo** (formato: 2 dígitos)  
  - **ipv_spvcodigo** (formato: 3 dígitos)  
  - **ipv_pednumero** (formato: 10 dígitos)  
  - **ipv_procodigo** (formato: 11 dígitos)  
  *Exemplo:* `LPAD(ipv_filcodigo,2,'0') & LPAD(ipv_spvcodigo,3,'0') & LPAD(ipv_pednumero,10,'0') & LPAD(ipv_procodigo,11,'0')`

**Outros Campos Importantes:**

*Do Cabeçalho (Pedido):*
- **ped_filcodigo** – Código da filial.
- **ped_pescodigo** – Código do cliente.
- **ped_vencodigo** – Código do vendedor.
- **ped_natcodigo** – Natureza do pedido.
- **ped_numero** – Número do pedido.
- **ped_dtemissao** – Data de emissão.
- **ped_dtEntrega** – Data de entrega.

*Do Item:*
- **ipv_ProCodigo** – Código do produto.
- **ipv_mtdcodigo** – Código do motivo de devolução (caso o item tenha devolução); se não houver, assume-se 999999.
- **ipv_Quantidade** – Quantidade do item.
- **ipv_propbruto** – Peso bruto (kg) do item.
- **ipv_precovenda** – Preço de venda.
- **ipv_valsubst** – Valor substituto (se houver).

**Join com Movdevolucao:**  
- A carga do fato inclui um LEFT JOIN com a tabela **movdevolucao** para capturar o motivo de devolução.  
- A lógica de concatenação para gerar o identificador de join na tabela **movdevolucao** é a mesma utilizada para **Id_ItemPedido**, utilizando os campos:
  - **mvd_filcodigo** (2 dígitos),
  - **mvd_spvcodigo** (3 dígitos),
  - **mvd_numero** (10 dígitos),
  - **mvd_procodigo** (11 dígitos).  
- **Adicionalmente,** consideramos que `ipv_numitem = mvd_numitem` para garantir a correspondência exata entre o item registrado e o motivo de devolução.

**Atualização Incremental:**  
- O processo de carga na tabela **fato_vendasitens** é incremental, onde para cada registro novo identificado é feito um DELETE (caso exista) seguido do INSERT, garantindo a consistência dos dados.

**Filtros Aplicados na Carga:**  
- Ano de emissão: `YEAR(Ped_DtEmissao) >= 2025`
- Status do pedido: `Ped_StpCodigo = 6`
- Natureza do pedido: `Ped_NatCodigo IN ('VE','VTE','VIN','VTI','DEV','DTR','DV')`

---

✅ **Considerações Técnicas**  
- O ETL utiliza `pyodbc` e `pandas` para a extração dos dados em chunks (5000 registros por vez), evitando sobrecarga de memória.
- Todos os comandos de inserção são executados no padrão DELETE + INSERT para manter a integridade e a consistência dos dados.
- O script ETL está estruturado para ser executado de forma sequencial, começando pelo carregamento das dimensões e depois efetuando a carga do fato unificado.
- Verificações de integridade são realizadas para assegurar que os registros inseridos possuem os valores corretos e que os joins (especialmente com a tabela **movdevolucao**) funcionem de maneira satisfatória.

---

🔎 **Verificações de Integridade**  
- Após a carga do fato, recomenda-se validar a consistência dos dados; por exemplo, verificar se os registros de motivos de devolução estão corretos (contar quantas vezes cada **mtd_codigo** é utilizado).

---

📖 **Sugestões de Expansão**  
- Criar tabelas de auditoria para registrar log de execução do ETL.
- Automatizar a execução do ETL com agendadores ou ferramentas como Airflow.
- Implementar flags de atualização incremental nas dimensões para otimizar as cargas futuras.

---

Esse documento resume o novo processo ETL, as mudanças implementadas para unificar as tabelas de fato e os detalhes importantes para a manutenção e expansão do Data Warehouse.
