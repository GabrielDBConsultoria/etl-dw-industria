import pyodbc
import pandas as pd
import numpy as np

# Conexões utilizando DSN (confira que os DSNs "xodo" e "dw_xodo" estão configurados)
conn_erp = pyodbc.connect("DSN=xd")
conn_dw = pyodbc.connect("DSN=dw_xd")
cursor_dw = conn_dw.cursor()

print("\u2705 Conectado ao ERP e DW")

# Função auxiliar para converter valores numpy para tipos nativos do Python
def cast_value(val):
    if isinstance(val, (np.integer,)):
        return int(val)
    elif isinstance(val, (np.floating,)):
        return float(val)
    else:
        return val

# ------------------------------------------------
# FUNÇÃO AUXILIAR: Upsert para as DIMENSÕES
# ------------------------------------------------
def upsert_table_com_update(df, table_name, insert_query, pk_column, compare_columns):
    df = df.fillna("")
    cursor_dw.execute(f"SELECT * FROM {table_name}")
    columns = [column[0] for column in cursor_dw.description]
    existing_rows = cursor_dw.fetchall()
    existing_dict = {
        row[columns.index(pk_column)]: dict(zip(columns, row))
        for row in existing_rows
    }
    updated, inserted_count = 0, 0
    for _, row in df.iterrows():
        pk_value = cast_value(row[pk_column])
        if pk_value in existing_dict:
            has_changes = any(
                str(cast_value(row[col])) != str(existing_dict[pk_value].get(col, ""))
                for col in compare_columns
            )
            if has_changes:
                set_clause = ", ".join([f"{col} = ?" for col in compare_columns])
                update_query = f"UPDATE {table_name} SET {set_clause} WHERE {pk_column} = ?"
                update_values = [cast_value(row[col]) for col in compare_columns] + [pk_value]
                cursor_dw.execute(update_query, update_values)
                updated += 1
        else:
            cursor_dw.execute(insert_query, tuple(cast_value(row[col]) for col in [pk_column] + compare_columns))
            inserted_count += 1
    conn_dw.commit()
    print(f"✅ {inserted_count} inseridos e {updated} atualizados em {table_name}")

# ---------------------------
# CARGA DAS DIMENSÕES
# ---------------------------

def load_dim_filial():
    df = pd.read_sql("SELECT DISTINCT fil_codigo FROM filial", conn_erp)
    insert_q = "INSERT INTO dim_filial (fil_codigo) VALUES (?)"
    upsert_table_com_update(df, "dim_filial", insert_q, "fil_codigo", [])

def load_dim_atividade():
    df = pd.read_sql("SELECT DISTINCT atv_codigo, atv_desc FROM atividade", conn_erp)
    insert_q = "INSERT INTO dim_atividade (atv_codigo, atv_desc) VALUES (?, ?)"
    upsert_table_com_update(df, "dim_atividade", insert_q, "atv_codigo", ["atv_desc"])

def load_dim_pessoa():
    query = """
        SELECT pes_codigo, pes_razao, pes_fantasia, pes_atvcodigo,
               CASE WHEN pes_ativo IS NULL THEN 1 ELSE pes_ativo END AS pes_ativo
        FROM pessoa
    """
    df = pd.read_sql(query, conn_erp)
    insert_q = """
        INSERT INTO dim_pessoa (pes_codigo, pes_razao, pes_fantasia, pes_atvcodigo, pes_ativo)
        VALUES (?, ?, ?, ?, ?)
    """
    upsert_table_com_update(df, "dim_pessoa", insert_q, "pes_codigo",
                            ["pes_razao", "pes_fantasia", "pes_atvcodigo", "pes_ativo"])

def load_dim_colaborador():
    df = pd.read_sql("SELECT clb_codigo, clb_razao FROM colaborador", conn_erp)
    insert_q = "INSERT INTO dim_colaborador (clb_codigo, clb_razao) VALUES (?, ?)"
    upsert_table_com_update(df, "dim_colaborador", insert_q, "clb_codigo", ["clb_razao"])

def load_dim_motdevolucao():
    df = pd.read_sql("SELECT mtd_codigo, mtd_desc FROM motdevolucao", conn_erp)
    # Adiciona linha "SEM MOTIVO" com código 999999
    df_sem_motivo = pd.DataFrame([{'mtd_codigo': 999999, 'mtd_desc': 'SEM MOTIVO'}])
    df = pd.concat([df, df_sem_motivo], ignore_index=True).drop_duplicates(subset='mtd_codigo')
    insert_q = "INSERT INTO dim_motdevolucao (mtd_codigo, mtd_desc) VALUES (?, ?)"
    upsert_table_com_update(df, "dim_motdevolucao", insert_q, "mtd_codigo", ["mtd_desc"])

def load_dim_produto():
    query = """
        SELECT pro_codigo, pro_desc, pro_ativo
        FROM produto
        WHERE pro_grpcodigo = 1 OR pro_codigo IN (1801, 704, 1723)
    """
    df = pd.read_sql(query, conn_erp)
    insert_q = "INSERT INTO dim_produto (pro_codigo, pro_desc, pro_ativo) VALUES (?, ?, ?)"
    upsert_table_com_update(df, "dim_produto", insert_q, "pro_codigo", ["pro_desc", "pro_ativo"])

# ----------------------------------------
# CARGA DO FATO UNIFICADO: fato_vendasitens
# ----------------------------------------
def load_fato_vendasitens():
    """
    Carrega dados unificados a partir da tabela de itenspedido com join em pedidos
    e left join com movdevolucao para capturar o motivo, na tabela fato_vendasitens.
    
    O Id_ItemPedido é calculado pela concatenação de:
      ipv_filcodigo, ipv_spvcodigo, ipv_pednumero, ipv_procodigo
    Assim como na movdevolucao, o identificador é gerado com a mesma lógica e, adicionalmente,
    consideramos que ipv_numitem = mvd_numitem para o join.
    """
    # Recupera os Id_ItemPedido já inseridos no DW para evitar duplicação
    cursor_dw.execute("SELECT Id_ItemPedido FROM fato_vendasitens")
    ids_dw = {row[0] for row in cursor_dw.fetchall()}
    
    # Recupera os códigos válidos de motivo e produto da dimensão
    cursor_dw.execute("SELECT mtd_codigo FROM dim_motdevolucao")
    valid_mot_codes = {row[0] for row in cursor_dw.fetchall()}
    cursor_dw.execute("SELECT pro_codigo FROM dim_produto")
    valid_prod_codes = {row[0] for row in cursor_dw.fetchall()}
    
    # Consulta unificada
    query = """
    SELECT
        CONCAT(
            LPAD(ipv.ipv_filcodigo, 2, '0'),
            LPAD(ipv.ipv_spvcodigo, 3, '0'),
            LPAD(ipv.ipv_pednumero, 10, '0'),
            LPAD(ipv.ipv_procodigo, 11, '0')
        ) AS Id_ItemPedido,
        ipv.ipv_filcodigo AS ped_filcodigo,
        p.Ped_PesCodigo AS ped_pescodigo,
        p.Ped_VenCodigo AS ped_vencodigo,
        ipv.ipv_procodigo AS ipv_ProCodigo,
        COALESCE(mov.mvd_mtdcodigo, 999999) AS ipv_mtdcodigo,
        p.Ped_NatCodigo AS ped_natcodigo,
        p.Ped_Numero AS ped_numero,
        p.Ped_DtEmissao AS ped_dtemissao,
        p.Ped_DtEntrega AS ped_dtEntrega,
        ipv.ipv_quantidade AS ipv_Quantidade,
        ipv.ipv_propbruto AS ipv_propbruto,
        ipv.ipv_precovenda AS ipv_precovenda,
        ipv.ipv_valsubst AS ipv_valsubst
    FROM itenspedido ipv
    JOIN pedidos p ON
         ipv.ipv_filcodigo = p.Ped_FilCodigo AND
         ipv.ipv_spvcodigo = p.Ped_SpvCodigo AND
         ipv.ipv_pednumero = p.Ped_Numero
    LEFT JOIN (
         SELECT
              CONCAT(
                  LPAD(mvd_filcodigo, 2, '0'),
                  LPAD(mvd_spvcodigo, 3, '0'),
                  LPAD(mvd_numero, 10, '0'),
                  LPAD(mvd_procodigo, 11, '0')
              ) AS mvd_iditempedido,
              mvd_mtdcodigo,
              mvd_numitem
         FROM movdevolucao
    ) mov ON mov.mvd_iditempedido = CONCAT(
            LPAD(ipv.ipv_filcodigo, 2, '0'),
            LPAD(ipv.ipv_spvcodigo, 3, '0'),
            LPAD(ipv.ipv_pednumero, 10, '0'),
            LPAD(ipv.ipv_procodigo, 11, '0')
    )
    AND ipv.ipv_numitem = mov.mvd_numitem
    WHERE YEAR(p.Ped_DtEmissao) >= 2025
      AND p.Ped_StpCodigo = 6
      AND p.Ped_NatCodigo IN ('VE','VTE','VIN','VTI','DEV','DTR','DV')
    ORDER BY ipv.ipv_filcodigo, ipv.ipv_pednumero, ipv.ipv_procodigo
    """
    
    # Ler os dados em chunks para evitar sobrecarga de memória
    for chunk in pd.read_sql(query, conn_erp, chunksize=5000):
        # Filtra registros já processados
        chunk = chunk[~chunk['Id_ItemPedido'].isin(ids_dw)]
        # Filtra registros cujo código de produto exista na dimensão
        chunk = chunk[chunk['ipv_ProCodigo'].isin(valid_prod_codes)]
        
        insert_q = """
            INSERT INTO fato_vendasitens (
                Id_ItemPedido,
                ped_filcodigo,
                ped_pescodigo,
                ped_vencodigo,
                ipv_ProCodigo,
                ipv_mtdcodigo,
                ped_natcodigo,
                ped_numero,
                ped_dtemissao,
                ped_dtEntrega,
                ipv_Quantidade,
                ipv_propbruto,
                ipv_precovenda,
                ipv_valsubst
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        for _, row in chunk.iterrows():
            # Verifica se o código do motivo é válido; se não, força para 999999
            mtdcode = cast_value(row.ipv_mtdcodigo)
            if mtdcode not in valid_mot_codes:
                mtdcode = 999999
            cursor_dw.execute("DELETE FROM fato_vendasitens WHERE Id_ItemPedido = ?", cast_value(row.Id_ItemPedido))
            cursor_dw.execute(insert_q, (
                cast_value(row.Id_ItemPedido),
                cast_value(row.ped_filcodigo),
                cast_value(row.ped_pescodigo),
                cast_value(row.ped_vencodigo),
                cast_value(row.ipv_ProCodigo),
                mtdcode,
                row.ped_natcodigo,
                cast_value(row.ped_numero),
                row.ped_dtemissao,
                row.ped_dtEntrega,
                row.ipv_Quantidade or 0,
                row.ipv_propbruto or 0,
                row.ipv_precovenda or 0,
                row.ipv_valsubst or 0
            ))
        conn_dw.commit()
        print(f"fato_vendasitens: chunk de {len(chunk)} registros inserido")
        for id_item in chunk['Id_ItemPedido']:
            ids_dw.add(id_item)

# ---------------------------
# EXECUÇÃO FINAL DO ETL
# ---------------------------
def main():
    # Carrega as dimensões
    load_dim_filial()
    load_dim_atividade()
    load_dim_pessoa()
    load_dim_colaborador()
    load_dim_motdevolucao()
    load_dim_produto()
    
    # Carrega o fato unificado
    load_fato_vendasitens()
    
    print("✅ ETL concluído com sucesso!")

if __name__ == "__main__":
    main()
