
import pyodbc
import pandas as pd

# Conexões
conn_erp = pyodbc.connect("DSN=xodo")
conn_dw = pyodbc.connect("DSN=dw_xodo")
cursor_dw = conn_dw.cursor()

print("✅ Conectado ao ERP e DW")

# Função auxiliar
def truncate_and_insert(df, table_name, insert_query):
    cursor_dw.execute(f"DELETE FROM {table_name}")  # Substitui TRUNCATE por DELETE
    for _, row in df.iterrows():
        cursor_dw.execute(insert_query, tuple(row))
    conn_dw.commit()
    print(f"✅ {len(df)} registros inseridos em {table_name}")

# ---------------------------
# DIMENSÕES
# ---------------------------

def load_dim_filial():
    df = pd.read_sql("SELECT DISTINCT fil_codigo FROM filial", conn_erp)
    insert_q = "INSERT INTO dim_filial (fil_codigo) VALUES (?)"
    truncate_and_insert(df, "dim_filial", insert_q)

def load_dim_atividade():
    df = pd.read_sql("SELECT DISTINCT atv_codigo, atv_desc FROM atividade", conn_erp)
    insert_q = "INSERT INTO dim_atividade (atv_codigo, atv_desc) VALUES (?, ?)"
    truncate_and_insert(df, "dim_atividade", insert_q)

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
    truncate_and_insert(df, "dim_pessoa", insert_q)

def load_dim_colaborador():
    df = pd.read_sql("SELECT clb_codigo, clb_razao FROM colaborador", conn_erp)
    insert_q = "INSERT INTO dim_colaborador (clb_codigo, clb_razao) VALUES (?, ?)"
    truncate_and_insert(df, "dim_colaborador", insert_q)

def load_dim_motdevolucao():
    df = pd.read_sql("SELECT mtd_codigo, mtd_desc FROM motdevolucao", conn_erp)
    df_sem_motivo = pd.DataFrame([{
        'mtd_codigo': 999999,
        'mtd_desc': 'SEM MOTIVO'
    }])
    df = pd.concat([df, df_sem_motivo], ignore_index=True).drop_duplicates(subset='mtd_codigo')
    insert_q = "INSERT INTO dim_motdevolucao (mtd_codigo, mtd_desc) VALUES (?, ?)"
    truncate_and_insert(df, "dim_motdevolucao", insert_q)

def load_dim_produto():
    query = """
        SELECT pro_codigo, pro_desc, pro_ativo
        FROM produto
        WHERE pro_grpcodigo = 1 OR pro_codigo IN (1801, 704, 1723)
    """
    df = pd.read_sql(query, conn_erp)
    insert_q = "INSERT INTO dim_produto (pro_codigo, pro_desc, pro_ativo) VALUES (?, ?, ?)"
    truncate_and_insert(df, "dim_produto", insert_q)

# ---------------------------
# FATOS
# ---------------------------

def load_fato_pedidos():
    query = """
        SELECT
            ped_filcodigo,
            ped_spvcodigo,
            ped_numero,
            ped_pesobruto,
            ped_valor,
            ped_pescodigo,
            ped_vencodigo,
            ped_dtemissao,
            ped_dtentrega,
            ped_natcodigo,
            ped_stpcodigo,
            ped_valsubst
        FROM pedidos
        WHERE YEAR(ped_dtemissao) >= 2025
          AND ped_stpcodigo = 6
          AND ped_natcodigo IN ('VE','VTE','VIN','VTI','DEV','DTR','DV')
    """
    cursor_dw.execute("SELECT Id_Pedido FROM fato_pedidos")
    ids_dw = set(row[0] for row in cursor_dw.fetchall())

    for chunk in pd.read_sql(query, conn_erp, chunksize=5000):
        chunk['Id_Pedido'] = chunk.apply(
            lambda row: f"{int(row.ped_filcodigo):02}{str(row.ped_spvcodigo).zfill(3)}{int(row.ped_numero):010}",
            axis=1
        )
        chunk = chunk[~chunk['Id_Pedido'].isin(ids_dw)]

        insert_q = """
            INSERT INTO fato_pedidos (
                Id_Pedido, Ped_FilCodigo, Ped_SpvCodigo, Ped_Numero,
                Ped_pesobruto, Ped_Valor, Ped_PesCodigo, Ped_VenCodigo,
                Ped_DtEmissao, Ped_DtEntrega, Ped_NatCodigo, Ped_StpCodigo, Ped_valsubst
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        for _, row in chunk.iterrows():
            cursor_dw.execute("DELETE FROM fato_pedidos WHERE Id_Pedido = ?", row.Id_Pedido)
            cursor_dw.execute(insert_q, (
                row.Id_Pedido,
                row.ped_filcodigo,
                row.ped_spvcodigo,
                row.ped_numero,
                row.ped_pesobruto or 0,
                row.ped_valor or 0,
                row.ped_pescodigo,
                row.ped_vencodigo,
                row.ped_dtemissao,
                row.ped_dtentrega,
                row.ped_natcodigo,
                row.ped_stpcodigo,
                row.ped_valsubst or 0
            ))
        conn_dw.commit()
        print(f"fato_pedidos: chunk de {len(chunk)} registros inserido")

def load_fato_itenspedido():
    query = """
        SELECT
            ipv.ipv_filcodigo,
            ipv.ipv_spvcodigo,
            ipv.ipv_pednumero,
            ipv.ipv_numitem,
            ipv.ipv_procodigo,
            ipv.ipv_quantidade,
            ipv.ipv_precovenda,
            ipv.ipv_valsubst,
            ipv.ipv_unpunidade,
            ipv.ipv_propbruto,
            p.ped_dtemissao
        FROM itenspedido ipv
        JOIN pedidos p ON
            ipv.ipv_filcodigo = p.ped_filcodigo AND
            ipv.ipv_spvcodigo = p.ped_spvcodigo AND
            ipv.ipv_pednumero = p.ped_numero
        WHERE YEAR(p.ped_dtemissao) >= 2025
          AND p.ped_stpcodigo = 6
          AND p.ped_natcodigo IN ('VE','VTE','VIN','VTI','DEV','DTR','DV')
    """
    cursor_dw.execute("SELECT Id_ItemPedido FROM fato_itenspedido")
    ids_dw = set(row[0] for row in cursor_dw.fetchall())

    query_motivos = """
        SELECT mvd_filcodigo, mvd_spvcodigo, mvd_numero AS mvd_pednumero, mvd_procodigo, mvd_mtdcodigo
        FROM movdevolucao
    """
    df_motivos = pd.read_sql(query_motivos, conn_erp)

    for chunk in pd.read_sql(query, conn_erp, chunksize=5000):
        chunk['Id_Pedido'] = chunk.apply(
            lambda row: f"{int(row.ipv_filcodigo):02}{str(row.ipv_spvcodigo).zfill(3)}{int(row.ipv_pednumero):010}",
            axis=1
        )
        chunk['Id_ItemPedido'] = chunk.apply(
            lambda row: f"{row.Id_Pedido}{int(row.ipv_numitem):03}",
            axis=1
        )
        chunk = chunk[~chunk['Id_ItemPedido'].isin(ids_dw)]

        chunk = chunk.merge(
            df_motivos,
            how='left',
            left_on=['ipv_filcodigo','ipv_spvcodigo','ipv_pednumero','ipv_procodigo'],
            right_on=['mvd_filcodigo','mvd_spvcodigo','mvd_pednumero','mvd_procodigo']
        )
        chunk['ipv_mtdcodigo'] = chunk['mvd_mtdcodigo'].fillna(999999).astype(int)

        insert_query = """
            INSERT INTO fato_itenspedido (
                Id_ItemPedido, Id_Pedido,
                ipv_numitem, ipv_ProCodigo, ipv_Quantidade, ipv_precovenda,
                ipv_valsubst, ipv_UnpUnidade, ipv_propbruto, ipv_mtdcodigo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        for _, row in chunk.iterrows():
            cursor_dw.execute("DELETE FROM fato_itenspedido WHERE Id_ItemPedido = ?", row.Id_ItemPedido)
            cursor_dw.execute(insert_query, (
                row.Id_ItemPedido,
                row.Id_Pedido,
                row.ipv_numitem,
                row.ipv_procodigo,
                row.ipv_quantidade or 0,
                row.ipv_precovenda or 0,
                row.ipv_valsubst or 0,
                row.ipv_unpunidade,
                row.ipv_propbruto or 0,
                row.ipv_mtdcodigo
            ))
        conn_dw.commit()
        print(f"fato_itenspedido: chunk de {len(chunk)} registros inserido")

# Execução
load_dim_filial()
load_dim_atividade()
load_dim_pessoa()
load_dim_colaborador()
load_dim_motdevolucao()
load_dim_produto()

load_fato_pedidos()
load_fato_itenspedido()
