# ETL DW XODO - VERSAO MySQL 8 COM MYSQL.CONNECTOR

import mysql.connector
import pandas as pd

# Conexoes
conn_erp = mysql.connector.connect(
    host='localhost', port=3306, user='root', password='sua_senha', database='erp_xodo'
)

conn_dw = mysql.connector.connect(
    host='localhost', port=3307, user='root', password='@xodo123', database='dw_xodo'
)

cursor_dw = conn_dw.cursor()
print("✅ Conectado ao ERP e DW")

# Função auxiliar para deletar e inserir
def truncate_and_insert(df, table_name, insert_query):
    cursor_dw.execute(f"DELETE FROM {table_name}")
    for _, row in df.iterrows():
        cursor_dw.execute(insert_query, tuple(row))
    conn_dw.commit()
    print(f"✅ {len(df)} registros inseridos em {table_name}")

# DIMENSOES (mantidas iguais)
def load_dim_filial():
    df = pd.read_sql("SELECT DISTINCT fil_codigo FROM filial", conn_erp)
    insert_q = "INSERT INTO dim_filial (fil_codigo) VALUES (%s)"
    truncate_and_insert(df, "dim_filial", insert_q)

# (demais dimensões também iguais, basta trocar o "?" por "%s" nos INSERTs)

# FATOS

def load_fato_pedidos():
    query = """
        SELECT ped_filcodigo, ped_spvcodigo, ped_numero, ped_pesobruto, ped_valor,
               ped_pescodigo, ped_vencodigo, ped_dtemissao, ped_dtentrega,
               ped_natcodigo, ped_stpcodigo, ped_valsubst
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
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        for _, row in chunk.iterrows():
            cursor_dw.execute("DELETE FROM fato_pedidos WHERE Id_Pedido = %s", (row.Id_Pedido,))
            cursor_dw.execute(insert_q, tuple(row))
        conn_dw.commit()
        print(f"fato_pedidos: chunk de {len(chunk)} registros inserido")

def load_fato_itenspedido():
    query = """
        SELECT ipv.ipv_filcodigo, ipv.ipv_spvcodigo, ipv.ipv_pednumero, ipv.ipv_numitem,
               ipv.ipv_procodigo, ipv.ipv_quantidade, ipv.ipv_precovenda, ipv.ipv_valsubst,
               ipv.ipv_unpunidade, ipv.ipv_propbruto, p.ped_dtemissao
        FROM itenspedido ipv
        JOIN pedidos p ON ipv.ipv_filcodigo = p.ped_filcodigo AND ipv.ipv_spvcodigo = p.ped_spvcodigo AND ipv.ipv_pednumero = p.ped_numero
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
            lambda row: f"{int(row.ipv_filcodigo):02}{str(row.ipv_spvcodigo).zfill(3)}{int(row.ipv_pednumero):010}", axis=1)
        chunk['Id_ItemPedido'] = chunk.apply(
            lambda row: f"{row.Id_Pedido}{int(row.ipv_numitem):03}", axis=1)
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
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        for _, row in chunk.iterrows():
            cursor_dw.execute("DELETE FROM fato_itenspedido WHERE Id_ItemPedido = %s", (row.Id_ItemPedido,))
            cursor_dw.execute(insert_query, tuple(row))
        conn_dw.commit()
        print(f"fato_itenspedido: chunk de {len(chunk)} registros inserido")

# EXECUCAO COMPLETA
load_dim_filial()
# (chamar aqui as outras funções de dimensão)
load_fato_pedidos()
load_fato_itenspedido()

cursor_dw.close()
conn_erp.close()
conn_dw.close()
