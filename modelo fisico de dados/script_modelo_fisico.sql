-- Cria o schema do Data Warehouse, se ainda não existir
CREATE SCHEMA IF NOT EXISTS dw_xodo;
USE dw_xodo;


-- TABELAS DIMENSÃO


-- Tabela: dim_filial
CREATE TABLE dim_filial (
  fil_codigo INT(11) NOT NULL,
  PRIMARY KEY (fil_codigo)
) ENGINE=InnoDB;

-- Tabela: dim_atividade
CREATE TABLE dim_atividade (
  atv_codigo INT(11) NOT NULL,
  atv_desc VARCHAR(200) NOT NULL,
  PRIMARY KEY (atv_codigo)
) ENGINE=InnoDB;

-- Tabela: dim_pessoa
CREATE TABLE dim_pessoa (
  pes_codigo INT(25) NOT NULL,
  pes_razao VARCHAR(200) NOT NULL,
  pes_fantasia VARCHAR(200),
  pes_atvcodigo INT(11),
  pes_ativo BOOLEAN NOT NULL DEFAULT 1,
  PRIMARY KEY (pes_codigo),
  FOREIGN KEY (pes_atvcodigo) REFERENCES dim_atividade(atv_codigo)
) ENGINE=InnoDB;

-- Tabela: dim_colaborador
CREATE TABLE dim_colaborador (
  clb_codigo INT(25) NOT NULL,
  clb_razao VARCHAR(200) NOT NULL,
  PRIMARY KEY (clb_codigo)
) ENGINE=InnoDB;

-- Tabela: dim_produto
CREATE TABLE dim_produto (
  pro_codigo INT(11) NOT NULL,
  pro_desc VARCHAR(250) NOT NULL,
  PRIMARY KEY (pro_codigo)
) ENGINE=InnoDB;

-- Tabela: dim_motdevolucao
CREATE TABLE dim_motdevolucao (
  mtd_codigo INT(11) NOT NULL,
  mtd_desc VARCHAR(200) NOT NULL,
  PRIMARY KEY (mtd_codigo)
) ENGINE=InnoDB;

-- TABELAS FATO


-- Tabela: fato_pedidos
CREATE TABLE fato_pedidos (
  Id_Pedido       VARCHAR(25) NOT NULL,             -- junção de Ped_FilCodigo, Ped_SpvCodigo e Ped_Numero
  Ped_FilCodigo   INT(11) NOT NULL,
  Ped_SpvCodigo   VARCHAR(3) NOT NULL,
  Ped_Numero      INT(11) NOT NULL,
  Ped_pesobruto   DOUBLE NOT NULL,
  Ped_Valor       DOUBLE NOT NULL,
  Ped_PesCodigo   INT(25) NOT NULL,
  Ped_VenCodigo   INT(25) NOT NULL,
  Ped_DtEmissao   DATE NOT NULL,
  Ped_DtEntrega   DATE NOT NULL,
  Ped_NatCodigo   VARCHAR(3) NOT NULL,
  Ped_StpCodigo   INT(11) NOT NULL,
  Ped_valsubst    DOUBLE ,
  PRIMARY KEY (Id_Pedido),
  FOREIGN KEY (Ped_FilCodigo) REFERENCES dim_filial(fil_codigo),
  FOREIGN KEY (Ped_PesCodigo) REFERENCES dim_pessoa(pes_codigo),
  FOREIGN KEY (Ped_VenCodigo) REFERENCES dim_colaborador(clb_codigo)
) ENGINE=InnoDB;

-- Tabela: fato_itenspedido
CREATE TABLE fato_itenspedido (
  Id_ItemPedido     VARCHAR(61) NOT NULL,
  Id_Pedido         VARCHAR(25) NOT NULL,
  ipv_numitem       INT(25) NOT NULL,
  ipv_ProCodigo     INT(11) NOT NULL,
  ipv_Quantidade    DOUBLE NOT NULL,
  ipv_precovenda    DOUBLE NOT NULL,
  ipv_valsubst      DOUBLE,
  ipv_UnpUnidade    VARCHAR(3) NOT NULL,
  ipv_propbruto     DOUBLE NOT NULL,
  ipv_mtdcodigo		INT(11) NOT NULL,
  PRIMARY KEY (Id_ItemPedido),
  FOREIGN KEY (Id_Pedido) REFERENCES fato_pedidos(Id_Pedido),
  FOREIGN KEY (ipv_ProCodigo) REFERENCES dim_produto(pro_codigo)
) ENGINE=InnoDB;


