--✅ Relacionamentos
--Conferir se todos os Id_Pedido da fato_itenspedido existem na fato_pedidos.
--Verificar se todas as chaves estrangeiras de dimensões existem nas tabelas fato.-- Exemplos de consultas úteis para validação e auditoria
-- Exemplo: Itens com pedidos inexistentes (não deveria existir nenhum)
SELECT *
FROM fato_itenspedido f
LEFT JOIN fato_pedidos p ON f.Id_Pedido = p.Id_Pedido
WHERE p.Id_Pedido IS NULL;


-- Total de pedidos no ERP vs no DW (pode ser melhorado para filtra naturezas) 
SELECT YEAR(ped_dtemissao) AS ano, COUNT(*) AS total_erp
FROM pedidos
WHERE ped_stpcodigo = 6
GROUP BY YEAR(ped_dtemissao);

SELECT YEAR(Ped_DtEmissao) AS ano, COUNT(*) AS total_dw
FROM fato_pedidos
GROUP BY YEAR(Ped_DtEmissao);

