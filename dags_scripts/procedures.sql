CREATE OR REPLACE PROCEDURE `teste-boticario-377702.procedures.treat_tables_vendas`()
BEGIN
CREATE TABLE IF NOT EXISTS `teste-boticario-377702.silver.vendas_work_table`   AS
(
SELECT 	
 CAST(ID_MARCA	AS INT64)  AS IdMarca		
,CAST(MARCA		AS STRING) AS nmMarca		
,CAST(ID_LINHA	AS INT64)  AS IdLinha		
,CAST(LINHA		AS STRING) AS nmLinha	
,FORMAT_DATE("%Y", DATA_VENDA ) as nmAno
,FORMAT_DATE("%m", DATA_VENDA ) as nmMes
,CAST(QTD_VENDA  AS INT64)  AS QtdVenda  
FROM `teste-boticario-377702.raw.dado_vendas` 
);


CREATE TABLE IF NOT EXISTS `teste-boticario-377702.gold.vendas_ano_mes`  AS
(
SELECT 
nmAno
,nmMes 
,SUM(QtdVenda) as total_vendas
FROM `teste-boticario-377702.silver.vendas_work_table` 
GROUP BY 
nmAno
,nmMes
ORDER BY
nmAno DESC
,nmMes DESC
);


CREATE TABLE IF NOT EXISTS `teste-boticario-377702.gold.vendas_marca_linha`  AS
(
SELECT 
 IdMarca
,nmMarca
,IdLinha
,nmLinha
,SUM(QtdVenda) as total_vendas
FROM `teste-boticario-377702.silver.vendas_work_table` 
GROUP BY 
 IdMarca
,nmMarca
,IdLinha
,nmLinha
ORDER BY
IdMarca DESC
,IdLinha DESC
);



CREATE TABLE IF NOT EXISTS `teste-boticario-377702.gold.vendas_marca_ano_mes`  AS
(
 SELECT 
 IdMarca
 ,nmMarca
 ,nmAno
 ,nmMes 
 ,SUM(QtdVenda) as total_vendas
 FROM `teste-boticario-377702.silver.vendas_work_table` 
 GROUP BY 
 IdMarca
,nmMarca
,nmAno
,nmMes
ORDER BY
idMarca DESC
,nmAno DESC
,nmMes DESC    
);


CREATE TABLE IF NOT EXISTS `teste-boticario-377702.gold.vendas_linha_ano_mes`  AS
(
SELECT 
 IdLinha
,nmLinha
,nmAno
,nmMes 
,SUM(QtdVenda) as total_vendas
 FROM `teste-boticario-377702.silver.vendas_work_table` 
 GROUP BY 
 IdLinha
,nmLinha
,nmAno
,nmMes
ORDER BY
idLinha DESC
,nmAno DESC
,nmMes DESC  
);

END;