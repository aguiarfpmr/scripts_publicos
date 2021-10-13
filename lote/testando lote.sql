


SET STATISTICS TIME OFF
EXEC delete_lote_sp
@tabela_delete = 'tabela1'
,@tabela_aux = 'tabela1_aux'
,@colunas_chave = 'serie,ID_TABELA1'
--,@colunas_chave = 'serie'
,@lote = 1000
,@executar = 1


CREATE INDEX IX_TEMP ON tabela1_aux (SERIE,ID_TABELA1)


SET STATISTICS TIME ON
DELETE TABELA1
FROM TABELA1 A
WHERE EXISTS (SELECT 1
			  FROM tabela1_aux B
			  WHERE A.SERIE = B.SERIE
			  AND A.ID_TABELA1 = B.ID_TABELA1)

