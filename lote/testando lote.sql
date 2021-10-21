
USE DTS_TOOLS
GO

/*

SET STATISTICS TIME OFF
EXEC delete_lote_sp
@tabela_delete = 'tabela1'
,@tabela_aux = 'tabela1_aux'
,@colunas_chave = 'ID_TABELA1'
,@lote = 1000
,@executar = 1

*/


SET STATISTICS TIME OFF
DROP TABLE IF EXISTS tabela1_aux_BACKUP_UPDATE_LOTE_SP
EXEC update_lote_sp
@tabela_update = 'tabela1'
,@tabela_aux = 'tabela1_aux'
,@colunas_chave = 'ID_TABELA1'
,@colunas_update = 'VALOR,codigo'
,@lote = 1000
,@backup = 1
,@executar = 1

SELECT *
FROM tabela1_aux_BACKUP_UPDATE_LOTE_SP
ORDER BY ID_TABELA1


SELECT *
FROM tabela1
ORDER BY ID_TABELA1

/*
CREATE INDEX IX_TEMP ON tabela1_aux (SERIE,ID_TABELA1)


SET STATISTICS TIME ON
DELETE TABELA1
FROM TABELA1 A
WHERE EXISTS (SELECT 1
			  FROM tabela1_aux B
			  WHERE A.SERIE = B.SERIE
			  AND A.ID_TABELA1 = B.ID_TABELA1)


*/