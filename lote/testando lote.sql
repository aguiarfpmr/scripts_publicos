
USE DTS_TOOLS
GO



SET STATISTICS TIME OFF



EXEC UPDATE_OR_DELETE_LOTE
 @COMANDO = 'delete'
,@tabela_UPDATE_OR_DELETE_LOTE = 'tabela1'
,@tabela_aux = 'tabela1_aux'
,@colunas_chave = 'ID_TABELA1'
,@lote = 1000
,@executar = 0




SELECT *
FROM dbo.tabela1

SELECT *
FROM dbo.tabela1_aux

SET STATISTICS TIME OFF
DROP TABLE IF EXISTS tabela1_aux_BACKUP_UPDATE_LOTE_SP


EXEC UPDATE_OR_DELETE_LOTE
 @COMANDO = 'update'
,@tabela_UPDATE_OR_DELETE_LOTE = 'tabela1'
,@tabela_aux = 'tabela1_aux'
,@colunas_chave = 'ID_TABELA1'
,@colunas_update = 'VALOR,codigo'
,@lote = 1000
,@backup = 1
,@executar = 1



SELECT * FROM [tabela1_aux_BACKUP_UPDATE_LOTE_SP]




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