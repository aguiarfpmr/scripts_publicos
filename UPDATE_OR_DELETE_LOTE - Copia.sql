--- *************************************** ---
-- 2022-09-20 - FELIPE MOURA - INCLUSÃO DOS PARÂMETROS DE HISTÓRICO
--- *************************************** ---

-- Cria Função split string
create OR ALTER function fSplit (@string varchar(max), @separador char(1)) 
returns table as return
    with a as (
        select
            id = 1,
            len_string = len(@string) + 1,
            ini = 1,
            fim = coalesce(nullif(charindex(@separador, @string, 1), 0), len(@string) + 1),
            elemento = ltrim(rtrim(substring(@string, 1, coalesce(nullif(charindex(@separador, @string, 1), 0), len(@string) + 1)-1)))
        union all
        select
            id + 1,
            len(@string) + 1,
            convert(int, fim) + 1,
            coalesce(nullif(charindex(@separador, @string, fim + 1), 0), len_string), 
            ltrim(rtrim(substring(@string, fim + 1, coalesce(nullif(charindex(@separador, @string, fim + 1), 0), len_string)-fim-1)))
        from a where fim < len_string)
    select id, elemento from a 
    -- incluir with option (maxrecursion 0) na chamada da FC para strings com mais de 100 elementos
GO


IF NOT EXISTS (SELECT *
FROM INFORMATION_SCHEMA.ROUTINES
WHERE SPECIFIC_NAME = 'UPDATE_OR_DELETE_LOTE'
AND ROUTINE_TYPE = 'PROCEDURE'
)
BEGIN
	EXEC ('CREATE PROCEDURE UPDATE_OR_DELETE_LOTE AS BEGIN SELECT 1 END')
END
GO

ALTER PROCEDURE UPDATE_OR_DELETE_LOTE
(
	 @COMANDO VARCHAR(50)
	,@tabela_UPDATE_OR_DELETE_LOTE	varchar(50)
	,@tabela_aux	 varchar(50)
	,@colunas_chave	 varchar(150) --SEPARADO POR VÍRGULAS
	,@colunas_update varchar(150) = '' --SEPARADO POR VÍRGULAS / DEIXAR VAZIO EM CASO DE @COMANDO = 'DELETE' 
	,@backup		 bit = 0 -- 0 = Não guarda backup / 1 = Cria uma tabela de backup com os valores OLD e NEW
							 -- DEIXAR VAZIO EM CASO DE @COMANDO = 'DELETE' 
	,@tabela_historico VARCHAR(250) = '' -- INFORMAR O NOME DA TABELA EM QUE OS DADOS SERÃO ENVIADOS APÓS O EXPURGO DA TABELA PRINCIPAL
	,@database_tabela_historico VARCHAR(250) = '' -- INFORMAR O NOME DA DATABASE ONDE FICARÁ A TABELA HISTÓRICO 
	,@schema_tabela_historico VARCHAR(250) = '' -- INFORMAR O NOME DO SCHEMA ONDE FICARÁ A TABELA HISTÓRICO 
	,@lote			 VARCHAR(10)
	,@executar		 BIT = 0 -- 0 = apenas exibe o script final / 1 = executa o delete por lote
)
AS BEGIN

IF @COMANDO NOT IN ('UPDATE','DELETE')
BEGIN
	RAISERROR('@comando não reconhecido, deve ser informado DELETE OU UPDATE',16,0)
END


-- variaveis 
declare @query varchar(max) = ''
DECLARE @ID INT = 0
DECLARE @DT_INICIO SMALLDATETIME
--

----------------------------------------------------------------------------------------
-- CRIA TABELA QUE VAI GUARDAR TODOS OS SCRIPTS PARA SER EXECUTADOS NO FINAL
----------------------------------------------------------------------------------------
IF OBJECT_ID('TEMPDB..#TEMP_SCRIPTS') IS NOT NULL
DROP TABLE #TEMP_SCRIPTS
CREATE TABLE #TEMP_SCRIPTS 
(
	 ID				INT IDENTITY(1,1) PRIMARY KEY NOT NULL
	,QUERY			VARCHAR(8000)
	,FEITO			BIT DEFAULT 0
	,SUCESSO		BIT DEFAULT 0
	,DT_INICIO_EXEC DATETIME2 DEFAULT NULL
	,DT_FIM_EXEC	DATETIME2 DEFAULT NULL
	,TEMPO_TOTAL_EXECUCAO AS 
		(CONVERT(VARCHAR(10),DATEDIFF(HH,DT_INICIO_EXEC,DT_FIM_EXEC) % 60) + ' Hora : ' +
		 CONVERT(VARCHAR(10),DATEDIFF(MI,DT_INICIO_EXEC,DT_FIM_EXEC) % 60) + ' Min : ' +
		 CONVERT(VARCHAR(10),DATEDIFF(SS,DT_INICIO_EXEC,DT_FIM_EXEC) % 60) + ' Seg : ' + 
		 CONVERT(VARCHAR(10),DATEDIFF(MS,DT_INICIO_EXEC,DT_FIM_EXEC) % 1000) + ' Ms'
		)
)

IF @COMANDO = 'DELETE'
BEGIN

DECLARE @tabela_delete varchar(50)
SELECT @tabela_delete =  @tabela_UPDATE_OR_DELETE_LOTE

IF ISNULL(@database_tabela_historico,'') = ''
SET @database_tabela_historico = DB_NAME()

IF LEFT(@database_tabela_historico,1) <> '['
SET @database_tabela_historico = '[' + @database_tabela_historico + ']'

IF ISNULL(@schema_tabela_historico,'') = ''
SET @schema_tabela_historico = 'DBO'

IF LEFT(@schema_tabela_historico,1) <> '['
SET @schema_tabela_historico = '[' + @schema_tabela_historico + ']'
  
BEGIN TRY 
set nocount on


CREATE INDEX IX_TEMP ON #TEMP_SCRIPTS (ID,FEITO) INCLUDE (QUERY,SUCESSO,DT_INICIO_EXEC,DT_FIM_EXEC,TEMPO_TOTAL_EXECUCAO)


--VERIFICA SE A TABELA HISTÓRICO JÁ FOI CRIADA, CASO NÃO, REALIZA A CRIAÇÃO
insert into #TEMP_SCRIPTS (QUERY)
SELECT '
--VERIFICA SE A TABELA HISTÓRICO JÁ FOI CRIADA, CASO NÃO, REALIZA A CRIAÇÃO
IF NOT EXISTS (
SELECT 1
FROM ' + @database_tabela_historico + '.SYS.tables
WHERE NAME = ' + '''' + @tabela_historico + '''' + '
)
BEGIN
SELECT TOP 0 *
INTO ' + @database_tabela_historico + '.' + @schema_tabela_historico + '.' + @tabela_historico + '
FROM ' + @tabela_delete + '
END
'

-- REALIZA O SPLIT STRING DA @colunas_chave
----------------------------------------------------------------------------------------
IF OBJECT_ID('TEMPDB..##TEMP_STRING_SPLIT_DELETE_LOTE_SP') IS NOT NULL
DROP TABLE ##TEMP_STRING_SPLIT_DELETE_LOTE_SP

CREATE TABLE ##TEMP_STRING_SPLIT_DELETE_LOTE_SP
(
ID INT,
COLUNA VARCHAR(100),
script_ligacao VARCHAR(8000)
)

insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- REALIZA O SPLIT STRING DA @colunas_chave
----------------------------------------------------------------------------------------
SET NOCOUNT ON

IF OBJECT_ID(''TEMPDB..##TEMP_STRING_SPLIT_DELETE_LOTE_SP'') IS NOT NULL
DROP TABLE ##TEMP_STRING_SPLIT_DELETE_LOTE_SP

CREATE TABLE ##TEMP_STRING_SPLIT_DELETE_LOTE_SP
(
ID INT,
COLUNA VARCHAR(100),
script_ligacao VARCHAR(8000)
)

declare @colunas_chave	varchar(150) = ' + '''' + @colunas_chave + '''' + '
INSERT INTO ##TEMP_STRING_SPLIT_DELETE_LOTE_SP
SELECT 
id, 
COLUNA = elemento,
script_ligacao = ''and A.'' + elemento + '' = B.'' + elemento
FROM fSplit (' + '''' + @colunas_chave + '''' + ', '','')
option (maxrecursion 0)
'

insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- VERIFICA SE A TABELA DELETE POSSUI AS COLUNAS CHAVES INDICADAS NO @colunas_chave
----------------------------------------------------------------------------------------
-- SE A TABELA FOR HEAP, AS COLUNAS DO @colunas_chave SÓ PRECISAM EXISTIR NA @tabela_delete

DECLARE @tabela_delete VARCHAR(150) = ' + '''' + @tabela_delete + '''' + '
DECLARE @COLUNAS_NAO_EXISTEM VARCHAR(8000) = ''''
IF EXISTS (
			SELECT *
			FROM SYS.tables	ST
			WHERE NOT EXISTS (	SELECT 1
								FROM SYS.indexes SI 
								WHERE ST.object_id = SI.object_id
								AND SI.type = 1	--CLUSTERED
							 )
			AND ST.object_id = object_id(@tabela_delete)
)
BEGIN
	
	SELECT @COLUNAS_NAO_EXISTEM = @COLUNAS_NAO_EXISTEM + COLUNA + '', ''
	FROM ##TEMP_STRING_SPLIT_DELETE_LOTE_SP TEMP
	WHERE NOT EXISTS (
						SELECT *
						FROM INFORMATION_SCHEMA.COLUMNS IS_COL
						WHERE IS_COL.COLUMN_NAME COLLATE DATABASE_DEFAULT = TEMP.COLUNA COLLATE DATABASE_DEFAULT
						AND IS_COL.TABLE_NAME = CASE WHEN @tabela_delete NOT LIKE ''%.%'' THEN @tabela_delete ELSE SUBSTRING(@tabela_delete,CHARINDEX(''.'',@tabela_delete+''.'')+1,LEN(@tabela_delete)) END )
	
	SELECT @COLUNAS_NAO_EXISTEM = ''As seguintes colunas chaves não existem na @tabela_delete: '' + CASE WHEN NULLIF(@COLUNAS_NAO_EXISTEM,'''') IS NOT NULL THEN QUOTENAME(LEFT(@COLUNAS_NAO_EXISTEM,LEN(@COLUNAS_NAO_EXISTEM)-1)) ELSE NULL END

	IF ISNULL(@COLUNAS_NAO_EXISTEM,'''') <> ''''
	BEGIN
	   RAISERROR (@COLUNAS_NAO_EXISTEM,16,0)  
	   PRINT @COLUNAS_NAO_EXISTEM
	   SELECT @COLUNAS_NAO_EXISTEM 
	   --return @COLUNAS_NAO_EXISTEM
	END


END-- FIM DO EXISTS
-- FIM DA VALIDAÇÃO TABELA HEAP
-- COMEÇO DA VALIDAÇÃO TABELA CLUSTERED
ELSE
BEGIN
	
	--DECLARE @tabela_delete VARCHAR(150) = ''TABELA4''
	--DECLARE @COLUNAS_NAO_EXISTEM VARCHAR(8000) = '''' 
	SELECT @COLUNAS_NAO_EXISTEM = @COLUNAS_NAO_EXISTEM + COLUNA + '', ''
	FROM ##TEMP_STRING_SPLIT_DELETE_LOTE_SP TEMP
	WHERE NOT EXISTS ( 
						SELECT SC.name
						FROM SYS.indexes SI
						JOIN SYS.index_columns SIC ON OBJECT_NAME(SIC.object_id) = OBJECT_NAME(SI.object_id) AND SIC.index_id = SI.index_id
						JOIN SYS.columns SC ON OBJECT_NAME(SC.object_id) = OBJECT_NAME(SI.object_id) AND SIC.column_id = SC.column_id
						WHERE OBJECT_NAME(SI.object_id) = CASE WHEN @tabela_delete NOT LIKE ''%.%'' THEN @tabela_delete ELSE SUBSTRING(@tabela_delete,CHARINDEX(''.'',@tabela_delete+''.'')+1,LEN(@tabela_delete)) END
						AND SI.type = 1	 --CLUSTERED
						AND SC.name COLLATE DATABASE_DEFAULT = TEMP.COLUNA COLLATE DATABASE_DEFAULT
					 )

	--SELECT @COLUNAS_NAO_EXISTEM

	SELECT @COLUNAS_NAO_EXISTEM = ''A seguinte coluna não é uma PRIMARY KEY da tabela @tabela_delete: '' + CASE WHEN NULLIF(@COLUNAS_NAO_EXISTEM,'''') IS NOT NULL THEN QUOTENAME(LEFT(@COLUNAS_NAO_EXISTEM,LEN(@COLUNAS_NAO_EXISTEM)-1)) ELSE NULL END

	IF ISNULL(@COLUNAS_NAO_EXISTEM,'''') <> ''''
	BEGIN
	   RAISERROR (@COLUNAS_NAO_EXISTEM,16,0)  
	   PRINT @COLUNAS_NAO_EXISTEM
	   SELECT @COLUNAS_NAO_EXISTEM 
	END
						

END	--FIM DO ELSE
-- FIM DA VALIDAÇÃO TABELA CLUSTERED
'

 insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- CRIA UMA FLAG DE CONTROLE DAS LINHAS QUE JÁ FORAM DELETADAS
----------------------------------------------------------------------------------------

--DECLARE @tabela_aux VARCHAR(150) = ' + '' + @tabela_aux + '' + '
IF NOT EXISTS (
				SELECT *
				FROM SYS.columns
				WHERE object_id = object_id(' + '''' + @tabela_aux + '''' + ')
				AND name = ''FEITO_LOTE''
			  )
BEGIN
	ALTER TABLE ' + '' + @tabela_aux + '' + ' ADD FEITO_LOTE BIT DEFAULT 0 NOT NULL 
END
' +
CASE WHEN @executar = 0 THEN 
'GO' ELSE '' END 


insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- CRIA ÍNDICE PARA MELHOR ATENDER A QUERY 
----------------------------------------------------------------------------------------
IF NOT EXISTS (
			SELECT *
			FROM SYS.indexes
			WHERE OBJECT_NAME(object_id) = ' + '''' + @tabela_aux + '''' + '
			AND TYPE IN (1,2)
			AND NAME = ''IX_TEMP_LOTE''
		  )							 
BEGIN
CREATE INDEX IX_TEMP_LOTE ON ' +  @tabela_aux + ' (' + @colunas_chave + ', FEITO_LOTE) 
END

IF NOT EXISTS (
			SELECT *
			FROM SYS.indexes
			WHERE OBJECT_NAME(object_id) = ' + '''' + @tabela_aux + '''' + '
			AND TYPE IN (1,2)
			AND NAME = ''IX_TEMP_LOTE2''
		  )							 
BEGIN
CREATE INDEX IX_TEMP_LOTE2 ON ' +  @tabela_aux + ' (FEITO_LOTE,' + @colunas_chave + ')
END


'

SELECT @query = '
-- MONTA O DELETE POR LOTE
----------------------------------------------------------------------------------------
declare @LIGACAO varchar(8000) = ''''
declare @colunas_cross varchar(8000) = ''''

select @LIGACAO = @LIGACAO + script_ligacao + char(13) + char(10) + char(9),
	   @colunas_cross = @colunas_cross + coluna + '',''
from  ##TEMP_STRING_SPLIT_DELETE_LOTE_SP


select @colunas_cross = left(@colunas_cross,len(@colunas_cross)-1) 

DECLARE @QUERY VARCHAR(MAX) = ''''
		
select @QUERY = ''
-- EXIBINDO O DELETE POR LOTE
----------------------------------------------------------------------------------------
SET NOCOUNT ON
DECLARE @QNTD_REGISTROS INT
		,@TEMPO_DELETE VARCHAR(4000) = '''''''' 
		,@TEMPO_INICIO DATETIME2 = NULL
		,@MSG VARCHAR(4000) = ''''''''
WHILE (SELECT COUNT(*) FROM ' + @tabela_aux + ' WHERE FEITO_LOTE = 0) > 0
BEGIN

	SELECT @QNTD_REGISTROS = COUNT(*)
	FROM ' + @tabela_aux + ' WHERE FEITO_LOTE = 0
	
	SELECT @MSG = ''''Quantidade de registros restantes: '''' + CONVERT(VARCHAR(10),@QNTD_REGISTROS )
	RAISERROR( @MSG ,0,1) WITH NOWAIT

	SELECT @TEMPO_INICIO = GETDATE()

	DELETE A ' +
	CASE WHEN ISNULL(@tabela_historico,'') <> '' THEN '
	OUTPUT 
	DELETED.*
	INTO ' + @database_tabela_historico + '.' + @schema_tabela_historico + '.' + @tabela_historico
	ELSE '' END + '
	FROM ' + @tabela_delete + ' A 
	CROSS APPLY (SELECT TOP ' + @lote + ' '' + @colunas_cross + ''' + ' 
				 FROM ' + @tabela_aux + ' AUX --WITH (INDEX (IX_TEMP_LOTE2))
				 where FEITO_LOTE = 0
				 ORDER BY ' + @colunas_chave + ') B
	WHERE 1=1
	' + ''' + @LIGACAO + ''' + '
	--option(recompile)

	UPDATE A
	SET FEITO_LOTE = 1
	FROM ' + @tabela_aux + ' A
	CROSS APPLY (SELECT TOP ' + @lote + ' '' + @colunas_cross + ''' + ' 
				 FROM ' + @tabela_aux + ' AUX2
				 where FEITO_LOTE = 0
				 ORDER BY ' + @colunas_chave + ') B
	WHERE 1=1
	' + ''' + @LIGACAO + ''' + '

	SELECT @TEMPO_DELETE = CONVERT(VARCHAR(10),DATEDIFF(HH,@TEMPO_INICIO,GETDATE()) % 60) + '''' Hora : '''' +
CONVERT(VARCHAR(10),DATEDIFF(MI,@TEMPO_INICIO,GETDATE()) % 60) + '''' Min : '''' +
CONVERT(VARCHAR(10),DATEDIFF(SS,@TEMPO_INICIO,GETDATE()) % 60) + '''' Seg : '''' + 
CONVERT(VARCHAR(10),DATEDIFF(MS,@TEMPO_INICIO,GETDATE()) % 1000) + '''' Ms''''

SELECT @MSG = ''''	- Tempo execução do lote: '''' + @TEMPO_DELETE
RAISERROR( @MSG , 0,1) WITH NOWAIT 
	

END	-- fim do while

SELECT @QNTD_REGISTROS = COUNT(*)
FROM ' + @tabela_aux + ' WHERE FEITO_LOTE = 0	

SELECT @MSG = ''''Quantidade de registros restantes: '''' + CONVERT(VARCHAR(10),@QNTD_REGISTROS )
RAISERROR(@MSG, 0,1) WITH NOWAIT
-----------------------------------------------------------------------------------------------------

' +
'''
'
+ CASE WHEN @executar = 0 THEN '
print (@QUERY) --EXIBE O DELETE EM LOTE SEM EXECUTAR
' WHEN @executar = 1 THEN 
'EXEC (@QUERY)' END + ' 

IF OBJECT_ID (''tempdb..##TEMP_STRING_SPLIT_DELETE_LOTE_SP'') IS NOT NULL
DROP TABLE ##TEMP_STRING_SPLIT_DELETE_LOTE_SP
'

INSERT INTO #TEMP_SCRIPTS (QUERY)
SELECT @query

IF @executar = 1
BEGIN
	
	WHILE (SELECT COUNT(*) FROM #TEMP_SCRIPTS WHERE FEITO = 0) > 0
	BEGIN
		
	   SELECT TOP 1 @ID = ID, @QUERY = query, @DT_INICIO = GETDATE()
	   FROM #TEMP_SCRIPTS
	   WHERE FEITO = 0

	   UPDATE #TEMP_SCRIPTS
	   SET DT_INICIO_EXEC = GETDATE()
	   FROM #TEMP_SCRIPTS
	   WHERE ID = @ID

	   EXEC (@QUERY)

	   UPDATE #TEMP_SCRIPTS
	   SET FEITO = 1,
		   SUCESSO = 1,
		   DT_FIM_EXEC = GETDATE()
	   FROM #TEMP_SCRIPTS
	   WHERE ID = @ID

	END
END

SELECT case when @executar = 1 then 'Delete executado com sucesso! Resumo abaixo:'
else 'Scrips criados com sucesso! Copie a coluna query logo abaixo.' END

SELECT *
FROM #TEMP_SCRIPTS

IF OBJECT_ID ('tempdb..#TEMP_SCRIPTS') IS NOT NULL
DROP TABLE #TEMP_SCRIPTS

END TRY
BEGIN CATCH

PRINT N'Error Message = ' + CAST(ERROR_MESSAGE() AS varchar(8000));

END CATCH

END --IF @COMANDO = 'DELETE'

IF @COMANDO = 'UPDATE'
BEGIN

DECLARE @tabela_update varchar(50)
SELECT @tabela_update =  @tabela_UPDATE_OR_DELETE_LOTE

BEGIN TRY 


set nocount on


CREATE INDEX IX_TEMP ON #TEMP_SCRIPTS (ID,FEITO) INCLUDE (QUERY,SUCESSO,DT_INICIO_EXEC,DT_FIM_EXEC,TEMPO_TOTAL_EXECUCAO)

-- REALIZA O SPLIT STRING DA @colunas_chave
----------------------------------------------------------------------------------------
IF OBJECT_ID('TEMPDB..##TEMP_STRING_SPLIT_UPDATE_LOTE_SP') IS NOT NULL
DROP TABLE ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP

CREATE TABLE ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP
(
ID INT,
COLUNA VARCHAR(100),
script_ligacao VARCHAR(8000)
)

insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- REALIZA O SPLIT STRING DA @colunas_chave
----------------------------------------------------------------------------------------
SET NOCOUNT ON

IF OBJECT_ID(''TEMPDB..##TEMP_STRING_SPLIT_UPDATE_LOTE_SP'') IS NOT NULL
DROP TABLE ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP

CREATE TABLE ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP
(
ID INT,
COLUNA VARCHAR(100),
script_ligacao VARCHAR(8000)
)

declare @colunas_chave	varchar(150) = ' + '''' + @colunas_chave + '''' + '

INSERT INTO ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP
SELECT 
id, 
COLUNA = elemento,
script_ligacao	= ''and A.'' + elemento + '' = B.'' + elemento 
FROM fSplit (' + '''' + @colunas_chave + '''' + ', '','')
ORDER BY id
option (maxrecursion 0)
'

-- REALIZA O SPLIT STRING DA SELECT @colunas_update
----------------------------------------------------------------------------------------
IF OBJECT_ID('TEMPDB..##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP') IS NOT NULL
DROP TABLE ##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP

CREATE TABLE ##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP
(
ID INT,
COLUNA VARCHAR(100),
script_backup VARCHAR(8000),
script_update VARCHAR(8000)
)

insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- REALIZA O SPLIT STRING DA SELECT @colunas_update
----------------------------------------------------------------------------------------
SET NOCOUNT ON

IF OBJECT_ID(''TEMPDB..##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP'') IS NOT NULL
DROP TABLE ##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP

CREATE TABLE ##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP
(
ID INT,
COLUNA VARCHAR(100),
script_backup VARCHAR(8000),
script_update VARCHAR(8000)
)

declare @colunas_update	varchar(150) = ' + '''' + @colunas_update + '''' + '

INSERT INTO ##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP
SELECT 
id, 
COLUNA = elemento,
script_ligacao	= ''DELETED.'' + elemento + '', INSERTED.'' + elemento + ' + ''',''' + ',
script_update	= elemento + '' = B.'' + elemento
FROM fSplit (' + '''' + @colunas_update + '''' + ', '','')
ORDER BY id
option (maxrecursion 0)
'

insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- VERIFICA SE A TABELA UPDATE POSSUI AS COLUNAS CHAVES INDICADAS NO @colunas_chave
----------------------------------------------------------------------------------------
-- SE A TABELA FOR HEAP, AS COLUNAS DO @colunas_chave SÓ PRECISAM EXISTIR NA @tabela_update

DECLARE @tabela_update VARCHAR(150) = ' + '''' + @tabela_update + '''' + '
DECLARE @COLUNAS_NAO_EXISTEM VARCHAR(8000) = ''''
IF EXISTS (
			SELECT *
			FROM SYS.tables	ST
			WHERE NOT EXISTS (	SELECT 1
								FROM SYS.indexes SI 
								WHERE ST.object_id = SI.object_id
								AND SI.type = 1	--CLUSTERED
							 )
			AND ST.object_id = object_id(@tabela_update)
)
BEGIN
	
	SELECT @COLUNAS_NAO_EXISTEM = @COLUNAS_NAO_EXISTEM + COLUNA + '', ''
	FROM ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP TEMP
	WHERE NOT EXISTS (
						SELECT *
						FROM INFORMATION_SCHEMA.COLUMNS IS_COL
						WHERE IS_COL.COLUMN_NAME = TEMP.COLUNA
						AND IS_COL.TABLE_NAME = CASE WHEN @tabela_update NOT LIKE ''%.%'' THEN @tabela_update ELSE SUBSTRING(@tabela_update,CHARINDEX(''.'',@tabela_update+''.'')+1,LEN(@tabela_update)) END )
	
	SELECT @COLUNAS_NAO_EXISTEM = ''As seguintes colunas chaves não existem na @tabela_update: '' + CASE WHEN NULLIF(@COLUNAS_NAO_EXISTEM,'''') IS NOT NULL THEN QUOTENAME(LEFT(@COLUNAS_NAO_EXISTEM,LEN(@COLUNAS_NAO_EXISTEM)-1)) ELSE NULL END

	IF ISNULL(@COLUNAS_NAO_EXISTEM,'''') <> ''''
	BEGIN
	   RAISERROR (@COLUNAS_NAO_EXISTEM,16,0)  
	   PRINT @COLUNAS_NAO_EXISTEM
	   SELECT @COLUNAS_NAO_EXISTEM 
	   --return @COLUNAS_NAO_EXISTEM
	END


END-- FIM DO EXISTS
-- FIM DA VALIDAÇÃO TABELA HEAP
-- COMEÇO DA VALIDAÇÃO TABELA CLUSTERED
ELSE
BEGIN
	
	--DECLARE @tabela_update VARCHAR(150) = ''TABELA4''
	--DECLARE @COLUNAS_NAO_EXISTEM VARCHAR(8000) = '''' 
	SELECT @COLUNAS_NAO_EXISTEM = @COLUNAS_NAO_EXISTEM + COLUNA + '', ''
	FROM ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP TEMP
	WHERE NOT EXISTS ( 
						SELECT SC.name
						FROM SYS.indexes SI
						JOIN SYS.index_columns SIC ON OBJECT_NAME(SIC.object_id) = OBJECT_NAME(SI.object_id) AND SIC.index_id = SI.index_id
						JOIN SYS.columns SC ON OBJECT_NAME(SC.object_id) = OBJECT_NAME(SI.object_id) AND SIC.column_id = SC.column_id
						WHERE OBJECT_NAME(SI.object_id) = CASE WHEN @tabela_update NOT LIKE ''%.%'' THEN @tabela_update ELSE SUBSTRING(@tabela_update,CHARINDEX(''.'',@tabela_update+''.'')+1,LEN(@tabela_update)) END
						AND SI.type = 1	 --CLUSTERED
						AND SC.name = TEMP.COLUNA
					 )

	--SELECT @COLUNAS_NAO_EXISTEM

	SELECT @COLUNAS_NAO_EXISTEM = ''As seguintes colunas não são uma primary key da @tabela_update: '' + CASE WHEN NULLIF(@COLUNAS_NAO_EXISTEM,'''') IS NOT NULL THEN QUOTENAME(LEFT(@COLUNAS_NAO_EXISTEM,LEN(@COLUNAS_NAO_EXISTEM)-1)) ELSE NULL END

	IF ISNULL(@COLUNAS_NAO_EXISTEM,'''') <> ''''
	BEGIN
	   RAISERROR (@COLUNAS_NAO_EXISTEM,16,0)  
	   PRINT @COLUNAS_NAO_EXISTEM
	   SELECT @COLUNAS_NAO_EXISTEM 
	END
						

END	--FIM DO ELSE
-- FIM DA VALIDAÇÃO TABELA CLUSTERED
'

-- VERIFICA SE A TABELA UPDATE POSSUI AS COLUNAS QUE SERÃO ATUALIZDAS INDICADAS NO @colunas_chave
----------------------------------------------------------------------------------------
insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- VERIFICA SE A TABELA UPDATE POSSUI AS COLUNAS QUE SERÃO ATUALIZDAS INDICADAS NO @colunas_chave
----------------------------------------------------------------------------------------
DECLARE @tabela_update2 VARCHAR(150) = ' + '''' + @tabela_update + '''' + '
DECLARE @COLUNAS_NAO_EXISTEM2 VARCHAR(8000) = ''''
	
SELECT @COLUNAS_NAO_EXISTEM2 = @COLUNAS_NAO_EXISTEM2 + COLUNA + '', ''
FROM ##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP TEMP
WHERE NOT EXISTS (
					SELECT *
					FROM INFORMATION_SCHEMA.COLUMNS IS_COL
					WHERE IS_COL.COLUMN_NAME = TEMP.COLUNA
					AND IS_COL.TABLE_NAME = CASE WHEN @tabela_update2 NOT LIKE ''%.%'' THEN @tabela_update2 ELSE SUBSTRING(@tabela_update2,CHARINDEX(''.'',@tabela_update2+''.'')+1,LEN(@tabela_update2)) END )
	
SELECT @COLUNAS_NAO_EXISTEM2 = ''As seguintes colunas que serão atualizadas não existem na @tabela_update2: '' + CASE WHEN NULLIF(@COLUNAS_NAO_EXISTEM2,'''') IS NOT NULL THEN QUOTENAME(LEFT(@COLUNAS_NAO_EXISTEM2,LEN(@COLUNAS_NAO_EXISTEM2)-1)) ELSE NULL END

IF ISNULL(@COLUNAS_NAO_EXISTEM2,'''') <> ''''
BEGIN
	RAISERROR (@COLUNAS_NAO_EXISTEM2,16,0)  
	PRINT @COLUNAS_NAO_EXISTEM2
	SELECT @COLUNAS_NAO_EXISTEM2 
	--return @COLUNAS_NAO_EXISTEM2
END
'

 insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- CRIA UMA FLAG DE CONTROLE DAS LINHAS QUE JÁ FORAM ATUALIZADAS
----------------------------------------------------------------------------------------

--DECLARE @tabela_aux VARCHAR(150) = ' + '' + @tabela_aux + '' + '
IF NOT EXISTS (
				SELECT *
				FROM SYS.columns
				WHERE object_id = object_id(' + '''' + @tabela_aux + '''' + ')
				AND name = ''FEITO_LOTE''
			  )
BEGIN
	ALTER TABLE ' + '' + @tabela_aux + '' + ' ADD FEITO_LOTE BIT DEFAULT 0 NOT NULL 
END
' +
CASE WHEN @executar = 0 THEN 
'GO' ELSE '' END 


insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- CRIA ÍNDICE PARA MELHOR ATENDER A QUERY 
----------------------------------------------------------------------------------------
IF NOT EXISTS (
			SELECT *
			FROM SYS.indexes
			WHERE OBJECT_NAME(object_id) = ' + '''' + @tabela_aux + '''' + '
			AND TYPE IN (1,2)
			AND NAME = ''IX_TEMP_LOTE''
		  )							 
BEGIN
CREATE INDEX IX_TEMP_LOTE ON ' +  @tabela_aux + ' (' + @colunas_chave + ', FEITO_LOTE)
END

IF NOT EXISTS (
			SELECT *
			FROM SYS.indexes
			WHERE OBJECT_NAME(object_id) = ' + '''' + @tabela_aux + '''' + '
			AND TYPE IN (1,2)
			AND NAME = ''IX_TEMP_LOTE2''
		  )							 
BEGIN
CREATE INDEX IX_TEMP_LOTE2 ON ' +  @tabela_aux + ' (FEITO_LOTE, ' + @colunas_chave + ')
END
			   
'

-- CRIA TABELA DE BACKUP
----------------------------------------------------------------------------------------
IF @backup = 1
BEGIN

	--DECLARE @tabela_update VARCHAR(150) = 'TABELA1'
	insert into #TEMP_SCRIPTS (QUERY)
	SELECT '
-- CRIA TABELA DE BACKUP
----------------------------------------------------------------------------------------
declare @colunas_backup varchar(8000) = ''''
select  @colunas_backup = @colunas_backup + COLUNA + ''_OLD '' + DATA_TYPE + 
		CASE WHEN DATA_TYPE LIKE ''%CHAR%'' OR DATA_TYPE = ''varbinary'' THEN '' ('' + CONVERT(VARCHAR,CHARACTER_MAXIMUM_LENGTH) + '')''
			WHEN DATA_TYPE IN (''numeric'', ''DECIMAL'',''float'') THEN '' ('' + CONVERT(VARCHAR,NUMERIC_PRECISION) + '','' + CONVERT(VARCHAR,NUMERIC_SCALE) + '')'' ELSE '''' END + 
		'',''
		+ COLUNA + ''_NEW '' + DATA_TYPE + 
		CASE WHEN DATA_TYPE LIKE ''%CHAR%'' OR DATA_TYPE = ''varbinary'' THEN '' ('' + CONVERT(VARCHAR,CHARACTER_MAXIMUM_LENGTH) + '')''
			WHEN DATA_TYPE IN (''numeric'', ''DECIMAL'',''float'') THEN '' ('' + CONVERT(VARCHAR,NUMERIC_PRECISION) + '','' + CONVERT(VARCHAR,NUMERIC_SCALE) + '')'' ELSE '''' END + 
		'',''
from ##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP A
JOIN INFORMATION_SCHEMA.COLUMNS INFCOL ON INFCOL.COLUMN_NAME = A.COLUNA
	AND TABLE_NAME = ' + '''' + CASE WHEN @tabela_update NOT LIKE '%.%' THEN @tabela_update ELSE SUBSTRING(@tabela_update,CHARINDEX('.',@tabela_update+'.')+1,LEN(@tabela_update)) END + '''' + '
order by a.id
		
declare @colunas_chave_BKP varchar(8000) = ''''
select  @colunas_chave_BKP = @colunas_chave_BKP + COLUNA + '' '' + DATA_TYPE + 
		CASE WHEN DATA_TYPE LIKE ''%CHAR%'' OR DATA_TYPE = ''varbinary'' THEN '' ('' + CONVERT(VARCHAR,CHARACTER_MAXIMUM_LENGTH) + '')''
			WHEN DATA_TYPE IN (''numeric'', ''DECIMAL'',''float'') THEN '' ('' + CONVERT(VARCHAR,NUMERIC_PRECISION) + '','' + CONVERT(VARCHAR,NUMERIC_SCALE) + '')'' ELSE '''' END + 
		'',''
from ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP A
JOIN INFORMATION_SCHEMA.COLUMNS INFCOL ON INFCOL.COLUMN_NAME = A.COLUNA
	AND TABLE_NAME =' + '''' + CASE WHEN @tabela_update NOT LIKE '%.%' THEN @tabela_update ELSE SUBSTRING(@tabela_update,CHARINDEX('.',@tabela_update+'.')+1,LEN(@tabela_update)) END + '''' + ' 
order by a.id

select @colunas_chave_BKP = left(@colunas_chave_BKP,len(@colunas_chave_BKP)-1)
select @colunas_backup = left(@colunas_backup,len(@colunas_backup)-1)

DECLARE @ERRO VARCHAR(8000)
IF OBJECT_ID (''' + @tabela_aux + '_BACKUP_UPDATE_LOTE_SP'',''U'') IS NOT NULL
BEGIN 
	SELECT @ERRO = ''A tabela de backup com nome: [' + @tabela_aux + '_BACKUP_UPDATE_LOTE_SP]' + ' ja existe,
será necessário dropar a tabela ou utilizar a @tabela_aux com outro nome''
	RAISERROR (@ERRO,16,0)  
	PRINT @ERRO
	SELECT @ERRO 
END

EXEC (''CREATE TABLE ' + @tabela_aux + '_BACKUP_UPDATE_LOTE_SP ('' + @colunas_chave_BKP + '','' + @colunas_backup + '' )'')'

END



SELECT @query = '
-- MONTA O UPDATE POR LOTE
----------------------------------------------------------------------------------------
declare @LIGACAO varchar(8000) = ''''
declare @colunas_cross varchar(8000) = ''''
declare @script_update varchar(8000) = ''''
declare @script_bkp varchar(8000) = ''''

select @LIGACAO = @LIGACAO + script_ligacao + char(13) + char(10) + char(9),
	   @colunas_cross = @colunas_cross + coluna + '','',
	   @script_bkp = @script_bkp + ''DELETED.'' + coluna + '','' + char(13) + char(10) + char(9)
from  ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP
order by id

select @script_update = @script_update + script_update + '','' + char(13) + char(10) + char(9),
	   @script_bkp	= @script_bkp + script_backup + char(13) + char(10) + char(9),
	   @colunas_cross = @colunas_cross + coluna + '',''
from  ##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP
order by id

select @colunas_cross = left(@colunas_cross,len(@colunas_cross)-1) 
select @script_bkp = left(@script_bkp,len(@script_bkp)-4) 
select @script_update = left(@script_update,len(@script_update)-4) 

DECLARE @QUERY VARCHAR(MAX) = ''''
select @QUERY = ''
-- EXIBINDO O UPDATE POR LOTE
----------------------------------------------------------------------------------------
SET NOCOUNT ON
DECLARE @QNTD_REGISTROS INT
		,@TEMPO_DELETE VARCHAR(4000) = '''''''' 
		,@TEMPO_INICIO DATETIME2 = NULL
		,@MSG VARCHAR(4000) = ''''''''
WHILE (SELECT COUNT(*) FROM ' + @tabela_aux + ' WHERE FEITO_LOTE = 0) > 0
BEGIN

	SELECT @QNTD_REGISTROS = COUNT(*)
	FROM ' + @tabela_aux + ' WHERE FEITO_LOTE = 0	
	
	SELECT @MSG = ''''Quantidade de registros restantes: '''' + CONVERT(VARCHAR(10),@QNTD_REGISTROS )
	RAISERROR( @MSG ,0,1) WITH NOWAIT 

	SELECT @TEMPO_INICIO = GETDATE()

	UPDATE A
	SET '' + @script_update + '' ' + case when @backup = 1 then '
	output
	'' + @script_bkp + ''
	into ' + @tabela_aux + '_BACKUP_UPDATE_LOTE_SP' else '' end + '
	FROM ' + @tabela_update + ' A
	CROSS APPLY (SELECT TOP ' + @lote + ' '' + @colunas_cross + ''' + ' 
				 FROM ' + @tabela_aux + ' AUX
				 where FEITO_LOTE = 0
				 ORDER BY ' + @colunas_chave + ') B
	WHERE 1=1
	' + ''' + @LIGACAO + ''' + '

	UPDATE A
	SET FEITO_LOTE = 1
	FROM ' + @tabela_aux + ' A
	CROSS APPLY (SELECT TOP ' + @lote + ' '' + @colunas_cross + ''' + ' 
				 FROM ' + @tabela_aux + ' AUX2
				 where FEITO_LOTE = 0
				 ORDER BY ' + @colunas_chave + ') B
	WHERE 1=1
	' + ''' + @LIGACAO + ''' + '

	SELECT @TEMPO_DELETE = CONVERT(VARCHAR(10),DATEDIFF(HH,@TEMPO_INICIO,GETDATE()) % 60) + '''' Hora : '''' +
CONVERT(VARCHAR(10),DATEDIFF(MI,@TEMPO_INICIO,GETDATE()) % 60) + '''' Min : '''' +
CONVERT(VARCHAR(10),DATEDIFF(SS,@TEMPO_INICIO,GETDATE()) % 60) + '''' Seg : '''' + 
CONVERT(VARCHAR(10),DATEDIFF(MS,@TEMPO_INICIO,GETDATE()) % 1000) + '''' Ms''''

SELECT @MSG = ''''	- Tempo execução do lote: '''' + @TEMPO_DELETE	
RAISERROR( @MSG ,0,1) WITH NOWAIT 

END	-- fim do while

SELECT @QNTD_REGISTROS = COUNT(*)
FROM ' + @tabela_aux + ' WHERE FEITO_LOTE = 0	

SELECT @MSG = ''''Quantidade de registros restantes: '''' + CONVERT(VARCHAR(10),@QNTD_REGISTROS )
RAISERROR(@MSG, 0,1) WITH NOWAIT
-----------------------------------------------------------------------------------------------------
IF ' + convert(varchar,@backup) + ' = 1
BEGIN
PRINT CHAR(10) + CHAR(13) + ''''Para consultar a tabela de backup: SELECT * FROM ['' ' + '+''' + @tabela_aux + '''+' + '''_BACKUP_UPDATE_LOTE_SP]''' + ''' + CHAR(10) + CHAR(13)   
END
' +
'''
'
+ CASE WHEN @executar = 0 THEN '
print (@QUERY) --EXIBE O UPDATE EM LOTE SEM EXECUTAR
' WHEN @executar = 1 THEN 
'EXEC (@QUERY)' END + ' 

IF OBJECT_ID (''tempdb..##TEMP_STRING_SPLIT_UPDATE_LOTE_SP'') IS NOT NULL
DROP TABLE ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP

IF OBJECT_ID (''tempdb..##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP'') IS NOT NULL
DROP TABLE ##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP
'

INSERT INTO #TEMP_SCRIPTS (QUERY)
SELECT @query


IF @executar = 1
BEGIN
	
	WHILE (SELECT COUNT(*) FROM #TEMP_SCRIPTS WHERE FEITO = 0) > 0
	BEGIN
		
	   SELECT TOP 1 @ID = ID, @QUERY = query, @DT_INICIO = GETDATE()
	   FROM #TEMP_SCRIPTS
	   WHERE FEITO = 0

	   UPDATE #TEMP_SCRIPTS
	   SET DT_INICIO_EXEC = GETDATE()
	   FROM #TEMP_SCRIPTS
	   WHERE ID = @ID

	   EXEC (@QUERY)

	   UPDATE #TEMP_SCRIPTS
	   SET FEITO = 1,
		   SUCESSO = 1,
		   DT_FIM_EXEC = GETDATE()
	   FROM #TEMP_SCRIPTS
	   WHERE ID = @ID

	END
END

SELECT case when @executar = 1 then 'UPDATE executado com sucesso! Resumo abaixo:'
else 'Scrips criados com sucesso! Copie a coluna query logo abaixo.' END as STATUS

SELECT *
FROM #TEMP_SCRIPTS

IF @backup = 1 and @executar = 1
BEGIN
SELECT 'Para consultar a tabela de backup: SELECT * FROM [' + @tabela_aux + '_BACKUP_UPDATE_LOTE_SP]' AS [CONSULTAR TABELA DE BACKUP]
END

IF OBJECT_ID ('tempdb..#TEMP_SCRIPTS') IS NOT NULL
DROP TABLE #TEMP_SCRIPTS

END TRY
BEGIN CATCH

PRINT N'Error Message = ' + CAST(ERROR_MESSAGE() AS varchar(8000));

END CATCH

IF OBJECT_ID ('tempdb..#TEMP_SCRIPTS') IS NOT NULL
DROP TABLE #TEMP_SCRIPTS

END --IF @COMANDO = 'UPDATE'

END -- FIM DA PROCEDURE