USE DTS_TOOLS
GO

-- Cria Fun��o split string
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

----------------------------------- come�o da proc
if OBJECT_ID('update_lote_sp','P') IS NULL
BEGIN
EXEC ('create procedure update_lote_sp AS RETURN 0')
END
GO

ALTER procedure update_lote_sp
(

	 @tabela_update	 varchar(50)
	,@tabela_aux	 varchar(50)
	,@colunas_chave	 varchar(150) --SEPARADO POR V�RGULAS
	,@colunas_update varchar(150) --SEPARADO POR V�RGULAS
	,@lote			 VARCHAR(10)
	,@backup		 bit = 0 -- 0 = N�o guarda backup / 1 = Cria uma tabela de backup com os valores OLD e NEW
	,@executar		 bit = 0 -- 0 = apenas exibe o script final / 1 = executa o UPDATE por lote
)
as begin

BEGIN TRY 


set nocount on
declare @query varchar(max) = ''

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
		(CONVERT(VARCHAR(10),DATEDIFF(HH,DT_INICIO_EXEC,DT_FIM_EXEC)) + ':' +
		 CONVERT(VARCHAR(10),DATEDIFF(MI,DT_INICIO_EXEC,DT_FIM_EXEC)) + ':' +
		 CONVERT(VARCHAR(10),DATEDIFF(SS,DT_INICIO_EXEC,DT_FIM_EXEC)) + ':' +
		 CONVERT(VARCHAR(10),DATEDIFF(MS,DT_INICIO_EXEC,DT_FIM_EXEC)))
)

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
-- SE A TABELA FOR HEAP, AS COLUNAS DO @colunas_chave S� PRECISAM EXISTIR NA @tabela_update

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
						AND IS_COL.TABLE_NAME = @tabela_update)
	
	SELECT @COLUNAS_NAO_EXISTEM = ''As seguintes colunas chaves n�o existem na @tabela_update: '' + CASE WHEN NULLIF(@COLUNAS_NAO_EXISTEM,'''') IS NOT NULL THEN QUOTENAME(LEFT(@COLUNAS_NAO_EXISTEM,LEN(@COLUNAS_NAO_EXISTEM)-1)) ELSE NULL END

	IF ISNULL(@COLUNAS_NAO_EXISTEM,'''') <> ''''
	BEGIN
	   RAISERROR (@COLUNAS_NAO_EXISTEM,16,0)  
	   PRINT @COLUNAS_NAO_EXISTEM
	   SELECT @COLUNAS_NAO_EXISTEM 
	   --return @COLUNAS_NAO_EXISTEM
	END


END-- FIM DO EXISTS
-- FIM DA VALIDA��O TABELA HEAP
-- COME�O DA VALIDA��O TABELA CLUSTERED
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
						WHERE OBJECT_NAME(SI.object_id) = @tabela_update
						AND SI.type = 1	 --CLUSTERED
						AND SC.name = TEMP.COLUNA
					 )

	--SELECT @COLUNAS_NAO_EXISTEM

	SELECT @COLUNAS_NAO_EXISTEM = ''As seguintes colunas n�o s�o uma primary key da @tabela_update: '' + CASE WHEN NULLIF(@COLUNAS_NAO_EXISTEM,'''') IS NOT NULL THEN QUOTENAME(LEFT(@COLUNAS_NAO_EXISTEM,LEN(@COLUNAS_NAO_EXISTEM)-1)) ELSE NULL END

	IF ISNULL(@COLUNAS_NAO_EXISTEM,'''') <> ''''
	BEGIN
	   RAISERROR (@COLUNAS_NAO_EXISTEM,16,0)  
	   PRINT @COLUNAS_NAO_EXISTEM
	   SELECT @COLUNAS_NAO_EXISTEM 
	END
						

END	--FIM DO ELSE
-- FIM DA VALIDA��O TABELA CLUSTERED
'

-- VERIFICA SE A TABELA UPDATE POSSUI AS COLUNAS QUE SER�O ATUALIZDAS INDICADAS NO @colunas_chave
----------------------------------------------------------------------------------------
insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- VERIFICA SE A TABELA UPDATE POSSUI AS COLUNAS QUE SER�O ATUALIZDAS INDICADAS NO @colunas_chave
----------------------------------------------------------------------------------------
DECLARE @tabela_update2 VARCHAR(150) = ' + '''' + @tabela_update + '''' + '
DECLARE @COLUNAS_NAO_EXISTEM2 VARCHAR(8000) = ''''
	
SELECT @COLUNAS_NAO_EXISTEM2 = @COLUNAS_NAO_EXISTEM2 + COLUNA + '', ''
FROM ##TEMP_STRING_SPLIT_2_UPDATE_LOTE_SP TEMP
WHERE NOT EXISTS (
					SELECT *
					FROM INFORMATION_SCHEMA.COLUMNS IS_COL
					WHERE IS_COL.COLUMN_NAME = TEMP.COLUNA
					AND IS_COL.TABLE_NAME = @tabela_update2)
	
SELECT @COLUNAS_NAO_EXISTEM2 = ''As seguintes colunas que ser�o atualizadas n�o existem na @tabela_update2: '' + CASE WHEN NULLIF(@COLUNAS_NAO_EXISTEM2,'''') IS NOT NULL THEN QUOTENAME(LEFT(@COLUNAS_NAO_EXISTEM2,LEN(@COLUNAS_NAO_EXISTEM2)-1)) ELSE NULL END

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
-- CRIA UMA FLAG DE CONTROLE DAS LINHAS QUE J� FORAM ATUALIZADAS
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
-- CRIA �NDICE PARA MELHOR ATENDER A QUERY 
----------------------------------------------------------------------------------------
IF EXISTS (
			SELECT *
			FROM SYS.indexes
			WHERE OBJECT_NAME(object_id) = ' + '''' + @tabela_aux + '''' + '
			AND TYPE IN (1,2)
			AND NAME = ''IX_TEMP_LOTE''
		  )							 
BEGIN
DROP INDEX IX_TEMP_LOTE ON ' +  @tabela_aux + ' 
END

IF EXISTS (
			SELECT *
			FROM SYS.indexes
			WHERE OBJECT_NAME(object_id) = ' + '''' + @tabela_aux + '''' + '
			AND TYPE IN (1,2)
			AND NAME = ''IX_TEMP_LOTE2''
		  )							 
BEGIN
DROP INDEX IX_TEMP_LOTE2 ON ' +  @tabela_aux + ' 
END

CREATE INDEX IX_TEMP_LOTE ON ' +  @tabela_aux + ' (' + @colunas_chave + ') INCLUDE (FEITO_LOTE)
CREATE INDEX IX_TEMP_LOTE2 ON ' +  @tabela_aux + ' (FEITO_LOTE) INCLUDE (' + @colunas_chave + ')
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
	AND TABLE_NAME = ' + '''' + @tabela_update + '''' + '
order by a.id
		
declare @colunas_chave_BKP varchar(8000) = ''''
select  @colunas_chave_BKP = @colunas_chave_BKP + COLUNA + '' '' + DATA_TYPE + 
		CASE WHEN DATA_TYPE LIKE ''%CHAR%'' OR DATA_TYPE = ''varbinary'' THEN '' ('' + CONVERT(VARCHAR,CHARACTER_MAXIMUM_LENGTH) + '')''
			WHEN DATA_TYPE IN (''numeric'', ''DECIMAL'',''float'') THEN '' ('' + CONVERT(VARCHAR,NUMERIC_PRECISION) + '','' + CONVERT(VARCHAR,NUMERIC_SCALE) + '')'' ELSE '''' END + 
		'',''
from ##TEMP_STRING_SPLIT_UPDATE_LOTE_SP A
JOIN INFORMATION_SCHEMA.COLUMNS INFCOL ON INFCOL.COLUMN_NAME = A.COLUNA
	AND TABLE_NAME =' + '''' + @tabela_update + '''' + ' 
order by a.id

select @colunas_chave_BKP = left(@colunas_chave_BKP,len(@colunas_chave_BKP)-1)
select @colunas_backup = left(@colunas_backup,len(@colunas_backup)-1)

DECLARE @ERRO VARCHAR(8000)
IF OBJECT_ID (''' + @tabela_aux + '_BACKUP_UPDATE_LOTE_SP'',''U'') IS NOT NULL
BEGIN 
	SELECT @ERRO = ''A tabela de backup com nome: [' + @tabela_aux + '_BACKUP_UPDATE_LOTE_SP]' + ' ja existe,
ser� necess�rio dropar a tabela ou utilizar a @tabela_aux com outro nome''
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
WHILE (SELECT COUNT(*) FROM ' + @tabela_aux + ' WHERE FEITO_LOTE = 0) > 0
BEGIN

	SELECT @QNTD_REGISTROS = COUNT(*)
	FROM ' + @tabela_aux + ' WHERE FEITO_LOTE = 0	

	PRINT ''''Quantidade de registros restantes: '''' + CONVERT(VARCHAR(10),@QNTD_REGISTROS )

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
	

END	-- fim do while

SELECT @QNTD_REGISTROS = COUNT(*)
FROM ' + @tabela_aux + ' WHERE FEITO_LOTE = 0	

PRINT ''''Quantidade de registros restantes: '''' + CONVERT(VARCHAR(10),@QNTD_REGISTROS )
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

DECLARE 
@ID INT,
@DT_INICIO SMALLDATETIME

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

end -- fim da proc
GO

IF OBJECT_ID ('tempdb..#TEMP_SCRIPTS') IS NOT NULL
DROP TABLE #TEMP_SCRIPTS