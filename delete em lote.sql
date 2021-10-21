USE DTS_TOOLS
GO

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

----------------------------------- começo da proc
if OBJECT_ID('delete_lote_sp','P') IS NULL
BEGIN
EXEC ('create procedure delete_lote_sp AS RETURN 0')
END
GO

ALTER procedure delete_lote_sp
(

	 @tabela_delete	varchar(50)
	,@tabela_aux	varchar(50)
	,@colunas_chave	varchar(150) --SEPARADO POR VÍRGULAS
	,@lote VARCHAR(10)
	,@executar bit = 0 -- 0 = apenas exibe o script final / 1 = executa o delete por lote
)
as begin

--/* excluir */
--declare 
-- @tabela_delete	varchar(50)
--,@tabela_aux	varchar(50)
--,@colunas_chave	varchar(150) 
--,@lote int
--,@executar bit = 1

--set @tabela_delete = 'tabela1'
--set @tabela_aux = 'tabela1_aux'
--set @colunas_chave = 'serie,aaaaa'--'serie,id_tabela1'
--set @lote = 1000
/************************************************/

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
						WHERE IS_COL.COLUMN_NAME = TEMP.COLUNA
						AND IS_COL.TABLE_NAME = @tabela_delete)
	
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
						WHERE OBJECT_NAME(SI.object_id) = @tabela_delete
						AND SI.type = 1	 --CLUSTERED
						AND SC.name = TEMP.COLUNA
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

	DELETE A
	FROM ' + @tabela_delete + ' A
	CROSS APPLY (SELECT TOP ' + @lote + ' '' + @colunas_cross + ''' + ' 
				 FROM ' + @tabela_aux + ' AUX --WITH (INDEX (IX_TEMP_LOTE2))
				 where FEITO_LOTE = 0
				 ORDER BY ' + @colunas_chave + ') B
	WHERE 1=1
	' + ''' + @LIGACAO + ''' + '
	option(recompile)

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

end -- fim da proc

