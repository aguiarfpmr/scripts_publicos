use tempdb
go
----------------------------------------------------------------------------------------------------------------------------------
-- tabelas teste
----------------------------------------------------------------------------------------------------------------------------------
set nocount on

drop table if exists tabela6

drop table if exists tabela5

drop table if exists tabela4

drop table if exists tabela3

drop table if exists tabela2

drop table if exists tabela1


create table tabela1 (serie varchar(10) --primary key not null, 
,id_tabela1 int)

--create table tabela2 (id_tabela2 int identity(1,1) primary key not null, serie2 varchar(10) --FILHA da tabela1 
--constraint 	fk_serie foreign key (serie2) references tabela1 (serie)
--)

--create table tabela3 (id_tabela3 int identity(1,1) primary key not null, serie varchar(3) DEFAULT NULL, id int, id_tabela2 INT	--FILHA da tabela2 - SEM FK COM A SERIE 
--constraint 	fk_id_pk foreign key (id_tabela2) references tabela2 (id_tabela2)
--)

--create table tabela4 (id_tabela4 int, serie varchar(3) , id int, id_tabela3 INT 	 --FILHA da tabela2 - pk composta com a coluna SERIE
--CONSTRAINT 	PK_COMPOSTA PRIMARY KEY (id_tabela4, serie)
--constraint 	fk_id_pk2 foreign key (id_tabela3) references tabela3 (id_tabela3)
--)

CREATE TABLE TABELA5 (ID INT, SERIE VARCHAR(3))
go
CREATE TABLE TABELA6 (ID INT)
go


-- popula a tabela 1
set nocount on
INSERT INTO tabela1 VALUES ( CONVERT(VARCHAR(10),CONVERT(VARCHAR(255),NEWID())),
							 CONVERT(INT,(RAND()*1000))
						  )
GO 10000

IF OBJECT_ID('TEMPDB..tabela1_aux') IS NOT NULL
DROP TABLE tabela1_aux
go
CREATE TABLE tabela1_aux (SERIE VARCHAR(10), ID_TABELA1 INT)
INSERT INTO tabela1_aux
SELECT * FROM TABELA1


go
----------------------------------- começo da proc

--create procedure delete_lote_sp
--(

--	 @tabela_delete	varchar(50)
--	,@tabela_aux	varchar(50)
--	,@colunas_chave	varchar(150) --SEPARADO POR VÍRGULAS
--	,@lote int
--	,@executar bit = 0 -- 0 = apenas exibe o script final / 1 = executa o delete por lote
--)
--as begin

/* excluir */
declare 
 @tabela_delete	varchar(50)
,@tabela_aux	varchar(50)
,@colunas_chave	varchar(150) 
,@lote int
,@executar bit = 0

set @tabela_delete = 'tabela1'
set @tabela_aux = 'tabela1_aux'
set @colunas_chave = 'serie,id_tabela1'
set @lote = 1000
/************************************************/
 
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

-- REALIZA O SPLIT STRING DA @colunas_chave
----------------------------------------------------------------------------------------
IF OBJECT_ID('TEMP_STRING_SPLIT_delete_lote_sp') IS NOT NULL
DROP TABLE TEMP_STRING_SPLIT_delete_lote_sp

CREATE TABLE TEMP_STRING_SPLIT_delete_lote_sp
(
ID INT,
COLUNA VARCHAR(100),
script_ligacao VARCHAR(8000)
)

insert into #TEMP_SCRIPTS (QUERY)
SELECT '
-- REALIZA O SPLIT STRING DA @colunas_chave
----------------------------------------------------------------------------------------
IF OBJECT_ID(''TEMP_STRING_SPLIT_delete_lote_sp'') IS NOT NULL
DROP TABLE TEMP_STRING_SPLIT_delete_lote_sp

CREATE TABLE TEMP_STRING_SPLIT_delete_lote_sp
(
ID INT,
COLUNA VARCHAR(100),
script_ligacao VARCHAR(8000)
)

declare @colunas_chave	varchar(150) = ' + '''' + @colunas_chave + '''' + '
;with CTE as 
(
	select
		id = 1,
		len_string = len(@colunas_chave) + 1,
		ini = 1,
		fim = coalesce(nullif(charindex('','', @colunas_chave, 1), 0), len(@colunas_chave) + 1),
		elemento = ltrim(rtrim(substring(@colunas_chave, 1, coalesce(nullif(charindex('','', @colunas_chave, 1), 0), len(@colunas_chave) + 1)-1)))
	UNION ALL
	select
		id + 1,
		len(@colunas_chave) + 1,
		convert(int, fim) + 1,
		coalesce(nullif(charindex('','', @colunas_chave, fim + 1), 0), len_string), 
		ltrim(rtrim(substring(@colunas_chave, fim + 1, coalesce(nullif(charindex('','', @colunas_chave, fim + 1), 0), len_string)-fim-1)))
	from CTE where fim < len_string
)
INSERT INTO TEMP_STRING_SPLIT_delete_lote_sp
SELECT 
id, 
COLUNA = elemento,
script_ligacao = ''and A.'' + elemento + '' = B.'' + elemento
FROM CTE
option (maxrecursion 0)
'

--SELECT *
--FROM #TEMP_SCRIPTS

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
	FROM TEMP_STRING_SPLIT_delete_lote_sp TEMP
	WHERE NOT EXISTS (
						SELECT *
						FROM INFORMATION_SCHEMA.COLUMNS IS_COL
						WHERE IS_COL.COLUMN_NAME = TEMP.COLUNA
						AND IS_COL.TABLE_NAME = @tabela_delete)
	
	SELECT @COLUNAS_NAO_EXISTEM = ''As seguintes colunas chaves não existem na @tabela_delete: '' + CASE WHEN NULLIF(@COLUNAS_NAO_EXISTEM,'''') IS NOT NULL THEN LEFT(@COLUNAS_NAO_EXISTEM,LEN(@COLUNAS_NAO_EXISTEM)-1) ELSE NULL END

	IF ISNULL(@COLUNAS_NAO_EXISTEM,'''') <> ''''
	BEGIN
	   RAISERROR (@COLUNAS_NAO_EXISTEM,16,0)
	END


END-- FIM DO EXISTS
-- FIM DA VALIDAÇÃO TABELA HEAP
-- COMEÇO DA VALIDAÇÃO TABELA CLUSTERED
ELSE
BEGIN
	
	--DECLARE @tabela_delete VARCHAR(150) = ''TABELA4''
	--DECLARE @COLUNAS_NAO_EXISTEM VARCHAR(8000) = '''' 
	SELECT @COLUNAS_NAO_EXISTEM = @COLUNAS_NAO_EXISTEM + COLUNA + '', ''
	FROM TEMP_STRING_SPLIT_delete_lote_sp TEMP
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

	SELECT @COLUNAS_NAO_EXISTEM = ''As seguintes colunas chaves não existem na @tabela_delete: '' + CASE WHEN NULLIF(@COLUNAS_NAO_EXISTEM,'''') IS NOT NULL THEN LEFT(@COLUNAS_NAO_EXISTEM,LEN(@COLUNAS_NAO_EXISTEM)-1) ELSE NULL END

	IF ISNULL(@COLUNAS_NAO_EXISTEM,'''') <> ''''
	BEGIN
	   RAISERROR (@COLUNAS_NAO_EXISTEM,16,0)
	END
						

END	--FIM DO ELSE
-- FIM DA VALIDAÇÃO TABELA CLUSTERED
'

--SELECT *
--FROM #TEMP_SCRIPTS

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
	ALTER TABLE ' + '' + @tabela_aux + '' + ' ADD FEITO_LOTE BIT 
END' +
CASE WHEN @executar = 0 THEN 
'GO' ELSE '' END 


--select *
--from #TEMP_SCRIPTS



SELECT @query = '
-- MONTA O DELETE POR LOTE
----------------------------------------------------------------------------------------
declare @LIGACAO varchar(8000) = ''''

select @LIGACAO = @LIGACAO + script_ligacao + char(13) + char(10) + char(9)
from  TEMP_STRING_SPLIT_delete_lote_sp

--select @LIGACAO

DECLARE @QUERY VARCHAR(MAX) = ''''
select @QUERY = ''
-- EXIBINDO O DELETE POR LOTE
----------------------------------------------------------------------------------------
SET NOCOUNT ON
DECLARE @QNTD_REGISTROS INT
WHILE (SELECT COUNT(*) FROM ' + @tabela_aux + ' WHERE ISNULL(FEITO_LOTE,0) = 0) > 0
BEGIN

	SELECT @QNTD_REGISTROS = COUNT(*)
	FROM ' + @tabela_aux + ' WHERE ISNULL(FEITO_LOTE,0) = 0	

	PRINT ''''Quantidade de registros restantes: '''' + CONVERT(VARCHAR(10),@QNTD_REGISTROS )

	DELETE tabela1
	FROM ' + @tabela_delete + ' A
	CROSS APPLY (SELECT TOP 1000 *
				 FROM ' + @tabela_aux + ' AUX
				 where ISNULL(FEITO_LOTE,0) = 0
				 ORDER BY serie) B
	WHERE 1=1
	' + ''' + @LIGACAO + ''' + '
	

	UPDATE A
	SET FEITO_LOTE = 1
	FROM ' + @tabela_aux + ' A
	CROSS APPLY (SELECT TOP 1000 *
				 FROM ' + @tabela_aux + ' AUX2
				 where ISNULL(FEITO_LOTE,0) = 0
				 ORDER BY AUX2.serie) B
	WHERE 1=1
	' + ''' + @LIGACAO + ''' + '
	

END	-- fim do while

SELECT @QNTD_REGISTROS = COUNT(*)
FROM ' + @tabela_aux + ' WHERE ISNULL(FEITO_LOTE,0) = 0	

PRINT ''''Quantidade de registros restantes: '''' + CONVERT(VARCHAR(10),@QNTD_REGISTROS )
' +
'''
'
+ CASE WHEN @executar = 0 THEN '
SELECT (@QUERY) --EXIBE O DELETE EM LOTE SEM EXECUTAR
' WHEN @executar = 1 THEN 
'EXEC (@QUERY)' END + ' 

IF OBJECT_ID (''TEMP_STRING_SPLIT_delete_lote_sp'') IS NOT NULL
DROP TABLE TEMP_STRING_SPLIT_delete_lote_sp
'

INSERT INTO #TEMP_SCRIPTS (QUERY)
SELECT @query
--select *
--from #TEMP_SCRIPTS

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
PRINT '

Para visualizar o tempo de execução execute: 

SELECT *
FROM #TEMP_SCRIPTS'

END

if @executar = 0
SELECT *
FROM #TEMP_SCRIPTS 

--end -- fim da proc



------------------------------

 
-- DROP TABLE TEMP_STRING_SPLIT_delete_lote_sp