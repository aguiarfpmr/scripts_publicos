use tempdb
go
----------------------------------------------------------------------------------------------------------------------------------
-- tabelas teste
----------------------------------------------------------------------------------------------------------------------------------
drop table if exists tabela4

drop table if exists tabela3

drop table if exists tabela2

drop table if exists tabela1

create table tabela1 (serie varchar(3) primary key not null, id_tabela1 int)

create table tabela2 (id_tabela2 int identity(1,1) primary key not null, serie2 varchar(3) --FILHA da tabela1 
constraint 	fk_serie foreign key (serie2) references tabela1 (serie)
)

create table tabela3 (id_tabela3 int identity(1,1) primary key not null, serie varchar(3) DEFAULT NULL, id int, id_tabela2 INT	--FILHA da tabela2 - SEM FK COM A SERIE 
constraint 	fk_id_pk foreign key (id_tabela2) references tabela2 (id_tabela2)
)

create table tabela4 (id_tabela4 int, serie varchar(3) , id int, id_tabela3 INT 	 --FILHA da tabela2 - pk composta com a coluna SERIE
CONSTRAINT 	PK_COMPOSTA PRIMARY KEY (id_tabela4, serie)
constraint 	fk_id_pk2 foreign key (id_tabela3) references tabela3 (id_tabela3)
)

-- popula a tabela 1
INSERT INTO tabela1 VALUES ( CONVERT(VARCHAR(3),CONVERT(VARCHAR(255),NEWID())),
							 CONVERT(INT,(RAND()*1000))
						  )
GO 10

CREATE INDEX IX_TEMP ON tabela1 (SERIE)

CREATE INDEX IX_TEMP2 ON tabela1 (SERIE) INCLUDE (id_tabela1) with (fillfactor = 99) 



DECLARE 
 @nome_coluna_alvo varchar(25) = 'serie' -- informar nome da coluna que será alterada
,@TIPO_DADOS_TAMANHO VARCHAR(50) = 'VARCHAR (5)' -- informar o tipo de dados e o tamanho para qual a coluna será alterada

------------------------------------------------------------------------------------------------------------
-- CRIA TABELA QUE VAI GUARDAR AS QUERYS
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS SCRIPTS_MODIFICACOES 
CREATE TABLE SCRIPTS_MODIFICACOES 
(
	QUERY VARCHAR(MAX),
	NOME_TABELA VARCHAR(150),
	ID INT,
	PESO INT,
	FEITO INT DEFAULT 0
)

------------------------------------------------------------------------------------------------------------
-- buscando as tabelas que possuem a coluna escolhida
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS tempdb..#TABELAS_COLUNA

select 
nome_tabela		= TABLE_NAME,
NOME_COLUNA		= COLUMN_NAME,
tipo_dados		= DATA_TYPE,
CHARACTER_MAXIMUM_LENGTH,
NUMERIC_PRECISION,
NUMERIC_SCALE,
script_alter_column = 'ALTER TABLE ' + TABLE_NAME + ' ALTER COLUMN ' + COLUMN_NAME + ' ' + @TIPO_DADOS_TAMANHO + ' NOT NULL',
ID = OBJ_ID,
rows		= convert(varchar(100),''),
reserved	= convert(varchar(100),''),
data		= convert(varchar(100),''),
index_size	= convert(varchar(100),''),
unused		= convert(varchar(100),'')
INTO #TABELAS_COLUNA
from INFORMATION_SCHEMA.COLUMNS ISC
CROSS APPLY (SELECT TOP 1 object_id	OBJ_ID
				FROM SYS.tables SYSTB
				WHERE SYSTB.name = ISC.TABLE_NAME) CRA
where COLUMN_NAME = 'serie'


------------------------------------------------------------------------------------------------------------
-- buscando as constraint FK
------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS TEMPDB..#SYSFK

select 
NOME_CONSTRAINT				= SYSFK.[name],
ID_NOME_CONSTRAINT			= SYSFK.OBJECT_ID,
NOME_TABELA					= OBJECT_NAME(SYSFK.parent_object_id),
ID_TABELA					= SYSFK.parent_object_id,
NOME_TABELA_REFERENCIA		= OBJECT_NAME(SYSFK.referenced_object_id),
ID_TABELA_REFERENCIA		= SYSFK.referenced_object_id,
TIPO_CONSTRAINT				= SYSFK.[TYPE_DESC],
RN = ROW_NUMBER() OVER (ORDER BY SYSFK.modify_date desc , SYSFK.parent_object_id DESC),
NOME_COLUNA_TABELA			= COL_NAME (SYSFKC.parent_object_id, SYSFKC.parent_column_id),
script_create_fk			= 'ALTER TABLE ' + OBJECT_NAME(SYSFK.parent_object_id) + 
							   ' ADD CONSTRAINT ' + SYSFK.[name] + ' FOREIGN KEY (' + COL_NAME (SYSFKC.parent_object_id, SYSFKC.parent_column_id) + 
							   ') references ' + OBJECT_NAME(SYSFK.referenced_object_id) + ' (' + COL_NAME (SYSFKC.referenced_object_id, SYSFKC.referenced_column_id) + ')'  ,
script_DROP_fk              = 'ALTER TABLE ' + OBJECT_NAME(SYSFK.parent_object_id) + 
							  ' DROP CONSTRAINT ' + SYSFK.[name],
SYSFK.schema_id,
SYSFK.is_ms_shipped,
SYSFK.is_published,
SYSFK.is_schema_published,
SYSFK.key_index_id,
SYSFK.is_disabled,
SYSFK.is_not_for_replication,
SYSFK.is_not_trusted,
SYSFK.delete_referential_action,
SYSFK.delete_referential_action_desc,
SYSFK.update_referential_action,
SYSFK.update_referential_action_desc,
SYSFK.is_system_named
--,''
--,FKC.*
INTO #SYSFK
from sys.foreign_keys SYSFK
INNER JOIN sys.foreign_key_columns SYSFKC  ON SYSFK.OBJECT_ID = SYSFKC.constraint_object_id
--JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ISKCU ON ISKCU.CONSTRAINT_NAME = SYSFK.[name]
WHERE (COL_NAME (SYSFKC.parent_object_id, SYSFKC.parent_column_id)		   = @nome_coluna_alvo         -- nome da coluna que será alterada  
	or COL_NAME (SYSFKC.referenced_object_id, SYSFKC.referenced_column_id) = @nome_coluna_alvo -- nome da coluna que será alterada   
	  )
and exists (	select 1
				from #TABELAS_COLUNA
				where (	#TABELAS_COLUNA.nome_tabela = OBJECT_NAME(SYSFK.parent_object_id) or
						#TABELAS_COLUNA.nome_tabela = OBJECT_NAME(SYSFK.referenced_object_id)
					  )
		   )		



--SELECT *
--FROM SYS.foreign_key_columns

------------------------------------------------------------------------------------------------------------
-- buscando as constraint PK
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS TEMPDB..#SYSPK

select 
NOME_TABELA			= OBJECT_NAME(parent_object_id),
ID_TABELA			= parent_object_id,
NOME_CONSTRAINT		= NAME,
ID_CONSTRAINT		= object_id,
TIPO_CONSTRAINT		= [TYPE_DESC],
RN = ROW_NUMBER() OVER (ORDER BY SYSPK.modify_date desc, SYSPK.parent_object_id DESC),
script_create_PK	= 'alter table ' + OBJECT_NAME(parent_object_id) +
					  ' add constraint ' + SYSPK.NAME + ' primary key (' + stuff(coluna,1,2,'') + ') with (data_compression = page)',
script_drop_PK		= 'alter table ' + OBJECT_NAME(parent_object_id) +
					  ' DROP constraint ' + SYSPK.NAME,
is_ms_shipped,
is_published,
is_schema_published,
unique_index_id,
is_system_named,
is_enforced,
coluna_pk			= stuff(coluna,1,2,'')
INTO #SYSPK
--,''
--,*
from sys.key_constraints SYSPK
CROSS APPLY (SELECT ', ' + ISKCU.COLUMN_NAME
			 FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE ISKCU 
			 WHERE ISKCU.CONSTRAINT_NAME = SYSPK.NAME
			 FOR XML PATH ('') 
			) colunas_pk (coluna) -- REALIZANDO XML PATH, POIS PODE SER UMA CHAVE COMPOSTA
where coluna like '%' + @nome_coluna_alvo + '%'



------------------------------------------------------------------------------------------------------------
-- buscando as default constraints
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS tempdb..#SYS_DF_CONSTR

select 
--constr = 'sys.default_constraints',
NOME_CONSTRAINT = OBJECT_NAME(object_id), 
ID_CONSTRAINT	= OBJECT_ID,
TABELA_CONSTRAINT = OBJECT_NAME(parent_object_id),
COLUNA_CONSTRAINT  = COL_NAME (parent_object_id,parent_column_id),
VALOR_DEFAULT = definition,
script_create_df_constraint = 'ALTER TABLE ' + OBJECT_NAME(parent_object_id) + ' ADD CONSTRAINT ' + NAME +
							   ' DEFAULT ' + REPLACE(REPLACE(definition,'(',''),')','') + ' FOR ' + COL_NAME (parent_object_id,parent_column_id),
script_drop_df_constraint	= 'ALTER TABLE ' + OBJECT_NAME(parent_object_id) +
							  ' DROP CONSTRAINT ' + NAME
--,*
INTO #SYS_DF_CONSTR
from sys.default_constraints
WHERE COL_NAME (parent_object_id,parent_column_id) = @nome_coluna_alvo

------------------------------------------------------------------------------------------------------------
-- buscando as tabelas que possuem uma FK ligada a coluna que está sendo modificada
------------------------------------------------------------------------------------------------------------

insert INTO #TABELAS_COLUNA
select 
nome_tabela		= TABLE_NAME,
NOME_COLUNA		= COLUMN_NAME,
tipo_dados		= DATA_TYPE,
CHARACTER_MAXIMUM_LENGTH,
NUMERIC_PRECISION,
NUMERIC_SCALE,
script_alter_column = 'ALTER TABLE ' + TABLE_NAME + ' ALTER COLUMN ' + COLUMN_NAME + ' ' + @TIPO_DADOS_TAMANHO + ' NOT NULL'
,ID = OBJ_ID,
rows		= convert(varchar(100),''),
reserved	= convert(varchar(100),''),
data		= convert(varchar(100),''),
index_size	= convert(varchar(100),''),
unused		= convert(varchar(100),'')
from INFORMATION_SCHEMA.COLUMNS ISC
CROSS APPLY (SELECT TOP 1 object_id	OBJ_ID
				FROM SYS.tables SYSTB
				WHERE SYSTB.name = ISC.TABLE_NAME) CRA
WHERE 1=1
AND EXISTS	(	select *
				from #SYSFK SYSFK
				where SYSFK.NOME_TABELA = isc.TABLE_NAME
				and SYSFK.NOME_COLUNA_TABELA = ISC.COLUMN_NAME)
AND NOT EXISTS (	SELECT 1
					FROM #TABELAS_COLUNA TMP
					WHERE  TMP.nome_tabela = TABLE_NAME
					AND  NOME_COLUNA = COLUMN_NAME )

------------------------------------------------------------------------------------------------------------
-- VERIFICA SE EXISTE OUTRAS CONSTRAINTS
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS tempdb..#checar_constraint 
select 
constr = 'sys.check_constraints'
into #checar_constraint
from sys.check_constraints
WHERE COL_NAME (parent_object_id,parent_column_id) = @nome_coluna_alvo

if exists (select 1 from #checar_constraint)
begin
	raiserror('existe constraint não esperadas',16,0)
end 

------------------------------------------------------------------------------------------------------------
-- VERIFICA O TAMANHO DAS TABELAS E INDICES
------------------------------------------------------------------------------------------------------------
IF OBJECT_ID('tamanho_tabelas_indice') IS NULL
BEGIN
create table tamanho_tabelas_indice
(
 name		varchar(100)
,rows		varchar(100)
,reserved	varchar(100)
,data		varchar(100)
,index_size	varchar(100)
,unused		varchar(100)
)
END
ELSE
BEGIN
	TRUNCATE TABLE tamanho_tabelas_indice
END

DROP TABLE IF EXISTS TEMPDB..#VERIFICA_TAMNHO

SELECT DISTINCT  
QUERY = 'sp_spaceused ' + nome_tabela
, ID = IDENTITY(INT,1,1)
, FEITO = 0
INTO #VERIFICA_TAMNHO
FROM #TABELAS_COLUNA

DECLARE @QUERY VARCHAR(MAX) = ''
,@ID INT = 0
WHILE (SELECT COUNT(*) FROM #VERIFICA_TAMNHO WHERE FEITO = 0 ) <> 0
BEGIN

	 SELECT TOP 1
	 @QUERY = QUERY	,
	 @ID	= ID
	FROM #VERIFICA_TAMNHO WHERE FEITO = 0 
	
	INSERT INTO tamanho_tabelas_indice
	EXEC (@QUERY)

	UPDATE #VERIFICA_TAMNHO
	SET FEITO = 1
	FROM #VERIFICA_TAMNHO
	WHERE @ID = ID
END

UPDATE #TABELAS_COLUNA 
SET rows		= B.rows		 
	,reserved	= B.reserved	 
	,data		= B.data		 
	,index_size	= B.index_size	 
	,unused		= B.unused		 
FROM #TABELAS_COLUNA A
JOIN tamanho_tabelas_indice B ON A.NOME_COLUNA = B.name

------------------------------------------------------------------------------------------------------------
-- BUSCANDO ÍNDICES
------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS TEMPDB..#INDICES

select SCHEMA_NAME (o.SCHEMA_ID) SchemaName
  ,o.name ObjectName
  ,i.name IndexName
  ,i.type_desc
  ,LEFT(list, ISNULL(splitter-1,len(list))) Columns
  , SUBSTRING(list, indCol.splitter+1, 1000) includedColumns--len(name) - splitter-1) columns
  --, COUNT(1)over (partition by o.object_id)
  , script_create_suggested = 'CREATE ' + I.type_desc + ' INDEX ' + i.name + ' ON ' + o.name +
' ( ' + LEFT(list, ISNULL(splitter-1,len(list))) + ' )' + ISNULL(' INCLUDE ( ' + SUBSTRING(list, indCol.splitter+1, 1000) + ' )','') +
--' ' +  ISNULL(' ,FILLFACTOR = ' + CONVERT(VARCHAR(5), NULLIF(I.fill_factor,0)) + ')' ,'') COLLATE Latin1_General_CI_AI
' ' + 'WITH (FILLFACTOR = 99, DATA_COMPRESSION = PAGE)' COLLATE Latin1_General_CI_AI
  ,script_drop_suggested = 'DROP INDEX ' + i.name + ' ON ' + o.name	 COLLATE Latin1_General_CI_AI
INTO #INDICES
from sys.indexes i
join sys.objects o on i.object_id= o.object_id
cross apply (select NULLIF(charindex('|',indexCols.list),0) splitter , list
             from (select cast((
                          select case when sc.is_included_column = 1 and sc.ColPos= 1 then'|'else '' end +
                                 case when sc.ColPos > 1 then ', ' else ''end + name
                            from (select sc.is_included_column, index_column_id, name
                                       , ROW_NUMBER()over (partition by sc.is_included_column
                                                            order by sc.index_column_id)ColPos
                                   from sys.index_columns  sc
                                   join sys.columns        c on sc.object_id= c.object_id
                                                            and sc.column_id = c.column_id
                                  where sc.index_id= i.index_id
                                    and sc.object_id= i.object_id) sc
                   order by sc.is_included_column
                           ,ColPos
                     for xml path (''),type) as varchar(max)) list)indexCols) indCol
where 1=1
AND i.type_desc <> 'CLUSTERED'
--and indCol.splitter is not null    -- defina se vai trazer os indices com ou sem include
--and o.name = 'tabela3'--tabela
AND (	indCol.splitter LIKE '%' + @nome_coluna_alvo + '%' OR 
		LEFT(list, ISNULL(splitter-1,len(list))) LIKE '%' + @nome_coluna_alvo + '%' 
	)
--order by SchemaName, ObjectName, IndexName
------------------------------------------------------------------------------------------------------------
-- INSERINDO OS SCRIPTS DROP E CREATE E GUARDANDO COMO BKP
------------------------------------------------------------------------------------------------------------
INSERT INTO SCRIPTS_MODIFICACOES
(
 QUERY
,NOME_TABELA
,ID
,PESO
)

SELECT 
 QUERY = '--DROPANDO INDICES'
,NOME_TABELA = ''
,ID	   = 0
,PESO  = 1

UNION 

SELECT 
 QUERY = #INDICES.script_drop_suggested COLLATE Latin1_General_CI_AI
,NOME_TABELA = #INDICES.ObjectName
,ID	   = ROW_NUMBER() OVER (ORDER BY #INDICES.ObjectName DESC )
,PESO  = 1
FROM #INDICES

UNION


SELECT 
 QUERY = '--DROPANDO DEFAULT CONSTRAINT'
,NOME_TABELA = ''
,ID	   = 0
,PESO  = 2

UNION 

SELECT 
 QUERY = #SYS_DF_CONSTR.script_drop_df_constraint
,NOME_TABELA = #SYS_DF_CONSTR.TABELA_CONSTRAINT
,ID	   = ROW_NUMBER() OVER (ORDER BY ID_CONSTRAINT DESC )
,PESO  = 2
FROM #SYS_DF_CONSTR


UNION


SELECT 
 QUERY = '--DROPANDO FOREIGN KEY'
,NOME_TABELA = ''
,ID	   = 0
,PESO  = 3

UNION 

SELECT 
 QUERY = SYSFK.script_DROP_fk
,NOME_TABELA = NOME_TABELA
,ID	   = RN
,PESO  = 3
FROM #SYSFK SYSFK

UNION

SELECT 
 QUERY = '--DROPANDO PRIMARY KEY'
,NOME_TABELA = ''
,ID	   = 0
,PESO  = 4

UNION

SELECT 
 QUERY = SYSPK.script_drop_PK
,NOME_TABELA = NOME_TABELA
,ID	   = RN
,PESO  = 4
FROM #SYSPK SYSPK

UNION

SELECT 
 QUERY = '--ALTERANDO TAMANHO COLUNA'
,NOME_TABELA = ''
,ID	   = 0
,PESO  = 5

UNION

SELECT 
 QUERY = TABELAS_COLUNA.script_alter_column
,NOME_TABELA = NOME_TABELA
,ID	   = TABELAS_COLUNA.ID
,PESO  = 5
FROM #TABELAS_COLUNA  TABELAS_COLUNA

UNION

SELECT 
 QUERY = '--RECRIANDO PRIMARY KEY'
,NOME_TABELA = ''
,ID	   = 0
,PESO  = 6

UNION 

SELECT 
 QUERY = SYSPK.script_create_PK
,NOME_TABELA = NOME_TABELA
,ID	   = ROW_NUMBER() OVER (ORDER BY RN DESC )
,PESO  = 6
FROM #SYSPK SYSPK

UNION

SELECT 
 QUERY = '--RECRIANDO FOREIGN KEY'
,NOME_TABELA = ''
,ID	   = 0
,PESO  = 8

UNION

SELECT 
 QUERY = SYSFK.script_create_fk
,NOME_TABELA = NOME_TABELA
,ID	   = ROW_NUMBER() OVER (ORDER BY RN DESC )
,PESO  = 8
FROM #SYSFK SYSFK

UNION

SELECT 
 QUERY = '--RECRIANDO DEFAULT CONSTRAINT'
,NOME_TABELA = ''
,ID	   = 0
,PESO  = 9

UNION

SELECT 
 QUERY = #SYS_DF_CONSTR.script_create_df_constraint
,NOME_TABELA = #SYS_DF_CONSTR.TABELA_CONSTRAINT
,ID	   = ROW_NUMBER() OVER (ORDER BY ID_CONSTRAINT )
,PESO  = 9
FROM #SYS_DF_CONSTR

UNION

SELECT 
 QUERY = '--RECRIANDO INDICES'
,NOME_TABELA = ''
,ID	   = 0
,PESO  = 10

UNION 

SELECT 
 QUERY = #INDICES.script_create_suggested COLLATE Latin1_General_CI_AI
,NOME_TABELA = #INDICES.ObjectName
,ID	   = ROW_NUMBER() OVER (ORDER BY #INDICES.ObjectName DESC )
,PESO  = 10
FROM #INDICES

-- EXIBE O RESULTADO DO SCRIPT
SELECT *
FROM SCRIPTS_MODIFICACOES A
LEFT JOIN tamanho_tabelas_indice B ON A.NOME_TABELA = B.name
ORDER BY PESO, ID



/*

SELECT *
FROM tamanho_tabelas_indice

select 
'sp_spaceused ' + nome_tabela
FROM #SYSFK SYSFK
	union
select 
'sp_spaceused ' + nome_tabela
FROM #SYSPK SYSPK
	union
select 
'sp_spaceused ' + nome_tabela
from #TABELAS_COLUNA

*/



/*
DECLARE @QUERY VARCHAR(MAX) = '',
@ID INT = 0,
@PESO INT = 0

WHILE (SELECT COUNT(*) FROM SCRIPTS_MODIFICACOES WHERE FEITO = 0 AND ID <> 0) <> 0
BEGIN

	SELECT TOP 1
	 @QUERY = QUERY
	,@ID = ID
	,@PESO = PESO
	FROM SCRIPTS_MODIFICACOES WHERE FEITO = 0 
	AND ID <> 0
	
	EXEC (@QUERY)

	UPDATE SCRIPTS_MODIFICACOES
	SET FEITO = 1
	FROM SCRIPTS_MODIFICACOES
	WHERE @QUERY = QUERY
	AND @ID = ID
	AND @PESO = PESO
END
*/