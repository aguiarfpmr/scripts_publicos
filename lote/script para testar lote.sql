use DTS_TOOLS
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


create table tabela1 (serie varchar(10) --primary key not null 
,id_tabela1 int	IDENTITY(1,1)  Primary key not null 
,VALOR varchar(10)
,codigo varchar(10)
)

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
							 --CONVERT(INT,(RAND()*1000)),
							 CONVERT(VARCHAR(10),CONVERT(VARCHAR(255),NEWID())),
							 CONVERT(VARCHAR(10),CONVERT(VARCHAR(255),NEWID()))
						  )
GO 10000

IF OBJECT_ID('TEMPDB..tabela1_aux') IS NOT NULL
DROP TABLE tabela1_aux
go
CREATE TABLE tabela1_aux (SERIE VARCHAR(10), ID_TABELA1 INT, VALOR VARCHAR(10),codigo varchar(10))
INSERT INTO tabela1_aux
SELECT 
serie
,id_tabela1
,CONVERT(VARCHAR(10),CONVERT(VARCHAR(255),NEWID()))
,CONVERT(VARCHAR(10),CONVERT(VARCHAR(255),NEWID()))
FROM TABELA1


go


