use tempdb
go
----------------------------------------------------------------------------------------------------------------------------------
-- tabelas teste
----------------------------------------------------------------------------------------------------------------------------------
set nocount on

drop table if exists tabela1


create table tabela1 (
id_tabela1 int	IDENTITY(1,1)  Primary key not null 
,serie varchar(10) --primary key not null 
,VALOR varchar(10)
,codigo varchar(10)
,data datetime 
)


--select CONVERT(int, CRYPT_GEN_RANDOM(1))

-- popula a tabela 1
set nocount on
declare @i int = 0
while @i < 10000
begin
INSERT INTO tabela1 (serie,VALOR,codigo,data) VALUES ( CONVERT(VARCHAR(10),CONVERT(VARCHAR(255),NEWID())),
							 CONVERT(VARCHAR(10),CONVERT(VARCHAR(255),NEWID())),
							 CONVERT(VARCHAR(10),CONVERT(VARCHAR(255),NEWID())),
							 DATEADD(DAY, CONVERT(int, CRYPT_GEN_RANDOM(2)), '1995-01-01T00:00:00')
						  )
set @i = @i +1 
end
go

IF OBJECT_ID('tabela1_aux') IS NOT NULL
DROP TABLE tabela1_aux
go
CREATE TABLE tabela1_aux (SERIE VARCHAR(10), ID_TABELA1 INT, VALOR VARCHAR(10),codigo varchar(10),data datetime )
INSERT INTO tabela1_aux
SELECT 
serie
,id_tabela1
,CONVERT(VARCHAR(10),CONVERT(VARCHAR(255),NEWID()))
,CONVERT(VARCHAR(10),CONVERT(VARCHAR(255),NEWID()))
,DATEADD(DAY, CONVERT(int, CRYPT_GEN_RANDOM(2)), '1995-01-01T00:00:00')
FROM TABELA1


go





