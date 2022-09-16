DECLARE @TABELA_PAI VARCHAR(800) = 'TABELA1'; --INFORMAR A TABELA QUE DESEJA IDENTIFICAR OS FILHOS, NETOS ETC...
DECLARE @SCHEMA_PAI VARCHAR(800) = 'DBO'; --INFORMAR O SCHEMA DA TABELA PAI
DECLARE @query VARCHAR(MAX) = ''; --N�O ALTERAR
DECLARE @resultado_texto BIT = 0;

SET NOCOUNT ON;
----------------------------------------
-- DAQUI PARA BAIXO N�O ALTERAR
----------------------------------------

IF OBJECT_ID('temp_tabelas') IS NOT NULL
    DROP TABLE temp_tabelas;
IF OBJECT_ID('TEMP_RELACIONAMENTOS') IS NOT NULL
    DROP TABLE TEMP_RELACIONAMENTOS;

CREATE TABLE temp_tabelas
(
    oType     INT
  , oObjName  NVARCHAR(4000)
  , oOwner    NVARCHAR(100)
  , oSequence INT
);

CREATE TABLE TEMP_RELACIONAMENTOS
(
    ID                INT         IDENTITY(1, 1)
  , NOME_TABELA       VARCHAR(800)
  , SCHEMA_TABELA     VARCHAR(800)
  , TABELA_DEPENDENTE VARCHAR(800)
  , NIVEL             INT
  , FEITO             INT
        DEFAULT 0
);

DECLARE @NIVEL INT = 1;
DECLARE @ROWCOUNT INT = 1;
DECLARE @ID INT = 0;

SELECT @query = '
INSERT INTO temp_tabelas
EXEC sp_MSdependencies N''' + @SCHEMA_PAI + '.' + @TABELA_PAI + ''', 8,1315327
';
--PRINT @query;
EXEC (@query);

--SELECT *
--FROM dbo.temp_tabelas;

DELETE temp_tabelas
FROM dbo.temp_tabelas
WHERE oType <> 8;

INSERT INTO TEMP_RELACIONAMENTOS
(
    NOME_TABELA
  , SCHEMA_TABELA
  , TABELA_DEPENDENTE
  , NIVEL
)
SELECT NOME_TABELA       = @TABELA_PAI
     , SCHEMA_TABELA     = oOwner
     , TABELA_DEPENDENTE = oObjName
     , NIVEL             = @NIVEL
FROM temp_tabelas A
WHERE NOT EXISTS
(
    SELECT 1 FROM dbo.TEMP_RELACIONAMENTOS B WHERE @TABELA_PAI = B.NOME_TABELA
);

TRUNCATE TABLE dbo.temp_tabelas;

--SELECT * FROM dbo.TEMP_RELACIONAMENTOS

--------------------------------------------
-- loop
--------------------------------------------
WHILE @ROWCOUNT <> 0
BEGIN

    SELECT TOP 1
           @SCHEMA_PAI = SCHEMA_TABELA
         , @TABELA_PAI = TABELA_DEPENDENTE
         , @ID         = ID
         , @NIVEL      = NIVEL
    FROM TEMP_RELACIONAMENTOS
    WHERE 1 = 1
    AND   FEITO = 0
    --AND  ID = @ID
    ORDER BY ID;

    SELECT @NIVEL = @NIVEL + 1;

    --SELECT @NIVEL AS NIVEL
    --SELECT tabela_atual = @TABELA_PAI;

    SELECT @query = '
INSERT INTO temp_tabelas
EXEC sp_MSdependencies N''' + @SCHEMA_PAI + '.' + @TABELA_PAI + ''', 8,1315327
'   ;
    --PRINT @query;
    EXEC (@query);

    --SELECT *
    --FROM dbo.temp_tabelas;

    DELETE temp_tabelas
    FROM dbo.temp_tabelas
    WHERE oType <> 8;

    INSERT INTO TEMP_RELACIONAMENTOS
    (
        NOME_TABELA
      , SCHEMA_TABELA
      , TABELA_DEPENDENTE
      , NIVEL
    )
    SELECT NOME_TABELA       = @TABELA_PAI
         , SCHEMA_TABELA     = oOwner
         , TABELA_DEPENDENTE = oObjName
         , NIVEL             = @NIVEL
    FROM temp_tabelas A
    WHERE NOT EXISTS
    (
        SELECT 1 FROM dbo.TEMP_RELACIONAMENTOS B WHERE @TABELA_PAI = B.NOME_TABELA
    );

    SELECT @ROWCOUNT = COUNT(*)
    FROM TEMP_RELACIONAMENTOS
    WHERE FEITO = 0;

    --SELECT * FROM dbo.TEMP_RELACIONAMENTOS

    UPDATE A
    SET FEITO = 1
    FROM TEMP_RELACIONAMENTOS A
    WHERE A.ID = @ID;

    TRUNCATE TABLE dbo.temp_tabelas;

END;

----------------------------------------
-- OUTPUT
----------------------------------------
IF @resultado_texto = 0
BEGIN
    SELECT ID
         , NOME_TABELA
         , SCHEMA_TABELA
         , TABELA_DEPENDENTE
         , NIVEL
    FROM dbo.TEMP_RELACIONAMENTOS;
END;

IF @resultado_texto = 1
BEGIN

    UPDATE dbo.TEMP_RELACIONAMENTOS
    SET FEITO = 0;

    PRINT CHAR(10) + 'Hierarquia: ' + REPLICATE(CHAR(10), 2);

    WHILE
    (SELECT COUNT(*)FROM dbo.TEMP_RELACIONAMENTOS WHERE FEITO = 0) > 0
    BEGIN

        SET @query = '';
        SET @ID = 0;

        SELECT TOP 1
               @query = REPLICATE('	'   , (NIVEL * 2)) + NOME_TABELA + CHAR(10) + REPLICATE('	', (NIVEL * 2)) + '|' + CHAR(10) + REPLICATE('	', (NIVEL * 2)) + REPLICATE('-', (NIVEL * NIVEL)) + TABELA_DEPENDENTE + CHAR(10) + REPLICATE('_', 200)
             , @ID    = ID
        FROM dbo.TEMP_RELACIONAMENTOS
        WHERE FEITO = 0
        ORDER BY ID;

        PRINT @query;

        UPDATE dbo.TEMP_RELACIONAMENTOS
        SET FEITO = 1
        FROM dbo.TEMP_RELACIONAMENTOS
        WHERE ID = @ID;
    END;
END;




/*


CREATE TABLE TABELA1 (ID_TABELA1 INT PRIMARY KEY, NOME VARCHAR(10))
GO
CREATE TABLE TABELA2 (ID_TABELA2 INT PRIMARY KEY, ID_TABELA1 INT)
GO
CREATE TABLE TABELA3 (ID_TABELA3 INT PRIMARY KEY, ID_TABELA2 INT)
GO
CREATE TABLE TABELA4 (ID_TABELA4 INT PRIMARY KEY, ID_TABELA3 INT)
GO
CREATE TABLE TABELA1_AUX (ID_TABELA1_AUX INT PRIMARY KEY, ID_TABELA1 INT)
GO
CREATE TABLE TABELA3_AUX (ID_TABELA3_AUX INT PRIMARY KEY, ID_TABELA3 INT)
GO

ALTER TABLE TABELA2 ADD CONSTRAINT FK_TAB1_TABELA2 FOREIGN KEY (ID_TABELA1) REFERENCES dbo.TABELA1 (ID_TABELA1)
GO
ALTER TABLE TABELA3 ADD CONSTRAINT FK_TAB2_TABELA3 FOREIGN KEY (ID_TABELA2) REFERENCES dbo.TABELA2 (ID_TABELA2)
GO
ALTER TABLE TABELA4 ADD CONSTRAINT FK_TAB3_TABELA4 FOREIGN KEY (ID_TABELA3) REFERENCES dbo.TABELA3 (ID_TABELA3)
GO
ALTER TABLE TABELA1_AUX ADD CONSTRAINT FK_TABELA1_TABELA1_AUX FOREIGN KEY (ID_TABELA1) REFERENCES dbo.TABELA1 (ID_TABELA1)
GO
ALTER TABLE TABELA3_AUX ADD CONSTRAINT FK_TABELA1_TABELA3 FOREIGN KEY (ID_TABELA3) REFERENCES dbo.TABELA3 (ID_TABELA3)
*/
