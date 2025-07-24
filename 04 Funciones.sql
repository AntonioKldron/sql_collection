/* =============================================
--  Titulo: fnCA_ValidateTableFieldsExist
--  Author: JOSE ANTONIO CORNELIO CALDERON
--  Creación: 23/07/2025
--  Descripción: Valida que todos los campos especificados existan dentro de una tabla.
--  Referencia: Usa INFORMATION_SCHEMA.COLUMNS para validar existencia de columnas.
--  Ejemplo de uso:
--      SELECT dbo.fnCA_ValidateTableFieldsExist('Usuarios', 'Nombre,Edad')
==============================================*/
CREATE FUNCTION [dbo].[fnCA_ValidateTableFieldsExist] 
(
    @tableName     SYSNAME,
    @fieldList     VARCHAR(MAX)
)
RETURNS BIT
AS
BEGIN
    DECLARE 
        @missingCount INT
    ;WITH FieldList AS (
        SELECT LTRIM(RTRIM(name)) AS ColumnName
        FROM dbo.fnCA_StringSplit(@fieldList, ',')
    )
    SELECT @missingCount = COUNT(*)
    FROM FieldList f
    WHERE NOT EXISTS (
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE 
            c.TABLE_NAME = @tableName
            AND c.COLUMN_NAME = f.ColumnName
    )
    RETURN CASE WHEN @missingCount = 0 THEN 1 ELSE 0 END
END
GO
/* =============================================
--  Titulo: fnCA_AddJsonProperty
--  Author: JOSE ANTONIO CORNELIO CALDERON
--  Creación: 23/07/2025
--  Descripción: Agrega dinámicamente una propiedad a un objeto JSON simple.
--  Referencia: Maneja valores tipo string, int, float, bool, null.
--  Ejemplo de uso:
--      SELECT dbo.fnCA_AddJsonProperty('{"name":"John"}', 'age', '30', 'int')
==============================================*/
CREATE FUNCTION [dbo].[fnCA_AddJsonProperty]
(
    @jsonString     VARCHAR(MAX),
    @propertyName   VARCHAR(100),
    @propertyValue  VARCHAR(MAX),
    @valueType      VARCHAR(20)  -- Supported: 'string', 'int', 'float', 'bool', 'null'
)
RETURNS VARCHAR(MAX)
AS
BEGIN
    DECLARE @modifiedJson VARCHAR(MAX)
    DECLARE @cleanJson VARCHAR(MAX)
    DECLARE @formattedValue VARCHAR(MAX)

    IF RIGHT(LTRIM(RTRIM(@jsonString)), 1) = '}'
    BEGIN
        SET @cleanJson = LEFT(@jsonString, LEN(@jsonString) - 1)

        IF LEN(REPLACE(REPLACE(@cleanJson, '{', ''), ' ', '')) > 0
        BEGIN
            SET @cleanJson = @cleanJson + ','
        END
    END
    ELSE
    BEGIN
        RETURN @jsonString
    END

    IF @valueType = 'string'
        SET @formattedValue = '"' + REPLACE(@propertyValue, '"', '\"') + '"'
    ELSE IF @valueType = 'int'
        SET @formattedValue = @propertyValue
    ELSE IF @valueType = 'float'
        SET @formattedValue = @propertyValue
    ELSE IF @valueType = 'bool'
        SET @formattedValue = CASE WHEN @propertyValue IN ('1', 'true') THEN 'true' ELSE 'false' END
    ELSE IF @valueType = 'null'
        SET @formattedValue = 'null'
    ELSE
        SET @formattedValue = '"' + REPLACE(@propertyValue, '"', '\"') + '"'

    SET @modifiedJson = @cleanJson + '"' + @propertyName + '":' + @formattedValue + '}'

    RETURN @modifiedJson
END
GO
/* =============================================
--  Titulo: fnCA_UpdateJsonProperty
--  Author: JOSE ANTONIO CORNELIO CALDERON
--  Creación: 23/07/2025
--  Descripción: Actualiza el valor de una propiedad existente en un JSON plano.
--  Referencia: Funciona para tipos de valor: string, int, float, bool, null.
--  Ejemplo de uso:
--      SELECT dbo.fnCA_UpdateJsonProperty('{"name":"John","age":30}', 'age', '31', 'int')
==============================================*/
CREATE FUNCTION [dbo].[fnCA_UpdateJsonProperty]
(
    @jsonString     VARCHAR(MAX),
    @propertyName   VARCHAR(100),
    @newValue       VARCHAR(MAX),
    @valueType      VARCHAR(20)  -- 'string', 'int', 'float', 'bool', 'null'
)
RETURNS VARCHAR(MAX)
AS
BEGIN
    DECLARE 
        @updatedJson     VARCHAR(MAX),
        @keyPattern      VARCHAR(150),
        @startIndex      INT,
        @colonIndex      INT,
        @endIndex        INT,
        @oldValueLength  INT,
        @formattedValue  VARCHAR(MAX)

    SET @keyPattern = '"' + @propertyName + '":'

    SET @startIndex = CHARINDEX(@keyPattern, @jsonString)

    IF @startIndex = 0
        RETURN @jsonString

    SET @colonIndex = @startIndex + LEN(@keyPattern)

    SET @endIndex = CHARINDEX(',', @jsonString, @colonIndex)
    IF @endIndex = 0
        SET @endIndex = CHARINDEX('}', @jsonString, @colonIndex)

    SET @oldValueLength = @endIndex - @colonIndex

    IF @valueType = 'string'
        SET @formattedValue = '"' + REPLACE(@newValue, '"', '\"') + '"'
    ELSE IF @valueType = 'int' OR @valueType = 'float'
        SET @formattedValue = @newValue
    ELSE IF @valueType = 'bool'
        SET @formattedValue = CASE WHEN @newValue IN ('1', 'true') THEN 'true' ELSE 'false' END
    ELSE IF @valueType = 'null'
        SET @formattedValue = 'null'
    ELSE
        SET @formattedValue = '"' + REPLACE(@newValue, '"', '\"') + '"'

    SET @updatedJson = STUFF(@jsonString, @colonIndex, @oldValueLength, @formattedValue)

    RETURN @updatedJson
END
GO
/* =============================================
--  Titulo: fnCA_JsonHasKey
--  Author: JOSE ANTONIO CORNELIO CALDERON
--  Creación: 23/07/2025
--  Descripción: Verifica si una clave existe dentro de un JSON simple.
--  Referencia: Utiliza CHARINDEX sobre cadena JSON.
--  Ejemplo de uso:
--      SELECT dbo.fnCA_JsonHasKey('{"name":"John"}', 'name') -- retorna 1
==============================================*/
CREATE FUNCTION [dbo].[fnCA_JsonHasKey]
(
    @json NVARCHAR(MAX),
    @key NVARCHAR(100)
)
RETURNS BIT
AS
BEGIN
    RETURN (
        SELECT CASE 
            WHEN CHARINDEX('"' + @key + '":', @json) > 0 THEN 1 
            ELSE 0 
        END
    )
END
GO
/* =============================================
--  Titulo: fnCA_JsonRemoveKey
--  Author: JOSE ANTONIO CORNELIO CALDERON
--  Creación: 23/07/2025
--  Descripción: Elimina una clave y su valor asociado de un JSON simple.
--  Referencia: Usa CHARINDEX y STUFF para remover fragmento del JSON.
--  Ejemplo de uso:
--      SELECT dbo.fnCA_JsonRemoveKey('{"name":"John","age":30}', 'age')
==============================================*/
CREATE FUNCTION [dbo].[fnCA_JsonRemoveKey]
(
    @json NVARCHAR(MAX),
    @key NVARCHAR(100)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @start INT, @end INT, @cleaned NVARCHAR(MAX)

    SET @start = CHARINDEX('"' + @key + '":', @json)

    IF @start = 0 RETURN @json

    SET @end = CHARINDEX(',', @json + ',', @start)

    IF @end = 0
        SET @cleaned = STUFF(@json, @start - 1, LEN(@json) - @start + 2, '')
    ELSE
        SET @cleaned = STUFF(@json, @start, @end - @start + 1, '')
    IF RIGHT(@cleaned, 1) = ','
    BEGIN
        SET @cleaned = LEFT(@cleaned, LEN(@cleaned) - 1) + '}'
    END
    RETURN @cleaned
END
GO
/* =============================================
--  Titulo: fnCA_JsonNormalizeBooleans
--  Author: JOSE ANTONIO CORNELIO CALDERON
--  Creación: 23/07/2025
--  Descripción: Normaliza booleanos en un JSON cambiando true/false por 1/0.
--  Referencia: También limpia espacios entre el ':' y el valor.
--  Ejemplo de uso:
--      SELECT dbo.fnCA_JsonNormalizeBooleans('{"activo":true,"verificado":false}')
--      -- Resultado: {"activo":1,"verificado":0}
==============================================*/
CREATE FUNCTION [dbo].[fnCA_JsonNormalizeBooleans]
(
    @json NVARCHAR(MAX)
)
RETURNS NVARCHAR(MAX)
AS
BEGIN
    DECLARE @result NVARCHAR(MAX) = @json
    DECLARE @pos INT
    DECLARE @len INT = LEN(@result)
    SET @pos = CHARINDEX(':', @result, 1)
    WHILE @pos > 0 AND @pos < @len
    BEGIN
        WHILE SUBSTRING(@result, @pos + 1, 1) = ' '
        BEGIN
            SET @result = STUFF(@result, @pos + 1, 1, '')
            SET @len = LEN(@result)
        END
        SET @pos = CHARINDEX(':', @result, @pos + 1)
    END
    SET @result = REPLACE(@result, ':true', ':1')
    SET @result = REPLACE(@result, ':false', ':0')
    RETURN @result
END
