/* =============================================
--  Titulo: xpCA_InsertFromJsonDynamic
--  Autor:  José Antonio Cornelio Calderón
--  Creación: 23/07/2025
--  Descripción: Método para insertar datos dinámicamente en una tabla a partir de un JSON, 
--               utilizando campos mapeados de forma dinámica.
--  Referencia: Utiliza la función fnCA_StringSplit y una función personalizada para obtener valores JSON.
--  Ejemplo de uso:
--      EXEC dbo.xpCA_InsertFromJsonDynamic 
--          @json = '{"name":"John","age":"30"}',
--          @table = 'Usuarios',
--          @jsonFields = 'name,age',
--          @tableFields = 'Nombre,Edad',
--          @jsonLabelFunction = 'dbo.fnCA_JsonGetValueByPath'
==============================================*/
CREATE PROCEDURE [dbo].[xpCA_InsertFromJsonDynamic]
    @json               VARCHAR(MAX),
    @table              SYSNAME,
    @jsonFields         VARCHAR(MAX),
    @tableFields        VARCHAR(MAX),
    @jsonLabelFunction  SYSNAME -- Ejemplo: 'dbo.fnCA_JsonGetValueByPath'
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        DECLARE @sql NVARCHAR(MAX) = '';
        DECLARE @columns NVARCHAR(MAX) = '';
        DECLARE @values NVARCHAR(MAX) = '';

        ------------------------------------------------------------------
        -- Validar que los campos existan
        ------------------------------------------------------------------
        IF dbo.fnCA_ValidateTableFieldsExist(@table, @tableFields) = 0
        BEGIN
            RAISERROR('Uno o más campos de @tableFields no existen en %s.', 16, 1, @table);
            RETURN;
        END

        ------------------------------------------------------------------
        -- Extraer valores de JSON
        ------------------------------------------------------------------
        DECLARE @FieldTable TABLE (
            JsonField   VARCHAR(200),
            TableField  VARCHAR(200),
            Value       NVARCHAR(MAX),
            DataType    NVARCHAR(100),
            IsNullable  BIT
        );

        DECLARE @jsonField VARCHAR(200), @tableField VARCHAR(200), @val NVARCHAR(MAX);

        DECLARE field_cursor CURSOR FAST_FORWARD FOR
        WITH JsonFieldsCTE AS (
            SELECT
                LTRIM(RTRIM(jf.name)) AS JsonField,
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
            FROM dbo.fnCA_StringSplit(@jsonFields, ',') jf
        ),
        TableFieldsCTE AS (
            SELECT
                LTRIM(RTRIM(tf.name)) AS TableField,
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS RowNum
            FROM dbo.fnCA_StringSplit(@tableFields, ',') tf
        )
        SELECT
            j.JsonField AS Campo_JSON,
            t.TableField AS Campo_Tabla
        FROM JsonFieldsCTE j
        FULL OUTER JOIN TableFieldsCTE t
            ON j.RowNum = t.RowNum
        ORDER BY ISNULL(j.RowNum, t.RowNum);

        OPEN field_cursor;
        FETCH NEXT FROM field_cursor INTO @jsonField, @tableField;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @query NVARCHAR(MAX) =
                N'SELECT @out = ' + @jsonLabelFunction + '(@json, @key)';

            EXEC sp_executesql 
                @query,
                N'@json VARCHAR(MAX), @key VARCHAR(200), @out NVARCHAR(MAX) OUTPUT',
                @json = @json,
                @key = @jsonField,
                @out = @val OUTPUT;

            ------------------------------------------------------------------
            -- Validar tipo y nulabilidad del campo en la tabla destino
            ------------------------------------------------------------------
            DECLARE @dataType NVARCHAR(100), @isNullable BIT;

            SELECT 
                @dataType = DATA_TYPE,
                @isNullable = CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = @table AND COLUMN_NAME = @tableField;

            ------------------------------------------------------------------
            -- Normalizar valor nulo y conversión según tipo
            ------------------------------------------------------------------
            IF @val IS NULL OR LTRIM(RTRIM(@val)) = ''
            BEGIN
                IF @isNullable = 0
                BEGIN
                    -- Asignar valor por defecto según tipo
                    SET @val = CASE 
                        WHEN @dataType LIKE '%int%' THEN '0'
                        WHEN @dataType LIKE '%decimal%' THEN '0'
                        WHEN @dataType LIKE '%numeric%' THEN '0'
                        WHEN @dataType LIKE '%bit%' THEN '0'
                        WHEN @dataType LIKE '%datetime%' THEN '1900-01-01'
                        ELSE ''
                    END
                END
            END
            ELSE
            BEGIN
                -- Normalizar tipos booleanos si vienen en formato JSON
                IF @dataType = 'bit'
                    SET @val = CASE WHEN @val IN ('true', '1') THEN '1' ELSE '0' END;
            END

            INSERT INTO @FieldTable (JsonField, TableField, Value, DataType, IsNullable)
            VALUES (@jsonField, @tableField, @val, @dataType, @isNullable);

            FETCH NEXT FROM field_cursor INTO @jsonField, @tableField;
        END

        CLOSE field_cursor;
        DEALLOCATE field_cursor;

        ------------------------------------------------------------------
        -- Construir el INSERT dinámico (compatible con SQL Server 2014+)
        ------------------------------------------------------------------
        SELECT 
            @columns = STUFF((
                SELECT ',' + QUOTENAME(TableField)
                FROM @FieldTable
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, ''),
            @values = STUFF((
                SELECT ',' + 
                    CASE 
                        WHEN DataType IN ('int','bigint','decimal','numeric','float','bit') 
                            THEN ISNULL(Value, '0')
                        WHEN DataType LIKE '%date%' 
                            THEN 'CAST(''' + ISNULL(Value, '1900-01-01') + ''' AS DATETIME)'
                        ELSE '''' + REPLACE(ISNULL(Value, ''), '''', '''''') + ''''
                    END
                FROM @FieldTable
                FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 1, '');

        SET @sql = N'INSERT INTO ' + QUOTENAME(@table) + '(' + @columns + ') VALUES (' + @values + ');';

        PRINT @sql; -- Para depuración
        EXEC (@sql);

    END TRY
    BEGIN CATCH
        PRINT 'Error en xpCA_InsertFromJsonDynamic';
        PRINT 'Mensaje: ' + ERROR_MESSAGE();
        PRINT 'Número: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Línea: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT 'Procedimiento: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    END CATCH
END
GO
/* =============================================
--  Titulo: xpCA_UpdateFromJsonDynamic
--  Author: JOSE ANTONIO CORNELIO CALDERON
--  Creación: 23/07/2025
--  Descripción: Método para actualizar dinámicamente una tabla a partir de un JSON, 
--               utilizando campos mapeados y una condición WHERE obligatoria.
--  Referencia: Utiliza la función fnCA_StringSplit y una función personalizada para obtener valores JSON.
--  Ejemplo de uso:
--      EXEC dbo.xpCA_UpdateFromJsonDynamic 
--          @json = '{"name":"John","age":"31"}',
--          @table = 'Usuarios',
--          @jsonFields = 'name,age',
--          @tableFields = 'Nombre,Edad',
--          @whereCondition = 'WHERE IdUsuario = 1',
--          @jsonLabelFunction = 'dbo.fnCA_JsonGetValueByPath'
==============================================*/
CREATE PROCEDURE [dbo].[xpCA_UpdateFromJsonDynamic]
    @json               VARCHAR(MAX),
    @table              VARCHAR(100),
    @jsonFields         VARCHAR(MAX),
    @tableFields        VARCHAR(MAX),
    @whereCondition     VARCHAR(MAX),
    @jsonLabelFunction  VARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    -- Configuración segura de sesión
    SET ANSI_NULLS ON;
    SET ANSI_PADDING ON;
    SET ANSI_WARNINGS ON;
    SET ARITHABORT ON;
    SET CONCAT_NULL_YIELDS_NULL ON;
    SET QUOTED_IDENTIFIER ON;
    SET NUMERIC_ROUNDABORT OFF;

    BEGIN TRY
        DECLARE @sql VARCHAR(MAX);

        IF @whereCondition IS NULL OR @whereCondition NOT LIKE '%WHERE%'
        BEGIN
            RAISERROR('Es necesario incluir la cláusula WHERE para actualizar la tabla %s.', 16, 1, @table);
            RETURN;
        END

        IF dbo.fnCA_ValidateTableFieldsExist(@table, @tableFields) = 0
        BEGIN
            RAISERROR('Uno o más campos especificados en @tableFields no existen en la tabla %s.', 16, 1, @table);
            RETURN;
        END

        SELECT @sql = 'UPDATE ' + @table + ' SET ' +
            STUFF((
                SELECT 
                    ', ' + t.name + ' = ' + @jsonLabelFunction + '(''' + @json + ''', ''' + j.name + ''')'
                FROM dbo.fnCA_StringSplit(@tableFields, ',') t
                INNER JOIN dbo.fnCA_StringSplit(@jsonFields, ',') j ON t.name = j.name
                FOR XML PATH(''), TYPE).value('.', 'VARCHAR(MAX)'),
                1, 2, '') + ' ' + @whereCondition;

        EXEC(@sql);
    END TRY
    BEGIN CATCH
        PRINT 'Error en xpCA_UpdateFromJsonDynamic';
        PRINT 'Mensaje: ' + ERROR_MESSAGE();
        PRINT 'Número: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Línea: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT 'Procedimiento: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    END CATCH
END
GO
/* =============================================
--  Titulo: xpCA_UpsertFromJsonDynamic
--  Autor:  José Antonio Cornelio Calderón
--  Fecha:  12/11/2025
--  Descripción: Inserta o actualiza dinámicamente en una tabla según si existe
--               la combinación de llaves primarias definidas, usando valores de JSON.
--  Parámetros:
--      @json               -> Cadena JSON con los datos
--      @table              -> Nombre de la tabla destino
--      @jsonFields         -> Campos del JSON (separados por coma)
--      @tableFields        -> Campos correspondientes de la tabla (mismo orden)
--      @jsonLabelFunction  -> Función para obtener valores JSON (por ej. dbo.fnCA_JsonGetValueByPath)
--      @primaryKeys        -> Campos que forman la llave primaria (separados por coma)
--  Ejemplo:
--      EXEC dbo.xpCA_UpsertFromJsonDynamic
--          @json = '{"IdUsuario":1,"Nombre":"Antonio","Edad":28}',
--          @table = 'Usuarios',
--          @jsonFields = 'IdUsuario,Nombre,Edad',
--          @tableFields = 'IdUsuario,Nombre,Edad',
--          @jsonLabelFunction = 'dbo.fnCA_JsonGetValueByPath',
--          @primaryKeys = 'IdUsuario'
==============================================*/
CREATE PROCEDURE [dbo].[xpCA_UpsertFromJsonDynamic]
    @json               VARCHAR(MAX),
    @table              SYSNAME,
    @jsonFields         VARCHAR(MAX),
    @tableFields        VARCHAR(MAX),
    @jsonLabelFunction  SYSNAME,
    @primaryKeys        VARCHAR(MAX)
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        DECLARE 
            @sql NVARCHAR(MAX) = '',
            @where NVARCHAR(MAX) = '',
            @exists INT = 0,
            @pkField VARCHAR(200),
            @pkValue NVARCHAR(MAX);

        ------------------------------------------------------------------
        -- Validar existencia de campos
        ------------------------------------------------------------------
        IF dbo.fnCA_ValidateTableFieldsExist(@table, @tableFields) = 0
        BEGIN
            RAISERROR('Uno o más campos en @tableFields no existen en la tabla %s.', 16, 1, @table);
            RETURN;
        END

        ------------------------------------------------------------------
        -- Armar condiciones dinámicas de llave primaria
        ------------------------------------------------------------------
        DECLARE @pk TABLE (FieldName VARCHAR(200), Value NVARCHAR(MAX));
        DECLARE pk_cursor CURSOR FOR
            SELECT LTRIM(RTRIM(name)) FROM dbo.fnCA_StringSplit(@primaryKeys, ',');

        OPEN pk_cursor;
        FETCH NEXT FROM pk_cursor INTO @pkField;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            DECLARE @query NVARCHAR(MAX) =
                N'SELECT @out = ' + @jsonLabelFunction + '(@json, @key)';
            
            EXEC sp_executesql
                @query,
                N'@json VARCHAR(MAX), @key VARCHAR(200), @out NVARCHAR(MAX) OUTPUT',
                @json = @json,
                @key = @pkField,
                @out = @pkValue OUTPUT;

            IF @pkValue IS NULL OR LTRIM(RTRIM(@pkValue)) = ''
                SET @pkValue = 'NULL';

            INSERT INTO @pk VALUES(@pkField, @pkValue);
            FETCH NEXT FROM pk_cursor INTO @pkField;
        END

        CLOSE pk_cursor;
        DEALLOCATE pk_cursor;

        SELECT @where = STRING_AGG(
            QUOTENAME(FieldName) + ' = ' + 
            CASE WHEN Value = 'NULL' THEN 'NULL' ELSE '''' + REPLACE(Value, '''', '''''') + '''' END,
            ' AND '
        )
        FROM @pk;

        ------------------------------------------------------------------
        -- Validar si el registro existe
        ------------------------------------------------------------------
        DECLARE @existSql NVARCHAR(MAX) = 
            N'SELECT @exists = COUNT(*) FROM ' + QUOTENAME(@table) + ' WHERE ' + @where;

        EXEC sp_executesql @existSql, N'@exists INT OUTPUT', @exists OUTPUT;

        ------------------------------------------------------------------
        -- Decidir si INSERT o UPDATE
        ------------------------------------------------------------------
        IF @exists = 0
        BEGIN
            -- Inserta si no existe
            EXEC dbo.xpCA_InsertFromJsonDynamic
                @json = @json,
                @table = @table,
                @jsonFields = @jsonFields,
                @tableFields = @tableFields,
                @jsonLabelFunction = @jsonLabelFunction;

            PRINT 'Registro insertado correctamente.';
        END
        ELSE
        BEGIN
            -- Actualiza si ya existe
            DECLARE @whereClause NVARCHAR(MAX) = 'WHERE ' + @where;

            EXEC dbo.xpCA_UpdateFromJsonDynamic
                @json = @json,
                @table = @table,
                @jsonFields = @jsonFields,
                @tableFields = @tableFields,
                @whereCondition = @whereClause,
                @jsonLabelFunction = @jsonLabelFunction;

            PRINT 'Registro actualizado correctamente.';
        END
    END TRY
    BEGIN CATCH
        PRINT 'Error en xpCA_UpsertFromJsonDynamic';
        PRINT 'Mensaje: ' + ERROR_MESSAGE();
        PRINT 'Número: ' + CAST(ERROR_NUMBER() AS VARCHAR);
        PRINT 'Línea: ' + CAST(ERROR_LINE() AS VARCHAR);
        PRINT 'Procedimiento: ' + ISNULL(ERROR_PROCEDURE(), 'N/A');
    END CATCH
END
GO

