/* =============================================
--  Titulo: xpCA_InsertFromJsonDynamic
--  Author: JOSE ANTONIO CORNELIO CALDERON
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
    @table              VARCHAR(100),
    @jsonFields         VARCHAR(MAX),
    @tableFields        VARCHAR(MAX),
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

        IF dbo.fnCA_ValidateTableFieldsExist(@table, @tableFields) = 0
        BEGIN
            RAISERROR('Uno o más campos especificados en @tableFields no existen en la tabla %s.', 16, 1, @table);
            RETURN;
        END

        SELECT @sql = 'INSERT INTO ' + @table + ' (' + @tableFields + ') VALUES (' +
            STUFF((
                SELECT ',' + @jsonLabelFunction + '(''' + @json + ''',''' + name + ''')'
                FROM dbo.fnCA_StringSplit(@jsonFields, ',')
                FOR XML PATH(''), TYPE).value('.', 'VARCHAR(MAX)'),
                1, 1, '') + ')';

        EXEC(@sql);
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
