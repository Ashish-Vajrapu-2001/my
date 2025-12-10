CREATE PROCEDURE control.sp_GetCDCChanges
    @SchemaName NVARCHAR(50),
    @TableName NVARCHAR(100),
    @LastSyncVersion BIGINT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @CurrentVersion BIGINT = CHANGE_TRACKING_CURRENT_VERSION();
    DECLARE @MinVersion BIGINT;
    DECLARE @FullName NVARCHAR(200) = QUOTENAME(@SchemaName) + '.' + QUOTENAME(@TableName);
    DECLARE @PKCols NVARCHAR(MAX);

    -- Get Min Valid Version
    SELECT @MinVersion = CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(@FullName));

    -- Handle log compaction / lost history
    IF @LastSyncVersion < @MinVersion
    BEGIN
        -- In production, trigger a full reload here or throw specific error
        THROW 51000, 'Last sync version is older than retention period. Full load required.', 1;
    END

    -- Get PK columns for join condition
    SELECT @PKCols = primary_key_columns
    FROM control.table_metadata
    WHERE source_schema = @SchemaName AND table_name = @TableName;

    -- Build Join Condition (Handle Composite Keys)
    DECLARE @JoinCondition NVARCHAR(MAX) = '';

    SELECT @JoinCondition = STRING_AGG('CT.' + value + ' = T.' + value, ' AND ')
    FROM STRING_SPLIT(@PKCols, ',');

    -- Construct Dynamic SQL
    -- CRITICAL: Avoid duplicate columns by selecting specific metadata columns + T.*
    SET @SQL = '
    SELECT
        CT.SYS_CHANGE_OPERATION,
        CT.SYS_CHANGE_VERSION,
        ' + CAST(@CurrentVersion AS NVARCHAR(20)) + ' as _current_sync_version,
        T.*
    FROM CHANGETABLE(CHANGES ' + @FullName + ', ' + CAST(@LastSyncVersion AS NVARCHAR(20)) + ') AS CT
    LEFT JOIN ' + @FullName + ' AS T ON ' + @JoinCondition;

    EXEC sp_executesql @SQL;
END
