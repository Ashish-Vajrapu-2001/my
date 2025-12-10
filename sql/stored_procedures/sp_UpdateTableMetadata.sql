CREATE PROCEDURE control.sp_UpdateTableMetadata
    @SchemaName NVARCHAR(50),
    @TableName NVARCHAR(100),
    @Status NVARCHAR(20), -- 'Success' or 'Failed'
    @RowsProcessed INT = 0,
    @PipelineRunId NVARCHAR(50),
    @PipelineName NVARCHAR(100),
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @TableID INT;
    DECLARE @CurrentVersion BIGINT = CHANGE_TRACKING_CURRENT_VERSION();

    SELECT @TableID = table_id
    FROM control.table_metadata
    WHERE source_schema = @SchemaName AND table_name = @TableName;

    -- Log Execution
    INSERT INTO control.pipeline_execution_log
    (execution_id, table_id, pipeline_name, status, rows_processed, end_time, error_message)
    VALUES
    (@PipelineRunId, @TableID, @PipelineName, @Status, @RowsProcessed, GETDATE(), @ErrorMessage);

    -- Update Metadata on Success
    IF @Status = 'Success'
    BEGIN
        UPDATE control.table_metadata
        SET
            last_sync_date = GETDATE(),
            -- For initial load completion
            initial_load_completed = 1,
            -- Update version only if this was an incremental run or initial load sets baseline
            last_sync_version = CASE
                WHEN @PipelineName LIKE '%Incremental%' THEN @CurrentVersion
                WHEN @PipelineName LIKE '%Initial%' THEN @CurrentVersion
                ELSE last_sync_version
            END
        WHERE table_id = @TableID;
    END
END
