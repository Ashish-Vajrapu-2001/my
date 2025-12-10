CREATE PROCEDURE control.sp_GetTableLoadOrder
AS
BEGIN
    SET NOCOUNT ON;

    -- CTE to determine load order based on dependencies
    -- This ignores cycles, assumes DAG
    WITH TableWeights AS (
        SELECT
            t.table_id,
            t.source_schema,
            t.table_name,
            t.initial_load_completed,
            0 as level
        FROM control.table_metadata t
        WHERE NOT EXISTS (SELECT 1 FROM control.load_dependencies d WHERE d.child_table_id = t.table_id)
        AND t.is_active = 1

        UNION ALL

        SELECT
            t.table_id,
            t.source_schema,
            t.table_name,
            t.initial_load_completed,
            tw.level + 1
        FROM control.table_metadata t
        JOIN control.load_dependencies d ON t.table_id = d.child_table_id
        JOIN TableWeights tw ON d.parent_table_id = tw.table_id
        WHERE t.is_active = 1
    )
    SELECT DISTINCT
        source_schema,
        table_name,
        initial_load_completed,
        MAX(level) as load_priority
    FROM TableWeights
    GROUP BY source_schema, table_name, initial_load_completed
    ORDER BY MAX(level) ASC;
END
