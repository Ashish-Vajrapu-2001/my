-- Schema Creation
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'control')
BEGIN
    EXEC('CREATE SCHEMA [control]')
END
GO

-- 1. Source Systems
CREATE TABLE control.source_systems (
    source_system_id INT IDENTITY(1,1) PRIMARY KEY,
    system_name NVARCHAR(100) NOT NULL,
    system_code NVARCHAR(50) NOT NULL UNIQUE,
    description NVARCHAR(255),
    is_active BIT DEFAULT 1
);

-- 2. Table Metadata
CREATE TABLE control.table_metadata (
    table_id INT IDENTITY(1,1) PRIMARY KEY,
    source_system_id INT FOREIGN KEY REFERENCES control.source_systems(source_system_id),
    source_schema NVARCHAR(50) NOT NULL,
    table_name NVARCHAR(100) NOT NULL,
    primary_key_columns NVARCHAR(255) NOT NULL, -- Comma separated if composite
    initial_load_completed BIT DEFAULT 0,
    last_sync_version BIGINT DEFAULT 0,
    last_sync_date DATETIME2,
    is_active BIT DEFAULT 1,
    CONSTRAINT UQ_Table UNIQUE (source_schema, table_name)
);

-- 3. Load Dependencies (Topological Sort Support)
CREATE TABLE control.load_dependencies (
    dependency_id INT IDENTITY(1,1) PRIMARY KEY,
    parent_table_id INT FOREIGN KEY REFERENCES control.table_metadata(table_id),
    child_table_id INT FOREIGN KEY REFERENCES control.table_metadata(table_id),
    CONSTRAINT UQ_Dependency UNIQUE (parent_table_id, child_table_id)
);

-- 4. Execution Log
CREATE TABLE control.pipeline_execution_log (
    log_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    execution_id NVARCHAR(50) NOT NULL, -- ADF Run ID
    table_id INT,
    pipeline_name NVARCHAR(100),
    status NVARCHAR(20), -- 'InProgress', 'Success', 'Failed'
    rows_processed INT,
    start_time DATETIME2 DEFAULT GETDATE(),
    end_time DATETIME2,
    error_message NVARCHAR(MAX)
);

-- 5. Data Quality Rules (Placeholder for metadata linkage)
CREATE TABLE control.data_quality_rules (
    rule_id INT IDENTITY(1,1) PRIMARY KEY,
    table_id INT FOREIGN KEY REFERENCES control.table_metadata(table_id),
    column_name NVARCHAR(100),
    rule_type NVARCHAR(50),
    rule_definition NVARCHAR(MAX),
    severity NVARCHAR(20) DEFAULT 'Warning'
);
