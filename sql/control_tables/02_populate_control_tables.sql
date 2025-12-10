-- 1. Populate Source Systems
INSERT INTO control.source_systems (system_name, system_code, description) VALUES
('Azure SQL ERP', 'SRC-001', 'Orders, Inventory, Finance'),
('Azure SQL CRM', 'SRC-002', 'Customer Master, Support'),
('Azure SQL Marketing', 'SRC-003', 'Campaign Management');

DECLARE @ERP_ID INT = (SELECT source_system_id FROM control.source_systems WHERE system_code = 'SRC-001');
DECLARE @CRM_ID INT = (SELECT source_system_id FROM control.source_systems WHERE system_code = 'SRC-002');
DECLARE @MKT_ID INT = (SELECT source_system_id FROM control.source_systems WHERE system_code = 'SRC-003');

-- 2. Populate Table Metadata
-- CRM Tables
INSERT INTO control.table_metadata (source_system_id, source_schema, table_name, primary_key_columns) VALUES
(@CRM_ID, 'CRM', 'Customers', 'CUSTOMER_ID'),
(@CRM_ID, 'CRM', 'CustomerRegistrationSource', 'REGISTRATION_SOURCE_ID'),
(@CRM_ID, 'CRM', 'INCIDENTS', 'INCIDENT_ID'),
(@CRM_ID, 'CRM', 'INTERACTIONS', 'INTERACTION_ID'),
(@CRM_ID, 'CRM', 'SURVEYS', 'SURVEY_ID');

-- ERP Tables
INSERT INTO control.table_metadata (source_system_id, source_schema, table_name, primary_key_columns) VALUES
(@ERP_ID, 'ERP', 'OE_ORDER_HEADERS_ALL', 'ORDER_ID'),
(@ERP_ID, 'ERP', 'OE_ORDER_LINES_ALL', 'LINE_ID'),
(@ERP_ID, 'ERP', 'ADDRESSES', 'ADDRESS_ID'),
(@ERP_ID, 'ERP', 'CITY_TIER_MASTER', 'CITY,STATE'), -- Composite PK
(@ERP_ID, 'ERP', 'MTL_SYSTEM_ITEMS_B', 'INVENTORY_ITEM_ID'),
(@ERP_ID, 'ERP', 'CATEGORIES', 'CATEGORY_ID'),
(@ERP_ID, 'ERP', 'BRANDS', 'BRAND_ID');

-- Marketing Tables
INSERT INTO control.table_metadata (source_system_id, source_schema, table_name, primary_key_columns) VALUES
(@MKT_ID, 'MARKETING', 'MARKETING_CAMPAIGNS', 'CAMPAIGN_ID');

-- 3. Populate Dependencies (Parent -> Child)
-- Helpers to get IDs
DECLARE @CustID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'Customers');
DECLARE @OrdHeadID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'OE_ORDER_HEADERS_ALL');
DECLARE @OrdLineID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'OE_ORDER_LINES_ALL');
DECLARE @AddrID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'ADDRESSES');
DECLARE @CityID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CITY_TIER_MASTER');
DECLARE @ItemID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'MTL_SYSTEM_ITEMS_B');
DECLARE @CatID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CATEGORIES');
DECLARE @BrandID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'BRANDS');
DECLARE @CampID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'MARKETING_CAMPAIGNS');
DECLARE @RegID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'CustomerRegistrationSource');
DECLARE @IncID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'INCIDENTS');
DECLARE @SurvID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'SURVEYS');
DECLARE @IntID INT = (SELECT table_id FROM control.table_metadata WHERE table_name = 'INTERACTIONS');

INSERT INTO control.load_dependencies (parent_table_id, child_table_id) VALUES
-- Customer is central
(@CustID, @OrdHeadID),
(@CustID, @RegID),
(@CustID, @AddrID),
(@CustID, @IncID),
(@CustID, @SurvID),
-- Order Dependencies
(@OrdHeadID, @OrdLineID),
-- Product Dependencies
(@CatID, @ItemID),
(@BrandID, @ItemID),
(@ItemID, @OrdLineID),
-- Geo Dependencies
(@CityID, @AddrID),
-- Marketing
(@CampID, @RegID),
-- Support
(@IncID, @IntID);
