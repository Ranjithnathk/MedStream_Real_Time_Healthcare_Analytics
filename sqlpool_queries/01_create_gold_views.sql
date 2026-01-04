CREATE OR ALTER VIEW dbo.vw_encounters_by_org_month
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://healthcarestoragerk.dfs.core.windows.net/gold/encounters_by_org_month',
    FORMAT = 'DELTA'
) AS rows;

SELECT name FROM sys.views WHERE name = 'vw_encounters_by_org_month';

SELECT TOP 10 * FROM dbo.vw_encounters_by_org_month;


CREATE OR ALTER VIEW dbo.vw_encounters_by_department
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://healthcarestoragerk.dfs.core.windows.net/gold/encounters_by_department',
    FORMAT = 'DELTA'
) AS rows;


CREATE OR ALTER VIEW dbo.vw_payer_coverage_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://healthcarestoragerk.dfs.core.windows.net/gold/payer_coverage_summary',
    FORMAT = 'DELTA'
) AS rows;

SELECT TOP 10 * FROM dbo.vw_encounters_by_org_month;
SELECT TOP 10 * FROM dbo.vw_encounters_by_department;
SELECT TOP 10 * FROM dbo.vw_payer_coverage_summary;