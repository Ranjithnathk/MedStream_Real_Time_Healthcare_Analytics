CREATE OR ALTER VIEW dbo.vw_pbi_encounters_by_org_month
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://healthcarestoragerk.dfs.core.windows.net/gold/powerbi/encounters_by_org_month/**',
    FORMAT = 'PARQUET'
) AS rows;

CREATE OR ALTER VIEW dbo.vw_pbi_encounters_by_department
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://healthcarestoragerk.dfs.core.windows.net/gold/powerbi/encounters_by_department/**',
    FORMAT = 'PARQUET'
) AS rows;

CREATE OR ALTER VIEW dbo.vw_pbi_payer_coverage_summary
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://healthcarestoragerk.dfs.core.windows.net/gold/powerbi/payer_coverage_summary/**',
    FORMAT = 'PARQUET'
) AS rows;


SELECT TOP 10 * FROM dbo.vw_pbi_encounters_by_org_month;
SELECT TOP 10 * FROM dbo.vw_pbi_encounters_by_department;
SELECT TOP 10 * FROM dbo.vw_pbi_payer_coverage_summary;


