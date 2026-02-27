-- 2. Vue enrichie avec calculs pour l'analyse

CREATE OR REPLACE VIEW v_Construction_Projects_Analytics AS
SELECT 
    Project_ID,
    Project_Name,
    Start_Date,
    End_Date,
    Total_Cost,
    Budget,
    Duration,
    Materials_Cost,
    Labor_Cost,
    -- Features calculées
    EXTRACT(YEAR FROM Start_Date) AS Year,
    EXTRACT(MONTH FROM Start_Date) AS Month,
    ROUND(Total_Cost / NULLIF(Duration, 0), 2) AS Cost_Per_Day,
    ROUND(Materials_Cost / NULLIF(Total_Cost, 0), 4) AS Materials_Ratio,
    ROUND(Labor_Cost / NULLIF(Total_Cost, 0), 4) AS Labor_Ratio,
    Total_Cost - Budget AS Budget_Deviation
FROM Construction_Projects
WHERE Total_Cost IS NOT NULL
  AND Duration > 0;
