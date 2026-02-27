-- 3. Requêtes analytiques pour rapports

-- Résumé annuel
SELECT 
    EXTRACT(YEAR FROM Start_Date) AS Year,
    COUNT(*) AS Nb_Projets,
    ROUND(SUM(Total_Cost), 0) AS Cout_Total,
    ROUND(AVG(Total_Cost), 0) AS Cout_Moyen,
    ROUND(AVG(Duration), 0) AS Duree_Moyenne_Jours
FROM Construction_Projects
GROUP BY EXTRACT(YEAR FROM Start_Date)
ORDER BY Year;

-- Top 10 projets les plus coûteux
SELECT Project_Name, Total_Cost, Duration, Materials_Cost, Labor_Cost
FROM Construction_Projects
ORDER BY Total_Cost DESC
LIMIT 10;

-- Projets par tranche de coût
SELECT 
    CASE 
        WHEN Total_Cost < 150000 THEN '< 150k€'
        WHEN Total_Cost < 300000 THEN '150k-300k€'
        ELSE '> 300k€'
    END AS Tranche_Cout,
    COUNT(*) AS Nb_Projets,
    ROUND(AVG(Total_Cost), 0) AS Cout_Moyen
FROM Construction_Projects
GROUP BY 1
ORDER BY MIN(Total_Cost);
