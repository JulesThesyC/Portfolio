-- ============================================================
-- Projet Analyse Construction - Scripts SQL
-- Préparation des données pour l'analyse et Power BI
-- ============================================================

-- 1. Création de la table principale
CREATE TABLE IF NOT EXISTS Construction_Projects (
    Project_ID INT PRIMARY KEY,
    Project_Name VARCHAR(100),
    Start_Date DATE,
    End_Date DATE,
    Total_Cost DECIMAL(12, 2),
    Budget DECIMAL(12, 2),
    Duration INT,
    Materials_Cost DECIMAL(12, 2),
    Labor_Cost DECIMAL(12, 2)
);
