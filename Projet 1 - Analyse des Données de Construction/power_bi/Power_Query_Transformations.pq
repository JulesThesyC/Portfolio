// ============================================================
// Power BI - Power Query M - Transformations des données
// Copier dans "Éditeur avancé" après import du fichier CSV
// ============================================================

let
    Source = Csv.Document(File.Contents("C:\chemin\vers\Construction_Projects.csv"), [Delimiter=",", Columns=9, Encoding=65001]),
    PromotedHeaders = Table.PromoteHeaders(Source, [PromoteAllScalars=true]),
    ChangedTypes = Table.TransformColumnTypes(PromotedHeaders, {
        {"Project_ID", Int64.Type},
        {"Project_Name", type text},
        {"Start_Date", type date},
        {"End_Date", type date},
        {"Total_Cost", Int64.Type},
        {"Budget", Int64.Type},
        {"Duration", Int64.Type},
        {"Materials_Cost", Int64.Type},
        {"Labor_Cost", Int64.Type}
    }),
    AddYear = Table.AddColumn(ChangedTypes, "Year", each Date.Year([Start_Date]), Int64.Type),
    AddMonth = Table.AddColumn(AddYear, "Month", each Date.Month([Start_Date]), Int64.Type),
    AddCostPerDay = Table.AddColumn(AddMonth, "Cost_Per_Day", each [Total_Cost] / [Duration], type number),
    AddMaterialsRatio = Table.AddColumn(AddCostPerDay, "Materials_Ratio", each [Materials_Cost] / [Total_Cost], type number)
in
    AddMaterialsRatio
