"""
Projet Analyse des Données de Construction
Thématique : Optimisation des coûts et de la durée des chantiers
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
from pathlib import Path

# Configuration des graphiques
try:
    plt.style.use('seaborn-v0_8-whitegrid')
except OSError:
    plt.style.use('seaborn-whitegrid')
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.size'] = 10
FIG_SIZE = (10, 6)
COLOR_PALETTE = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D', '#3B1F2B']

# Chemins
BASE_DIR = Path(__file__).parent
DATA_PATH = BASE_DIR / "Construction_Projects.csv"
OUTPUT_DIR = BASE_DIR / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

# ==================== 1. COLLECTE ET CHARGEMENT DES DONNÉES ====================

def load_data():
    """Charge les données des projets de construction."""
    df = pd.read_csv(DATA_PATH, encoding='utf-8')
    df['Start_Date'] = pd.to_datetime(df['Start_Date'])
    df['End_Date'] = pd.to_datetime(df['End_Date'])
    return df

# ==================== 2. NETTOYAGE ET TRANSFORMATION ====================

def clean_and_transform(df):
    """Nettoie et transforme les données."""
    df = df.copy()
    
    # Vérification des doublons
    df = df.drop_duplicates(subset=['Project_ID'])
    
    # Vérification des valeurs manquantes
    df = df.dropna()
    
    # Feature engineering
    df['Year'] = df['Start_Date'].dt.year
    df['Month'] = df['Start_Date'].dt.month
    df['Cost_Per_Day'] = df['Total_Cost'] / df['Duration']
    df['Materials_Ratio'] = df['Materials_Cost'] / df['Total_Cost']
    df['Labor_Ratio'] = df['Labor_Cost'] / df['Total_Cost']
    df['Budget_Deviation'] = df['Total_Cost'] - df['Budget']
    
    return df

# ==================== 3. ANALYSE EXPLORATOIRE ====================

def create_eda_visualizations(df):
    """Crée les graphiques d'analyse exploratoire."""
    
    # 1. Distribution des coûts totaux
    fig, ax = plt.subplots(figsize=FIG_SIZE)
    ax.hist(df['Total_Cost'], bins=50, color=COLOR_PALETTE[0], alpha=0.8, edgecolor='white')
    ax.set_xlabel('Cout Total (EUR)')
    ax.set_ylabel('Nombre de projets')
    ax.set_title('Distribution des coûts des projets')
    ax.axvline(df['Total_Cost'].median(), color=COLOR_PALETTE[2], linestyle='--', 
               label=f'Mediane: {df["Total_Cost"].median():,.0f} EUR')
    ax.legend()
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '01_distribution_couts.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 2. Coûts vs Durée
    fig, ax = plt.subplots(figsize=FIG_SIZE)
    ax.scatter(df['Duration'], df['Total_Cost'], alpha=0.5, c=df['Year'], 
               cmap='viridis', s=30)
    ax.set_xlabel('Durée (jours)')
    ax.set_ylabel('Coût Total (€)')
    ax.set_title('Relation Durée - Coût des projets')
    cbar = plt.colorbar(ax.collections[0], ax=ax)
    cbar.set_label('Année')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '02_cout_vs_duree.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 3. Matériaux vs Main d'œuvre
    fig, ax = plt.subplots(figsize=FIG_SIZE)
    ax.scatter(df['Materials_Cost'], df['Labor_Cost'], alpha=0.5, c=COLOR_PALETTE[0], s=30)
    ax.set_xlabel('Coût Matériaux (€)')
    ax.set_ylabel('Coût Main d\'œuvre (€)')
    ax.set_title('Répartition Matériaux / Main d\'œuvre')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '03_materiaux_vs_labor.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 4. Évolution des coûts moyens par année
    yearly_stats = df.groupby('Year').agg({
        'Total_Cost': ['mean', 'median'],
        'Duration': 'mean',
        'Project_ID': 'count'
    }).round(0)
    
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))
    
    ax1 = axes[0]
    ax1.bar(yearly_stats.index, yearly_stats[('Total_Cost', 'mean')], 
            color=COLOR_PALETTE[0], alpha=0.8, label='Coût moyen')
    ax1.set_xlabel('Année')
    ax1.set_ylabel('Coût moyen (€)')
    ax1.set_title('Évolution du coût moyen par année')
    ax1.tick_params(axis='x', rotation=45)
    
    ax2 = axes[1]
    ax2.bar(yearly_stats.index, yearly_stats[('Duration', 'mean')], 
            color=COLOR_PALETTE[1], alpha=0.8)
    ax2.set_xlabel('Année')
    ax2.set_ylabel('Durée moyenne (jours)')
    ax2.set_title('Évolution de la durée moyenne par année')
    ax2.tick_params(axis='x', rotation=45)
    
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '04_evolution_annuelle.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 5. Matrice de corrélation
    numeric_cols = ['Total_Cost', 'Duration', 'Materials_Cost', 'Labor_Cost', 
                    'Materials_Ratio', 'Labor_Ratio', 'Cost_Per_Day']
    corr_matrix = df[numeric_cols].corr()
    
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(corr_matrix, annot=True, fmt='.2f', cmap='coolwarm', center=0,
                ax=ax, square=True, linewidths=0.5)
    ax.set_title('Matrice de corrélation des variables')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '05_correlation_matrix.png', dpi=150, bbox_inches='tight')
    plt.close()

# ==================== 4. MODÈLE DE RÉGRESSION ====================

def build_regression_model(df):
    """Construit et évalue le modèle de régression pour prédire les coûts."""
    
    # Pour la prédiction, on utilise les variables connues en amont du projet
    # Duration (planifiée), Year, Month (évite le data leakage)
    X = df[['Duration', 'Year', 'Month']].copy()
    y = df['Total_Cost']
    
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    
    # Modèle 1: Régression Linéaire
    lr_model = LinearRegression()
    lr_model.fit(X_train, y_train)
    y_pred_lr = lr_model.predict(X_test)
    
    # Modèle 2: Random Forest (avec toutes les features pour comparaison)
    X_full = df[['Duration', 'Materials_Cost', 'Labor_Cost', 'Year', 'Month']]
    X_train_f, X_test_f, y_train_f, y_test_f = train_test_split(
        X_full, y, test_size=0.2, random_state=42
    )
    rf_model = RandomForestRegressor(n_estimators=100, random_state=42, max_depth=10)
    rf_model.fit(X_train_f, y_train_f)
    y_pred_rf = rf_model.predict(X_test_f)
    
    # Métriques
    metrics = {
        'Linear Regression': {
            'R2': r2_score(y_test, y_pred_lr),
            'RMSE': np.sqrt(mean_squared_error(y_test, y_pred_lr)),
            'MAE': mean_absolute_error(y_test, y_pred_lr)
        },
        'Random Forest': {
            'R2': r2_score(y_test_f, y_pred_rf),
            'RMSE': np.sqrt(mean_squared_error(y_test_f, y_pred_rf)),
            'MAE': mean_absolute_error(y_test_f, y_pred_rf)
        }
    }
    
    return lr_model, rf_model, metrics, X_train, X_test, y_test, y_pred_lr, y_pred_rf

def create_model_visualizations(df):
    """Crée les graphiques des résultats du modèle."""
    
    # 6. Prédictions vs Réel (Régression Linéaire)
    X = df[['Duration', 'Year', 'Month']]
    y = df['Total_Cost']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    lr_model = LinearRegression().fit(X_train, y_train)
    y_pred_lr = lr_model.predict(X_test)
    
    fig, ax = plt.subplots(figsize=FIG_SIZE)
    ax.scatter(y_test, y_pred_lr, alpha=0.6, c=COLOR_PALETTE[0])
    max_val = max(y_test.max(), y_pred_lr.max())
    ax.plot([0, max_val], [0, max_val], 'r--', lw=2, label='Prédiction parfaite')
    ax.set_xlabel('Coût Réel (€)')
    ax.set_ylabel('Coût Prédit (€)')
    ax.set_title('Modèle de Régression : Prédictions vs Réalité')
    ax.legend()
    ax.set_xlim(0, max_val * 1.05)
    ax.set_ylim(0, max_val * 1.05)
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '06_predictions_modele.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 7. Importance des variables (Random Forest)
    rf_model = RandomForestRegressor(n_estimators=100, random_state=42, max_depth=10)
    X_full = df[['Duration', 'Materials_Cost', 'Labor_Cost', 'Year', 'Month']]
    rf_model.fit(X_full, df['Total_Cost'])
    
    importance_df = pd.DataFrame({
        'Variable': X_full.columns,
        'Importance': rf_model.feature_importances_
    }).sort_values('Importance', ascending=True)
    
    fig, ax = plt.subplots(figsize=FIG_SIZE)
    ax.barh(importance_df['Variable'], importance_df['Importance'], color=COLOR_PALETTE)
    ax.set_xlabel('Importance')
    ax.set_title('Importance des variables (Random Forest)')
    plt.tight_layout()
    plt.savefig(OUTPUT_DIR / '07_importance_variables.png', dpi=150, bbox_inches='tight')
    plt.close()

# ==================== 5. ANALYSE DES ÉCONOMIES POTENTIELLES ====================

def analyze_potential_savings(df):
    """Analyse les économies potentielles."""
    
    # Coût moyen par jour par projet
    df['Cost_Per_Day'] = df['Total_Cost'] / df['Duration']
    
    # Projets "efficaces" = coût/jour < médiane
    median_cpd = df['Cost_Per_Day'].median()
    efficient = df[df['Cost_Per_Day'] <= median_cpd].copy()
    inefficient = df[df['Cost_Per_Day'] > median_cpd].copy()
    
    # Économie potentielle si les projets inefficaces atteignaient la médiane
    inefficient = inefficient.assign(
        Potential_Savings=(inefficient['Cost_Per_Day'] - median_cpd) * inefficient['Duration']
    )
    total_potential_savings = inefficient['Potential_Savings'].sum()
    
    return {
        'median_cost_per_day': median_cpd,
        'total_potential_savings': total_potential_savings,
        'efficient_projects': len(efficient),
        'inefficient_projects': len(inefficient),
        'avg_savings_per_inefficient': inefficient['Potential_Savings'].mean()
    }

# ==================== 6. EXPORT POUR POWER BI ====================

def export_for_power_bi(df):
    """Exporte les données transformées pour Power BI."""
    output_path = OUTPUT_DIR / "Construction_Projects_Analytics.xlsx"
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Donnees_Principales', index=False)
        
        # Feuille résumé par année
        yearly = df.groupby('Year').agg({
            'Total_Cost': ['sum', 'mean'],
            'Duration': 'mean',
            'Project_ID': 'count'
        }).round(0)
        yearly.columns = ['Cout_Total', 'Cout_Moyen', 'Duree_Moyenne', 'Nb_Projets']
        yearly.to_excel(writer, sheet_name='Resume_Annuel')
    
    return output_path

# ==================== MAIN ====================

def main():
    print("=" * 60)
    print("ANALYSE DES PROJETS DE CONSTRUCTION")
    print("Optimisation des coûts et de la durée des chantiers")
    print("=" * 60)
    
    # 1. Chargement
    print("\n[1/6] Chargement des données...")
    df = load_data()
    print(f"      -> {len(df)} projets charges")
    
    # 2. Nettoyage
    print("\n[2/6] Nettoyage et transformation...")
    df = clean_and_transform(df)
    print(f"      -> Donnees nettoyees, {len(df.columns)} colonnes")
    
    # 3. Visualisations EDA
    print("\n[3/6] Création des graphiques d'analyse...")
    create_eda_visualizations(df)
    print(f"      -> 5 graphiques sauvegardes dans {OUTPUT_DIR}")
    
    # 4. Modèle de régression
    print("\n[4/6] Construction du modèle prédictif...")
    lr_model, rf_model, metrics, _, _, _, y_pred_lr, y_pred_rf = build_regression_model(df)
    
    print("\n      Metriques du modele :")
    for model_name, m in metrics.items():
        print(f"      {model_name}:")
        print(f"        - R2 = {m['R2']:.4f}")
        print(f"        - RMSE = {m['RMSE']:,.0f} EUR")
        print(f"        - MAE = {m['MAE']:,.0f} EUR")
    
    create_model_visualizations(df)
    print(f"      -> 2 graphiques du modele sauvegardes")
    
    # 5. Économies potentielles
    print("\n[5/6] Analyse des économies potentielles...")
    savings = analyze_potential_savings(df)
    print(f"      -> Economie potentielle totale: {savings['total_potential_savings']:,.0f} EUR")
    print(f"      -> Projets a optimiser: {savings['inefficient_projects']}")
    
    # 6. Export
    print("\n[6/6] Export des données...")
    export_path = export_for_power_bi(df)
    print(f"      -> Fichier Excel: {export_path}")
    
    # Sauvegarde des métriques pour le rapport
    with open(OUTPUT_DIR / "metrics_summary.txt", "w", encoding="utf-8") as f:
        f.write(f"R2 Linear Regression: {metrics['Linear Regression']['R2']:.4f}\n")
        f.write(f"R2 Random Forest: {metrics['Random Forest']['R2']:.4f}\n")
        f.write(f"Économie potentielle: {savings['total_potential_savings']:,.0f} €\n")
    
    print("\n" + "=" * 60)
    print("ANALYSE TERMINEE")
    print("=" * 60)

if __name__ == "__main__":
    main()
