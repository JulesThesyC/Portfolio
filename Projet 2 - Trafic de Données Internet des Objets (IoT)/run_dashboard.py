"""
Lancement du dashboard Streamlit
--------------------------------
Usage: streamlit run run_dashboard.py
ou: python -m streamlit run run_dashboard.py
"""

import subprocess
import sys
from pathlib import Path

if __name__ == "__main__":
    app_path = Path(__file__).parent / "dashboard" / "app.py"
    subprocess.run([sys.executable, "-m", "streamlit", "run", str(app_path), "--server.port", "8501"], check=True)
