import sys
from pathlib import Path

# Permite importar o pacote `shopbrasil` como o Airflow faz (pasta dags no sys.path).
DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
sys.path.insert(0, str(DAGS_DIR))
