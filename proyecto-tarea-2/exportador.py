import sqlite3
import csv
import os
import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("exportador")

# --- Rutas (dentro del contenedor) ---
# 1. Ruta a la BDD de la Tarea 2
DB_PATH = '/app/data/data.db' # Basado en el servicio 'bdd'

# 2. Ruta al CSV original de Yahoo
CSV_PATH = '/app/generator_data/train.csv' # Basado en el servicio 'generator'

# 3. Ruta de salida (la carpeta de la Tarea 3)
OUTPUT_DIR = '/app/output'
LLM_OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'llm_answers.txt')
YAHOO_OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'yahoo_answers.txt')

# Asegurarse de que el directorio de salida exista
os.makedirs(OUTPUT_DIR, exist_ok=True)
logger.info(f"Directorio de salida preparado en: {OUTPUT_DIR}")

# --- 1. Exportar respuestas del LLM (desde SQLite) ---
logger.info(f"Iniciando exportación de BDD desde: {DB_PATH}")
try:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # La tabla es 'processed_questions' y la columna 'answer'
    cursor.execute("SELECT answer FROM processed_questions")
    
    count = 0
    with open(LLM_OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for row in cursor.fetchall():
            answer_text = str(row[0]).strip()
            if answer_text: # Evitar líneas vacías
                f.write(answer_text + '\n')
                count += 1
    
    conn.close()
    logger.info(f"✅ Se exportaron {count} respuestas del LLM a {LLM_OUTPUT_FILE}")

except sqlite3.OperationalError as e:
    logger.error(f"❌ Error al conectar o consultar la BDD en {DB_PATH}: {e}")
    logger.error("Asegúrate de haber corrido la Tarea 2 ('docker-compose up') al menos una vez.")
except Exception as e:
    logger.error(f"❌ Error inesperado procesando la BDD: {e}")

# --- 2. Exportar respuestas de Yahoo! (desde CSV) ---
logger.info(f"Iniciando exportación de CSV desde: {CSV_PATH}")
try:
    count = 0
    with open(CSV_PATH, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        with open(YAHOO_OUTPUT_FILE, 'w', encoding='utf-8') as f:
            for row in reader:
                # Asumiendo que la columna se llama 'answer' en tu train.csv
                answer_text = str(row.get('answer', '')).strip() 
                if answer_text:
                    f.write(answer_text + '\n')
                    count += 1
    logger.info(f"✅ Se exportaron {count} respuestas de Yahoo! a {YAHOO_OUTPUT_FILE}")
    
except FileNotFoundError:
    logger.error(f"❌ Error: No se encontró el archivo {CSV_PATH}.")
    logger.error("Asegúrate de que 'train.csv' esté en la carpeta 'generator'.")
except Exception as e:
    logger.error(f"❌ Error inesperado procesando el CSV: {e}")

logger.info("Exportación completada.")