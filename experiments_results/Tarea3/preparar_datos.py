import pandas as pd
import csv

# Nombre de tu archivo original (asegúrate que sea este)
INPUT_FILE = 'interacciones.csv' 
# Nombre del archivo limpio que vamos a generar
OUTPUT_FILE = 'datos_limpios.tsv'

try:
    # Cargar el dataset original
    df = pd.read_csv(INPUT_FILE)
    
    # Crear una lista para guardar los datos transformados
    data_rows = []
    
    for index, row in df.iterrows():
        # Extraer respuesta humana (Yahoo)
        if pd.notna(row.get('reference_answer')):
            data_rows.append({'source': 'yahoo', 'text': row['reference_answer']})
            
        # Extraer respuesta IA (LLM)
        if pd.notna(row.get('llm_answer')):
            data_rows.append({'source': 'llm', 'text': row['llm_answer']})
    
    # Convertir a DataFrame y guardar como TSV (separado por tabulaciones)
    # Usamos TSV para que las comas en el texto no confundan a Pig
    clean_df = pd.DataFrame(data_rows)
    clean_df.to_csv(OUTPUT_FILE, sep='\t', index=False, header=False)
    
    print(f"¡Éxito! Se generó '{OUTPUT_FILE}' con {len(clean_df)} registros.")
    print("Ahora actualiza tu script Pig para leer este archivo.")

except Exception as e:
    print(f"Error: {e}")
    print("Asegúrate de tener pandas instalado (pip install pandas) y que el nombre del archivo sea correcto.")