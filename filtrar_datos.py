import pandas as pd
import os

# Configuración de rutas
input_file = 'data/dataset.csv'
output_file = 'data/dataset_filtrado.csv'

# Verificar que el archivo existe
if not os.path.exists(input_file):
    print(f"Error: No se encuentra el archivo {input_file}")
    exit(1)

print("Cargando dataset...")
try:
    # Leemos el CSV. Pandas maneja automáticamente comillas y saltos de línea.
    df = pd.read_csv(input_file)
    
    # Filtramos solo las columnas que necesitas
    # Asegúrate de que los nombres coincidan EXACTAMENTE con los de tu CSV original
    cols_to_keep = ['question', 'llm_answer']
    
    # Verificamos que las columnas existan
    if not all(col in df.columns for col in cols_to_keep):
        print(f"Error: Las columnas {cols_to_keep} no están en el dataset.")
        print(f"Columnas encontradas: {df.columns.tolist()}")
        exit(1)

    df_filtrado = df[cols_to_keep]

    # Guardamos el nuevo archivo sin el índice
    df_filtrado.to_csv(output_file, index=False)
    
    print(f"¡Éxito! Archivo filtrado guardado en: {output_file}")
    print(f"Filas procesadas: {len(df_filtrado)}")

except Exception as e:
    print(f"Ocurrió un error al procesar el archivo: {e}")