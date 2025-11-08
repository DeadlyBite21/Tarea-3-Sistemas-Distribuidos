/*
 * SCRIPT DE ANÁLISIS LINGÜÍSTICO (WORDCOUNT) CON APACHE PIG
 * * Este script realiza las siguientes tareas:
 * 1. Carga los datos de entrada (respuestas).
 * 2. Carga un archivo de "stopwords" (palabras a ignorar).
 * 3. Limpia los datos: convierte a minúsculas, elimina puntuación.
 * 4. Tokeniza: separa cada respuesta en palabras[cite: 30].
 * 5. Filtra: elimina las stopwords y palabras vacías.
 * 6. Cuenta: calcula la frecuencia de cada palabra[cite: 32].
 * 7. Ordena: muestra las palabras más frecuentes primero.
 * 8. Almacena: guarda los resultados en HDFS.
 *
 * Parámetros esperados:
 * - INPUT_PATH: Ruta en HDFS al archivo de texto de entrada (ej. /datos/entrada/yahoo.txt)
 * - OUTPUT_PATH: Ruta en HDFS donde se guardarán los resultados (ej. /datos/salida/yahoo_conteo)
 * - STOPWORDS_PATH: Ruta en HDFS al archivo de stopwords (ej. /datos/stopwords/es.txt)
 */

-- --- 1. Carga de Datos ---

-- Carga el archivo de stopwords (una palabra por línea)
-- ASUME que este archivo ya está en HDFS en la ruta $STOPWORDS_PATH
stopwords = LOAD '$STOPWORDS_PATH' AS (stopword:chararray);

-- Carga el archivo principal de respuestas (una respuesta por línea)
-- La ruta se pasa como parámetro $INPUT_PATH
respuestas = LOAD '$INPUT_PATH' AS (linea:chararray);


-- --- 2. Limpieza y Tokenización (Map Phase) --- [cite: 27, 30, 31]

-- TOKENIZE: Separa cada línea en palabras.
-- LOWER: Convierte todo a minúsculas.
-- FLATTEN: Transforma la "bolsa" (bag) de palabras de cada línea en tuplas individuales (una palabra por fila).
palabras = FOREACH respuestas GENERATE FLATTEN(TOKENIZE(LOWER(linea))) AS palabra;

-- LIMPIEZA: Elimina todos los signos de puntuación usando una expresión regular.
-- [\\p{Punct}] coincide con cualquier carácter de puntuación.
palabras_limpias = FOREACH palabras GENERATE REPLACE(palabra, '[\\p{Punct}]', '') AS palabra;

-- FILTRADO INICIAL: Elimina cualquier fila que haya quedado vacía después de quitar la puntuación.
palabras_validas = FILTER palabras_limpias BY (palabra IS NOT NULL) AND (TRIM(palabra) != '');


-- --- 3. Filtrado de Stopwords (Map/Reduce Phase) --- 

-- COGROUP agrupa las palabras de 'palabras_validas' con las de 'stopwords'
-- que coincidan.
agrupado_con_stopwords = COGROUP palabras_validas BY palabra, stopwords BY stopword;

-- FILTRADO: Nos quedamos SÓLO con los grupos donde la bolsa de stopwords está VACÍA.
-- Si la bolsa 'stopwords' no está vacía, significa que la palabra era una stopword y se descarta.
palabras_filtradas_grupo = FILTER agrupado_con_stopwords BY IsEmpty(stopwords);

-- Proyectamos de nuevo para quedarnos solo con la palabra
palabras_finales = FOREACH palabras_filtradas_grupo GENERATE group AS palabra;


-- --- 4. Conteo y Orden (Reduce Phase) --- [cite: 32]

-- AGRUPAR: Agrupa todas las instancias de la misma palabra.
agrupado_por_palabra = GROUP palabras_finales BY palabra;

-- CONTAR: Cuenta cuántas tuplas hay en cada grupo.
conteo_palabras = FOREACH agrupado_por_palabra GENERATE 
    group AS palabra, 
    COUNT(palabras_finales) AS frecuencia;

-- ORDENAR: Ordena los resultados por frecuencia de mayor a menor.
conteo_ordenado = ORDER conteo_palabras BY frecuencia DESC;


-- --- 5. Almacenamiento de Resultados ---

-- Almacena el resultado final en la ruta de salida $OUTPUT_PATH
-- Se guarda como un archivo CSV (texto delimitado por comas)
STORE conteo_ordenado INTO '$OUTPUT_PATH' USING PigStorage(',');