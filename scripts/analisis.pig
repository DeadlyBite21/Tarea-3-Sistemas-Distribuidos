-- 1. REGISTRO DE LIBRERÍAS
-- Usamos la ruta absoluta donde descargamos el jar en el Dockerfile
REGISTER /usr/local/pig/contrib/piggybank/java/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- 2. CARGA DE DATOS
-- Carga del archivo de Stopwords (debe existir en HDFS en /user/input/stopwords_en.txt)
stopwords = LOAD '/user/input/stopwords_en.txt' AS (stopword:chararray);

-- Carga del Dataset Filtrado (dataset_filtrado.csv)
-- 'SKIP_INPUT_HEADER' salta la primera línea con los nombres de columnas
raw_data = LOAD '/user/input/dataset_filtrado.csv' 
    USING CSVExcelStorage(',', 'YES_MULTILINE', 'NOCHANGE', 'SKIP_INPUT_HEADER') 
    AS (col_a:chararray, col_b:chararray);

-- NOTA: Como tu script de Python seleccionó ['question', 'llm_answer']:
-- col_a = question (Usuario)
-- col_b = llm_answer (IA)

-- ============================================================
-- ANÁLISIS 1: COLUMNA A (Question / Usuario)
-- ============================================================

-- Tokenizar: Separar frases en palabras y aplanar la lista
tokens_a = FOREACH raw_data GENERATE FLATTEN(TOKENIZE(col_a)) AS word;

-- Limpieza: Minúsculas y filtrar caracteres no alfabéticos
clean_a_1 = FOREACH tokens_a GENERATE LOWER(word) AS word;
clean_a_2 = FILTER clean_a_1 BY (SIZE(word) > 2) AND (word matches '^[a-z]+$');

-- Filtrar Stopwords (Usamos Replicated Join que es muy rápido para esto)
-- Hacemos un LEFT JOIN y nos quedamos con los que NO encontraron pareja en stopwords
joined_a = JOIN clean_a_2 BY word LEFT, stopwords BY stopword USING 'replicated';
filtered_a = FILTER joined_a BY stopwords::stopword IS NULL;
words_a_final = FOREACH filtered_a GENERATE clean_a_2::word AS word;

-- Conteo (WordCount)
grouped_a = GROUP words_a_final BY word;
count_a = FOREACH grouped_a GENERATE group AS word, COUNT(words_a_final) AS freq;
ordered_a = ORDER count_a BY freq DESC;

-- Guardar Top 50
top_a = LIMIT ordered_a 50;
STORE top_a INTO '/user/output/human_top_50' USING PigStorage(',');

-- ============================================================
-- ANÁLISIS 2: COLUMNA B (LLM Answer / IA)
-- ============================================================

tokens_b = FOREACH raw_data GENERATE FLATTEN(TOKENIZE(col_b)) AS word;

clean_b_1 = FOREACH tokens_b GENERATE LOWER(word) AS word;
clean_b_2 = FILTER clean_b_1 BY (SIZE(word) > 2) AND (word matches '^[a-z]+$');

joined_b = JOIN clean_b_2 BY word LEFT, stopwords BY stopword USING 'replicated';
filtered_b = FILTER joined_b BY stopwords::stopword IS NULL;
words_b_final = FOREACH filtered_b GENERATE clean_b_2::word AS word;

grouped_b = GROUP words_b_final BY word;
count_b = FOREACH grouped_b GENERATE group AS word, COUNT(words_b_final) AS freq;
ordered_b = ORDER count_b BY freq DESC;

top_b = LIMIT ordered_b 50;
STORE top_b INTO '/user/output/llm_top_50' USING PigStorage(',');