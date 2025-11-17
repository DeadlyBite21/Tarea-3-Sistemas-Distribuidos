-- Registrar la librería piggybank para leer CSVs complejos (asegúrate de tener el jar)
REGISTER /path/to/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- 1. CARGA DE DATOS
-- Se carga el dataset definiendo el esquema según las columnas del CSV
raw_data = LOAD '/user/input/dataset.csv' USING CSVExcelStorage(',') AS (
    id:chararray,
    question_id:chararray,
    question:chararray,
    reference_answer:chararray, -- Respuesta Humana
    llm_answer:chararray,       -- Respuesta LLM
    final_answer:chararray,
    cached:chararray,
    latency_ms:int,
    score:float,
    model:chararray,
    dist_label:chararray,
    rate:int,
    times_asked:int,
    created_at:chararray,
    score_cosine:float,
    score_rouge1:float,
    score_rougel:float,
    score_bert:float
);

-- Filtrar la cabecera (si el CSV la tiene)
data = FILTER raw_data BY id != 'id';

-- 2. PROCESAMIENTO DE RESPUESTAS HUMANAS (Reference Answer)
-- Seleccionamos solo el texto humano
human_text = FOREACH data GENERATE reference_answer AS text;

-- Tokenizamos: separamos las oraciones en palabras individuales
human_words = FOREACH human_text GENERATE FLATTEN(TOKENIZE(text)) AS word;

-- Limpieza opcional: Convertir a minúsculas para evitar duplicados (ej: "Hola" vs "hola")
-- human_words_clean = FOREACH human_words GENERATE LOWER(word) AS word;

-- Agrupar y Contar
human_grouped = GROUP human_words BY word;
human_counts = FOREACH human_grouped GENERATE group AS word, COUNT(human_words) AS frequency;

-- Ordenar por frecuencia descendente para ver las más comunes
human_top = ORDER human_counts BY frequency DESC;

-- Guardar resultado Humano
STORE human_top INTO '/user/output/human_vocab_analysis';

-- 3. PROCESAMIENTO DE RESPUESTAS LLM (LLM Answer)
-- Repetimos el proceso para la IA
llm_text = FOREACH data GENERATE llm_answer AS text;

llm_words = FOREACH llm_text GENERATE FLATTEN(TOKENIZE(text)) AS word;
llm_grouped = GROUP llm_words BY word;
llm_counts = FOREACH llm_grouped GENERATE group AS word, COUNT(llm_words) AS frequency;

llm_top = ORDER llm_counts BY frequency DESC;

-- Guardar resultado LLM
STORE llm_top INTO '/user/output/llm_vocab_analysis';

-- 4. ANÁLISIS EXTRA: Longitud promedio de respuesta (Patrón léxico)
-- Esto ayuda a responder si la IA es más "verborrea" que el humano
length_analysis = FOREACH data GENERATE 
    SIZE(TOKENIZE(reference_answer)) as human_len, 
    SIZE(TOKENIZE(llm_answer)) as llm_len;

avg_lengths = GROUP length_analysis ALL;
final_stats = FOREACH avg_lengths GENERATE 
    AVG(length_analysis.human_len) as avg_human_words, 
    AVG(length_analysis.llm_len) as avg_llm_words;

STORE final_stats INTO '/user/output/length_comparison';