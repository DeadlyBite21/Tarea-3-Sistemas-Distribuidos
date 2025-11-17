-- Registrar la librería piggybank para leer CSVs complejos (asegúrate de tener el jar)
REGISTER /path/to/piggybank.jar;
DEFINE CSVExcelStorage org.apache.pig.piggybank.storage.CSVExcelStorage();

-- 1. CARGA DE DATOS
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

-- Filtrar la cabecera
data = FILTER raw_data BY id != 'id';

-- !! CAMBIO: Cargar Stopwords en INGLÉS
-- Carga el archivo 'stopwords_en.txt' que creaste en /user/input/
stopwords = LOAD '/user/input/stopwords_en.txt' AS (stopword:chararray);


-- 2. PROCESAMIENTO DE RESPUESTAS HUMANAS (Reference Answer)
human_text = FOREACH data GENERATE reference_answer AS text;
human_words = FOREACH human_text GENERATE FLATTEN(TOKENIZE(text)) AS word;

-- Cadena de limpieza completa (Requerimiento PDF)
-- 2a. Convertir a minúsculas
human_words_lower = FOREACH human_words GENERATE LOWER(word) AS word;
-- 2b. Eliminar signos de puntuación (expresión regular para inglés)
human_words_no_punct = FOREACH human_words_lower GENERATE REPLACE(word, '[\\p{Punct}]', '') AS word;
-- 2c. Filtrar palabras vacías (que quedaron de la puntuación)
human_words_meaningful = FILTER human_words_no_punct BY (word != '');

-- 2d. Usar COGROUP para filtrar stopwords
human_joined_stopwords = COGROUP human_words_meaningful BY word, stopwords BY stopword;
-- Nos quedamos solo con las palabras que NO están en la lista de stopwords
human_words_filtered = FILTER human_joined_stopwords BY IsEmpty(stopwords);
-- Extraemos la palabra limpia
human_final_words = FOREACH human_words_filtered GENERATE group AS word;


-- Agrupar y Contar (sobre las palabras limpias)
human_grouped = GROUP human_final_words BY word;
human_counts = FOREACH human_grouped GENERATE group AS word, COUNT(human_final_words) AS frequency;

human_top = ORDER human_counts BY frequency DESC;
STORE human_top INTO '/user/output/human_vocab_analysis';


-- 3. PROCESAMIENTO DE RESPUESTAS LLM (LLM Answer)
-- Repetimos el proceso exacto para la IA
llm_text = FOREACH data GENERATE llm_answer AS text;
llm_words = FOREACH llm_text GENERATE FLATTEN(TOKENIZE(text)) AS word;

-- Cadena de limpieza completa (Requerimiento PDF)
llm_words_lower = FOREACH llm_words GENERATE LOWER(word) AS word;
llm_words_no_punct = FOREACH llm_words_lower GENERATE REPLACE(word, '[\\p{Punct}]', '') AS word;
llm_words_meaningful = FILTER llm_words_no_punct BY (word != '');
llm_joined_stopwords = COGROUP llm_words_meaningful BY word, stopwords BY stopword;
llm_words_filtered = FILTER llm_joined_stopwords BY IsEmpty(stopwords);
llm_final_words = FOREACH llm_words_filtered GENERATE group AS word;

-- Agrupar y Contar (sobre las palabras limpias)
llm_grouped = GROUP llm_final_words BY word;
llm_counts = FOREACH llm_grouped GENERATE group AS word, COUNT(llm_final_words) AS frequency;

llm_top = ORDER llm_counts BY frequency DESC;
STORE llm_top INTO '/user/output/llm_vocab_analysis';


-- 4. ANÁLISIS EXTRA: Longitud promedio de respuesta (Sin cambios)
length_analysis = FOREACH data GENERATE 
    SIZE(TOKENIZE(reference_answer)) as human_len, 
    SIZE(TOKENIZE(llm_answer)) as llm_len;
avg_lengths = GROUP length_analysis ALL;
final_stats = FOREACH avg_lengths GENERATE 
    AVG(length_analysis.human_len) as avg_human_words, 
    AVG(length_analysis.llm_len) as avg_llm_words;
STORE final_stats INTO '/user/output/length_comparison';