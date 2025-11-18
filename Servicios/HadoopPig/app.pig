-- Tarea 3: Análisis batch (MODO LOCAL)
-- Ejecutar con: pig -x local /pig/app.pig

-- 1) Cargar datos LIMPIOS
-- Usamos PigStorage('\t') porque el script de Python generó un TSV (separado por tabs)
-- Esto evita que las comas dentro del texto rompan la carga.
raw = LOAD '/data/datos_limpios.tsv' USING PigStorage('\t') AS (source:chararray, text:chararray);

-- 2) Filtro de seguridad (asegurar que source sea válido)
data = FILTER raw BY (source == 'yahoo' OR source == 'llm');

-- 3) Pasar a minúsculas
lowered = FOREACH data GENERATE source, LOWER(text) AS text;

-- 4) Limpieza: Dejar solo letras y números
cleaned = FOREACH lowered GENERATE source, REPLACE(text, '[^a-z0-9 ]', ' ') AS text;

-- 5) Tokenizar
tokenized = FOREACH cleaned GENERATE source, FLATTEN(TOKENIZE(text)) AS word;

-- 6) Filtrar vacíos
nonempty = FILTER tokenized BY (word IS NOT NULL) AND (word != '');

-- 7) Filtrar Stopwords (Inglés)
filtered = FILTER nonempty BY NOT (word MATCHES '^(the|to|you|and|is|i|of|it|in|for|on|that|this|with|as|at|from|by|be|are|was|were|have|has|had|a|an|or|if|but|so|not|your|my|our|their|they|we|he|she|me|him|her|them|there|here|what|which|who|whom|when|where|why|how|can|could|would|should|will|just|about|into|over|than|then|also|too|because|while|during|other|such|do|does|did|done|any|all|every|some|no|yes|up|down|out|more|most|much|many|lot|lots|own|same|very|really|ever|never|maybe|sometimes|often|always|else|again|new|old|still|back|one|two|three|n|s|t|ll|re|ve|d)$');

-- 8) Conteo y Agrupación
grp = GROUP filtered BY (source, word);
word_counts = FOREACH grp GENERATE FLATTEN(group) AS (source, word), COUNT(filtered) AS cnt;

-- 9) Separar conjuntos
yahoo = FILTER word_counts BY source == 'yahoo';
llm   = FILTER word_counts BY source == 'llm';

-- 10) Ordenar
yahoo_sorted = ORDER yahoo BY cnt DESC, word ASC;
llm_sorted   = ORDER llm   BY cnt DESC, word ASC;

-- 11) Guardar resultados (ESTA ES LA PARTE CLAVE)
STORE yahoo_sorted INTO '/data/output/wordfreq_yahoo' USING PigStorage(',');
STORE llm_sorted INTO '/data/output/wordfreq_llm' USING PigStorage(',');