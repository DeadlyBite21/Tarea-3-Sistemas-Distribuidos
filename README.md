# 🐷 Tarea 3: Análisis Lingüístico Offline con Hadoop y Pig

Este repositorio contiene la solución para la **Tarea 3** del curso de **Sistemas Distribuidos**. El proyecto implementa un sistema de procesamiento batch utilizando **Apache Hadoop (HDFS)** y **Apache Pig** para realizar un análisis comparativo de vocabulario entre respuestas humanas y respuestas generadas por Grandes Modelos de Lenguaje (LLMs).

🚀 El sistema está completamente contenerizado con **Docker** para facilitar su despliegue y ejecución en cualquier entorno.

---

## 📋 Requisitos Previos

Para ejecutar este proyecto necesitas tener instalado:

- **Docker** y **Docker Compose**.
- **Python 3.x** (para el pre-procesamiento de datos).
- Archivo `dataset.csv` original (debe colocarse en la carpeta `data/`).

---

## 📂 Estructura del Proyecto

El proyecto mantiene la siguiente estructura de archivos y carpetas:

```text
.
├── docker-compose.yml      # Orquestación de contenedores (Namenode, Datanode, Pig Client)
├── hadoop.env              # Variables de entorno para configuración del clúster Hadoop
├── filtrar_datos.py        # Script Python para limpieza y reducción del dataset inicial
├── README.md               # Este archivo
├── data/                   # Carpeta local para datos de entrada y salida
│   ├── dataset.csv         # Archivo original
│   └── stopwords.txt       # Lista de palabras vacías a ignorar
├── scripts/
│   └── analisis.pig        # Script de Pig Latin con la lógica MapReduce
└── pig-image/
    └── Dockerfile          # Imagen personalizada que instala Pig y Piggybank.jar
```

---

## 🛠️ Despliegue del Clúster Hadoop

Construye las imágenes y levanta los contenedores. La imagen personalizada de Pig descargará automáticamente las librerías necesarias (`piggybank.jar`).

```bash
docker-compose up -d --build
```

> [!IMPORTANT]
> Espera unos minutos (1-2 min) hasta que los servicios de Hadoop arranquen por completo.

Puedes verificar que el nodo de datos está activo con:

```bash
docker exec -it namenode hdfs dfsadmin -report
```

---

## 📥 Ingesta de Datos a HDFS

Una vez el clúster está activo, subimos el dataset filtrado y la lista de stopwords al sistema de archivos distribuido (HDFS).

### 1. Crear directorio de entrada en HDFS
```bash
docker exec -it namenode hdfs dfs -mkdir -p /user/input
```

### 2. Subir el dataset filtrado
```bash
docker exec -it namenode hdfs dfs -put /user/input/dataset_filtrado.csv /user/input/dataset_filtrado.csv
```

### 3. Subir las stopwords
> [!NOTE]
> Esto es indispensable para el script Pig.

```bash
docker exec -it namenode hdfs dfs -put /user/input/stopwords.txt /user/input/stopwords_en.txt
```

---

## ⚙️ Ejecución del Análisis (MapReduce)

Ejecutamos el script de Pig Latin dentro del contenedor cliente. Este script realiza:

1.  **Tokenización** de textos.
2.  **Limpieza y normalización**.
3.  **Filtrado de stopwords**.
4.  **Conteo de frecuencia** (WordCount).
5.  **Cálculo del Top 50** palabras más usadas por Humanos y por la IA.

```bash
docker exec -it pig-client pig -x mapreduce /scripts/analisis.pig
```

---

## 📊 Obtención de Resultados

Cuando el proceso termine con el mensaje `Success!`, descarga los resultados desde HDFS a tu carpeta local `data/` para analizarlos.

### Descargar Top 50 palabras de Humanos
```bash
docker exec -it namenode hdfs dfs -getmerge /user/output/human_top_50 /user/input/human_top_50.csv
```

### Descargar Top 50 palabras de LLM
```bash
docker exec -it namenode hdfs dfs -getmerge /user/output/llm_top_50 /user/input/llm_top_50.csv
```
