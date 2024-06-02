# TMDB Data Aggregation Project

## Project Overview
This project leverages PySpark to perform data aggregation on a TMDB dataset. The primary goals are to create pre-aggregated tables for genres and to identify the most popular film in each original language. Pre-aggregated tables can significantly improve query performance when read frequency is higher than write frequency and the dataset size is substantial.

## Key Components

### 1. Data Loading
- The TMDB dataset is loaded from the local file system to HDFS using PySpark.
- The dataset file used is `tmdb_5000_movies.csv`.

### 2. Popular Films per Language
- Identifies the most popular film in each original language.
- The results are saved in `popular_film_per_lan.csv`.

### 3. Genres Aggregation
- Parses the `genres` column, which contains an array of JSON objects.
- Aggregates the data to create a pre-aggregated table for genres.
- The results are saved in `Genres_Agggregations.csv`.

## Project Structure
- **`tmdb_5000_movies.csv`**: The dataset file.
- **`TMDB_Data_Aggregation.ipynb`**: Jupyter Notebook containing the PySpark code for data aggregation.
- **`popular_film_per_lan.csv`**: CSV file with the most popular film in each original language.
- **`Genres_Agggregations.csv`**: CSV file with the ID, name, and number of movies for each genre.

## Prerequisites
- Hadoop installed and running.
- Docker for running Hadoop containers.
- PySpark environment set up.
- Jupyter Notebook for executing the code.

## Instructions

### 1. Set Up Hadoop Environment
Ensure Hadoop is set up correctly. The provided Docker Compose file can be used to set up a simple Hadoop cluster with a Namenode and Datanode.

#### Docker Compose Configuration:
```yaml
version: '3'
services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    ports:
      - "9870:9870"
    environment:
      - CLUSTER_NAME=test
    volumes:
      - namenode-data:/hadoop/dfs/name
    networks:
      - hadoop

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    environment:
      - CLUSTER_NAME=test
      - CORE_CONF_fs_defaultFS=hdfs://namenode:8020
      - CORE_CONF_hadoop_http_staticuser_user=root
      - HDFS_CONF_dfs_replication=1
    volumes:
      - datanode-data:/hadoop/dfs/data
    networks:
      - hadoop
    depends_on:
      - namenode

volumes:
  namenode-data:
  datanode-data:

networks:
  hadoop:
```

### 2. Run the Jupyter Notebook
- Open `TMDB_Data_Aggregation.ipynb` in Jupyter Notebook.
- Execute each cell in the notebook to load the dataset, perform data aggregation, and generate the required files.

### 3. Data Processing Steps
- **Data Loading**: The dataset is read into a PySpark DataFrame.
- **Popular Films per Language**: Aggregates data to find the most popular film for each original language.
- **Genres Aggregation**: Parses and aggregates the genres data.

### 4. Moving Data to HDFS
Ensure the dataset file `tmdb_5000_movies.csv` is copied to the Hadoop Namenode container:
```python
import subprocess
local_file_path = "./tmdb_5000_movies.csv"
subprocess.run(["docker", "cp", local_file_path, "namenode:/tmp/"])
```

## Output
- **`popular_film_per_lan.csv`**: Contains each original language and the most popular film in that language.
- **`Genres_Agggregations.csv`**: Contains the ID, name, and number of movies for each genre.

## Presentation
For a visual overview and further details, refer to the [Canva presentation](https://www.canva.com/design/DAGE2B9tzN4/Jzj5UglLTAjKnhI9PIELuw/edit?utm_content=DAGE2B9tzN4&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton).

![presentation](image.png)