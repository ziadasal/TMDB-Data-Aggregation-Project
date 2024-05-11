# TMDB Data Aggregation Project

## Project Overview:
In this project, we utilize PySpark to perform data aggregation tasks on a TMDB dataset. We aim to create pre-aggregated tables for genres and identify the most popular film in each original language.

Under certain conditions, having pre-aggregated tables can save us a lot of time. The cost of writing the aggregation results in the aggregation tables every time we update the main table is well worth it if the read frequency is much higher than the write frequency and the size of the data is very large.

## Key Components:

### 1. Data Loading:
- The TMDB dataset is loaded from the local file system to HDFS using PySpark.

### 2. Popular Films per Language:
- We identify the most popular film in each original language.
- The `popular_film_per_lan.csv` file contains each original language and the most popular film in each.

### 3. Genres Aggregation:
- We parse the genres column, which is in the form of an array of JSON objects.
- Then, we aggregate the data to create a pre-aggregated table for genres.
- The `Genres_Agggregations.csv` file contains the ID, name, and number of movies for each genre.

## Project Structure:
- **`tmdb_5000_movies.csv`**: The dataset file.
- **`TMDB_Data_Aggregation.ipynb`**: Jupyter Notebook containing the PySpark code for data aggregation.
- **`popular_film_per_lan.csv`**: CSV file containing the most popular film in each original language.
- **`Genres_Agggregations.csv`**: CSV file containing the ID, name, and number of movies for each genre.

## Instructions:
1. Run the Jupyter Notebook `TMDB_Data_Aggregation.ipynb` to execute the PySpark code.
2. Ensure that you have Hadoop installed and running.
3. Check that the paths specified in the code for HDFS directory and local files are correct.
4. Execute the code to generate the required files.

## How to Run:
- Open the Jupyter Notebook `TMDB_Data_Aggregation.ipynb`.
- Execute each cell of the notebook to load the dataset, perform data aggregation, and create the required files.

## Presentation
![presentation](image.png)
https://www.canva.com/design/DAGE2B9tzN4/Jzj5UglLTAjKnhI9PIELuw/edit?utm_content=DAGE2B9tzN4&utm_campaign=designshare&utm_medium=link2&utm_source=sharebutton
