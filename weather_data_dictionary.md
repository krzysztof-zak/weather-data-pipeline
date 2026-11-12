# Weather Data Pipeline – Project Documentation & Data Dictionary

## 1. Project Overview

This project implements an end-to-end weather data pipeline using Databricks and Apache Spark.

The pipeline retrieves historical hourly weather data from the Open-Meteo Historical Weather API, processes and validates the data, and prepares it for analytical use.

The project follows the Medallion Architecture, consisting of three data layers:

- **Bronze:** Raw data ingestion.
- **Silver:** Data cleaning, validation, and transformation.
- **Gold:** Dimensional data model prepared for analytical queries and reporting.

### Data Source

- **Provider:** Open-Meteo
- **API:** Historical Weather API
- **Endpoint:** `https://archive-api.open-meteo.com/v1/archive`
- **Frequency:** Hourly
- **Timezone:** Europe/Warsaw
- **Historical period:** 2020-01-01 to 2026-10-29

### Cities

The pipeline currently collects weather data for six Polish cities:

| City | Latitude | Longitude |
|---|---:|---:|
| Warszawa | 52.2297 | 21.0122 |
| Kraków | 50.0647 | 19.9450 |
| Gdańsk | 54.3520 | 18.6466 |
| Wrocław | 51.1079 | 17.0385 |
| Poznań | 52.4064 | 16.9252 |
| Mińsk Mazowiecki | 52.1794 | 21.5725 |

---

## 2. Data Architecture

The project uses the Medallion Architecture.

| Layer | Purpose | Storage |
|---|---|---|
| Bronze | Store raw data retrieved from the API | Delta Table |
| Silver | Store cleaned and transformed data | Delta Table |
| Gold | Store a dimensional analytical model | Delta Tables |

### 2.1 Bronze Layer

**Table:** `Workspace.weather_data.weather_raw`

Contains raw hourly weather records retrieved from the Open-Meteo API.

The original measurement timestamp is stored as a string.

The Bronze layer also stores the geographical coordinates of each city (`latitude` and `longitude`). These attributes are required to populate the city dimension in the Gold layer.

### 2.2 Silver Layer

**Table:** `Workspace.weather_data.weather_silver`

Contains cleaned and validated weather records.

Transformations include:

- Converting measurement timestamps to the TIMESTAMP data type.
- Removing leading and trailing whitespace from city names when necessary.
- Validating required values.
- Checking uniqueness of city and measurement time combinations.
- Adding calendar-related columns.
- Retaining city geographical coordinates from Bronze.
- Organizing columns into logical groups.

### 2.3 Gold Layer

The Gold layer follows a Star Schema dimensional modelling approach.

It consists of two dimension tables and one fact table:

| Table | Type | Purpose |
|---|---|---|
| `dim_city` | Dimension | Stores city information and geographical attributes. |
| `dim_date` | Dimension | Stores calendar attributes used for time-based analysis. |
| `fact_weather` | Fact | Stores hourly weather measurements and dimension keys. |

The fact table retains the hourly level of detail. Aggregations such as daily, monthly, quarterly, and yearly statistics can be calculated from the hourly data when needed.

---

## 3. Naming Conventions

The project follows consistent naming conventions to improve readability, maintainability, and consistency across notebooks, tables, columns, and variables.

### 3.1 General Rules

- Use `snake_case` for notebook names, table names, column names, and Python variables.
- Use lowercase letters for table and column names.
- Use descriptive names that clearly communicate the purpose of an object.
- Avoid spaces and special characters in technical names.
- Use English for all technical names and documentation.

### 3.2 Notebook Naming

Notebooks follow a numbered naming convention that reflects the execution order and pipeline layer.

**Format:**

`<number>_<layer>_<description>`

Examples:

- `01_weather_bronze_ingestion`
- `02_weather_silver_transformation`
- `03_weather_gold_star_schema`

### 3.3 Table Naming

Tables follow descriptive naming conventions based on their role in the dimensional model.

Examples:

- `weather_raw`
- `weather_silver`
- `dim_city`
- `dim_date`
- `fact_weather`

Tables are organized using the following structure:

`<catalog>.<schema>.<table>`

Example:

`Workspace.weather_data.weather_silver`

### 3.4 Column Naming

- Use lowercase `snake_case`.
- Use descriptive names.
- Use suffixes such as `_2m` and `_10m` when they identify measurement height.
- Do not include measurement units in column names. Units are documented in the Data Dictionary.

Examples:

- `measurement_time`
- `measurement_date`
- `temperature_2m`
- `relative_humidity_2m`
- `wind_speed_10m`
- `shortwave_radiation`
- `city_key`
- `date_key`

### 3.5 Python Variable Naming

- Use lowercase `snake_case` for regular variables and DataFrames.
- Use uppercase `UPPER_SNAKE_CASE` for constants and configuration values.

Examples:

```python
silver_df = spark.table(SILVER_TABLE)

BRONZE_TABLE = "Workspace.weather_data.weather_raw"
SILVER_TABLE = "Workspace.weather_data.weather_silver"
```

### 3.6 SQL Naming

SQL table and column references follow the same lowercase `snake_case` convention.

Use uppercase SQL keywords to improve readability.

Example:

```sql
SELECT
    city,
    measurement_date,
    AVG(temperature_2m) AS average_temperature
FROM Workspace.weather_data.weather_silver
GROUP BY
    city,
    measurement_date;
```

---

## 4. Data Dictionary – Bronze Layer

The Bronze layer contains the original weather variables retrieved from the API, together with city identification and geographical coordinates.

| Column | Description | Data Type | Unit |
|---|---|---|---|
| `city` | Name of the city where the weather measurement was recorded. | STRING | N/A |
| `latitude` | Geographic latitude of the city. | DOUBLE | Degrees |
| `longitude` | Geographic longitude of the city. | DOUBLE | Degrees |
| `measurement_time` | Date and time of the weather measurement. | STRING | N/A |
| `temperature_2m` | Air temperature measured at 2 meters above ground. | DOUBLE | °C |
| `apparent_temperature` | Perceived temperature, accounting for environmental conditions. | DOUBLE | °C |
| `relative_humidity_2m` | Relative humidity measured at 2 meters above ground. | DOUBLE | % |
| `precipitation` | Total precipitation, including rain and snowfall water equivalent. | DOUBLE | mm |
| `rain` | Amount of rainfall. | DOUBLE | mm |
| `snowfall` | Amount of snowfall. | DOUBLE | cm |
| `wind_speed_10m` | Wind speed measured at 10 meters above ground. | DOUBLE | km/h |
| `wind_direction_10m` | Wind direction measured at 10 meters above ground. | DOUBLE | ° |
| `wind_gusts_10m` | Maximum wind gust speed. | DOUBLE | km/h |
| `cloud_cover` | Percentage of the sky covered by clouds. | DOUBLE | % |
| `shortwave_radiation` | Incoming shortwave solar radiation at the surface. | DOUBLE | W/m² |

**Note:** Units represent the default Open-Meteo API settings used in this project. The API response field `hourly_units` should be retained or documented to verify the units returned by the source.

---

## 5. Data Dictionary – Silver Layer

The Silver layer contains the weather variables from Bronze, together with additional calendar attributes.

### 5.1 Original Columns

The original weather columns retain their meanings and units from the Bronze layer.

The `measurement_time` column is converted from STRING to TIMESTAMP.

The `latitude` and `longitude` columns are retained to support the creation of the Gold city dimension.

### 5.2 Derived Columns

These columns are generated from `measurement_time` during the Silver transformation process.

| Column | Description | Data Type | Example |
|---|---|---|---|
| `measurement_date` | Calendar date of the weather measurement. | DATE | 2025-06-15 |
| `year` | Calendar year of the measurement. | INTEGER | 2025 |
| `month` | Calendar month, ranging from 1 to 12. | INTEGER | 6 |
| `quarter` | Calendar quarter, ranging from 1 to 4. | INTEGER | 2 |
| `day` | Day of the month, ranging from 1 to 31. | INTEGER | 15 |

### 5.3 Data Grain

One record represents one hourly weather measurement for one city.

The combination of the following columns must be unique:

- `city`
- `measurement_time`

---

## 6. Gold Layer – Dimensional Model

The Gold layer uses a Star Schema consisting of two dimension tables and one fact table.

### 6.1 Dimension Table – dim_city

Stores descriptive information about the cities included in the pipeline.

| Column | Description |
|---|---|
| `city_key` | Surrogate key identifying a city dimension record. |
| `city_name` | Name of the city. |
| `latitude` | Geographic latitude of the city. |
| `longitude` | Geographic longitude of the city. |

The `city_key` is used to connect city records with the fact table.

### 6.2 Dimension Table – dim_date

Stores calendar information used for time-based analysis.

| Column | Description |
|---|---|
| `date_key` | Key identifying a calendar date. |
| `full_date` | Complete calendar date. |
| `day` | Day of the month. |
| `month` | Calendar month number. |
| `month_name` | Calendar month name. |
| `quarter` | Calendar quarter. |
| `year` | Calendar year. |
| `day_of_week` | Day of the week. |
| `is_weekend` | Indicates whether the date falls on a weekend. |

The date dimension allows weather measurements to be analysed by day, month, quarter, year, and day of the week.

### 6.3 Fact Table – fact_weather

Stores hourly weather measurements and references the dimension tables.

Expected columns include:

- `city_key` – reference to `dim_city`.
- `date_key` – reference to `dim_date`.
- `measurement_time` – exact timestamp of the hourly measurement.
- Weather measurement attributes retained from Silver, including temperature, humidity, precipitation, wind, cloud cover, and radiation.

### 6.4 Fact Table Grain

The grain of `fact_weather` is one hourly weather measurement for one city.

The business key is the combination of:

- City
- Measurement timestamp

The dimension keys provide the relationships between the fact table and its dimensions.

### 6.5 Analytical Use

The hourly fact table provides the foundation for different analytical perspectives.

Examples include:

- Daily average, minimum, and maximum temperature.
- Monthly and quarterly temperature trends.
- Yearly precipitation totals.
- Average daily precipitation.
- Average wind speed and maximum wind gusts.
- Weather comparisons between cities.
- Weekend versus weekday weather comparisons.

These measures can be calculated from the hourly fact data without storing separate pre-aggregated Gold tables.

---

## 7. Data Quality Rules

The pipeline applies the following data quality checks.

| Validation | Description |
|---|---|
| Missing values | Checks required weather fields for NULL values. |
| Timestamp validation | Checks whether measurement timestamps were converted successfully. |
| City validation | Checks for NULL, empty, or whitespace-only city names. |
| Whitespace cleaning | Removes leading and trailing whitespace from city names when necessary. |
| Uniqueness | Checks for duplicate city and measurement time combinations. |
| Value ranges | Checks weather measurements against expected value ranges. |
| Derived columns | Checks that calendar attributes contain no NULL values. |
| Table validation | Checks the saved Silver table's column names and order. |
| Dimension validation | Checks that dimension records are correctly prepared for fact table relationships. |
| Fact validation | Checks the hourly grain and dimension key relationships. |

The pipeline stops when a critical data quality validation fails.

---

## 8. Storage and Table Naming

The project uses Unity Catalog and Delta Lake.

**Catalog:** `Workspace`

**Schema:** `weather_data`

| Layer | Table Name |
|---|---|
| Bronze | `Workspace.weather_data.weather_raw` |
| Silver | `Workspace.weather_data.weather_silver` |
| Gold | `Workspace.weather_data.dim_city` |
| Gold | `Workspace.weather_data.dim_date` |
| Gold | `Workspace.weather_data.fact_weather` |

The Gold tables form a Star Schema, with `fact_weather` connected to `dim_city` and `dim_date`.

---

## 9. Technology Stack

| Technology | Purpose |
|---|---|
| Python | API data extraction and pipeline configuration. |
| REST API | Retrieving historical weather data. |
| Databricks | Data engineering and pipeline execution environment. |
| Apache Spark / PySpark | Distributed data processing and transformation. |
| SQL | Dimensional modelling and analytical queries. |
| Delta Lake | Reliable data storage and transaction management. |
| Unity Catalog | Data organization and table management. |
| Medallion Architecture | Organizing data into Bronze, Silver, and Gold layers. |
| Git | Source code version control. |
| GitHub | Remote repository hosting and project version history. |

---

## 10. Project Structure

The project is maintained in a Databricks Git folder connected to a GitHub repository.

Expected repository structure:

```text
weather-data-pipeline/
│
├── 01_weather_bronze_ingestion
│
├── 02_weather_silver_transformation
│
├── 03_weather_gold_star_schema
│
├── weather_data_dictionary.md
│
├── LICENSE
│
└── README.md
```

The GitHub repository stores the project notebooks and documentation.

Notebook changes are committed and pushed to GitHub through the Databricks Git integration.

---

## 11. Documentation Maintenance

This document should be updated whenever:

- New source columns are introduced.
- Existing columns are renamed or modified.
- Data types or units change.
- New transformations are implemented.
- New dimension or fact tables are created or modified.
- Data quality rules are added or modified.
- The dimensional model changes.

The Data Dictionary serves as the central reference for understanding the structure, meaning, and usage of data throughout the pipeline.