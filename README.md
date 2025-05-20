# TP3BD3 – Air Quality ETL

This project is an **ETL** (Extract – Transform – Load) pipeline coded in **Java**, designed to import and structure air quality data in Quebec. It uses **Docker Compose** to orchestrate a PostgreSQL database, a Java application, and a PGWeb interface.

---

## Project Structure

```
tp3bd3/
├── data/                # Source CSV files (air quality, stations)
├── docker/
│   └── java/            # Java Dockerfile
├── sql/                 # PostgreSQL initialization script (init.sql)
├── src/                 # Java source code
│   └── main/java/
│       ├── Etl/         # ETL pipeline (extractors, loader, runner)
│       ├── Models/      # Data models (Station, Measurement, etc.)
│       └── Utils/       # Utilities (DB, Parsing, Logging, Env)
├── .env                 # Environment variables (DB, user, etc.)
├── docker-compose.yml   # Services configuration
├── pom.xml              # Maven project
└── README.md            # This file
```

---

## Running the Project

Make sure you have **Docker** and **Docker Compose** installed.

1. Clone the project (paste in your terminal or IDE):
   ```bash
   mkdir tp3bd3
   git clone https://github.com/sam0rr/tp3bd3
   cd tp3bd3
   ```

2. Launch the services:
   ```bash
   docker compose up --build
   ```

3. Wait until the `tp3bd3_postgres` container is `healthy` and the Java application executes the ETL.

## Interface Access

Once started:
* View the database with PGWeb: http://localhost:8081

You can explore the tables automatically created and populated by the ETL with the CSV file data.

## Technologies Used

| Component      | Description |
|----------------|-------------|
| Java 21 (lts)  | Main ETL application (via Maven) |
| PostgreSQL 15  | Relational database |
| PGWeb          | Read-only web interface for the DB |
| Docker Compose | Service orchestration |

## ETL Pipeline Description

* **Extractors**:
    * `StationExtractor`, `MeasurementExtractor`: read CSV files in `data/`.
* **Transformations**:
    * Cleaning, parsing, data enrichment.
* **Loading**:
    * Data injected into PostgreSQL via `DataLoader`.

Models are defined in `Models.Etl` and DB interactions in `Utils.Database`.

## Data Model

The ETL pipeline structures the data according to the following schema:

* **Stations**: Geolocated measurement points
* **Measurements**: Air quality readings with date, value, and type
* **Measurement Types**: Pollutants and other tracked indicators (PM2.5, O3, etc.)

## Workflow

1. **Extraction**: Reading CSV files from the shared volume
2. **Transformation**: Cleaning and structuring raw data
3. **Loading**: Insertion into a normalized PostgreSQL database
4. **Visualization**: Access to data via PGWeb

## Testing and Validation

To verify proper functioning:
1. Consult the `stations` table in PGWeb
2. Check the data consistency in `measurements`
3. Examine the Java application logs to follow the ETL process

## Configuration

Project parameters are defined in the `.env` file:
* The .env will be included in the git clone; I didn't exclude it in the gitignore to facilitate grading.

## Technical Documentation

For more information on the implementation:
* See configuration details in `docker-compose.yml`
* Examine the structure in `init.sql`