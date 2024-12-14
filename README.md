# Travaux pratiques "Introduction à la data ingénierie"

## Sujet

Ce projet a pour objectif de créer un pipeline ETL pour l’ingestion, la transformation et le stockage de données en temps réel issues des bornes de vélos dans plusieurs grandes villes de France (Paris, Nantes, et Toulouse).

Le pipeline permet de récupérer les données open-source des bornes, de les enrichir avec des données descriptives sur les villes via une API publique de l'État français, et de les stocker dans une base de données DuckDB pour analyse.

Ce projet s’appuie sur [une base existante](https://github.com/kevinl75/polytech-de-101-2024-tp-subject) pour Paris et vise à l’étendre à d’autres villes tout en consolidant les données dans un format standardisé.

## Organisation des pipelines

La structure du pipeline du projet initial est conservée, au détail près que les données de 2 villes ont été ajoutées au niveau de l'ingestion avec `get_<city>_realtime_bicycle_data`, à savoir celles de Nantes et de Toulouse.

![Test](/images/image_2.png)

Bien que cette représentation suggère une exécution en parallèle, les limites de l'environnement d'exécution imposent une éxécution séquentielle.

## Principaux changements

Les fonctions de consolidation ont été retravaillées pour gagner en modularité, et factoriser un peu le code. Ensuite, elles sont appelées séquentiellement dans des fonctions qui effectuerons toutes les insertions dans DuckDB. Dans un cas d´application réel avec un orchestrateur par exemple, il serait plus pertinent au contraire de séparer et de parraléliser un maximum ces insertions qui sont (pour la plupart) indépendantes, mais étant donné l'éxécution séquentielle dans notre cas, cela a peu d'impact. 

Ainsi, pour chaque ville un appel à `consolidate_station_data` et `consolidate_station_statement_data` sera effectué. Ces fonctions permettent de sélectionner les colonnes désirées des fichiers `.json`, et de les renommer si nécessaire.

```python
def consolidate_station_data(city_name, columns_selected, rename_columns, station_id):
    data = {}

    with open(f"data/raw_data/{today_date}/{city_name.lower()}_realtime_bicycle_data.json") as fd:
        data = json.load(fd)
    
    city_code = get_commune_code(city_name)
    raw_data_df = pd.json_normalize(data)
    raw_data_df["id"] = raw_data_df[station_id].apply(lambda x: f"{city_code}-{x}")
    raw_data_df["created_date"] = date.today()

    # Creating empty columns for missing data
    missing_columns = [col for col in columns_selected if col not in raw_data_df.columns]

    for col in missing_columns:
        raw_data_df[col] = None  

    station_data_df = raw_data_df[columns_selected]

    station_data_df.rename(columns=rename_columns, inplace=True)

    return station_data_df
```

```python
def consolidate_station_statement_data(city_name, city_code, selected_columns, rename_columns, station_id):
    data = {}

    with open(f"data/raw_data/{today_date}/{city_name.lower()}_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["station_id"] = raw_data_df[station_id].apply(lambda x: f"{city_code}-{x}")
    raw_data_df["created_date"] = date.today()
    station_statement_data_df = raw_data_df[selected_columns]
    
    station_statement_data_df.rename(columns=rename_columns, inplace=True)

    return station_statement_data_df
```

## Exécution du projet

**Installation**

```bash
git clone https://github.com/masudaaaaaaa/polytech-de-101-2024-tp-subject

cd polytech-de-101-2024-tp-subject

python3 -m venv .venv

source .venv/bin/activate

pip install -r requirements.txt
```

**Exécution du pipeline**

```bash
python src/main.py
```
**Connexion à duckdb et requêtes de test**

```bash
./duckdb ./data/duckdb/mobility_analysis.duckdb 
```

```sql
SELECT dm.NAME, tmp.SUM_BICYCLE_DOCKS_AVAILABLE
FROM DIM_CITY dm INNER JOIN (
    SELECT CITY_ID, SUM(BICYCLE_DOCKS_AVAILABLE) AS SUM_BICYCLE_DOCKS_AVAILABLE
    FROM FACT_STATION_STATEMENT
    WHERE CREATED_DATE = (SELECT MAX(CREATED_DATE) FROM CONSOLIDATE_STATION)
    GROUP BY CITY_ID
) tmp ON dm.ID = tmp.CITY_ID
WHERE lower(dm.NAME) in ('paris', 'nantes', 'vincennes', 'toulouse');
```

```sql
SELECT ds.name, ds.code, ds.address, tmp.avg_dock_available
FROM DIM_STATION ds JOIN (
    SELECT station_id, AVG(BICYCLE_AVAILABLE) AS avg_dock_available
    FROM FACT_STATION_STATEMENT
    GROUP BY station_id
) AS tmp ON ds.id = tmp.station_id;
```