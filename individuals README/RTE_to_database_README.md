# RTE to SQLite Database

## Overview
Extracts RTE energy data from .xls files (TSV format) and stores in SQLite database.

## Usage

### Command Line
```bash
python RTE_to_database.py
python RTE_to_database.py --folder RTE_daily_data --output rte_energy.db
```

### Python API
```python
from RTE_to_database import RTEDatabase

rte_db = RTEDatabase()
rte_db.create_database_from_folder('RTE_daily_data', 'rte_energy.db')
```

## Database Schema

Table: `energy_data`

- `datetime` TEXT PRIMARY KEY
- `date`, `time` TEXT
- `perimetre`, `nature` TEXT
- `consommation`, `prevision_j1`, `prevision_j` REAL
- Energy sources: `nucleaire`, `eolien`, `solaire`, `hydraulique`, `gaz`, `charbon`, `fioul`, `bioenergies`
- Storage: `pompage`, `stockage_batterie`, `destockage_batterie`
- Exchanges: `ech_physiques`, `ech_comm_angleterre`, `ech_comm_espagne`, `ech_comm_italie`, `ech_comm_suisse`, `ech_comm_allemagne_belgique`
- `taux_co2` REAL
- Detailed breakdowns: `fioul_*`, `gaz_*`, `hydraulique_*`, `bioenergies_*`, `eolien_*`

## Output

For 366 files (2024):
- 8,784 records (hourly data)
- Database size: ~1.7 MB

## Verification

Check database integrity:
```bash
python RTE_verifications.py
```

Checks for:
- Missing values
- Duplicate entries
- Record counts
- Date range

## Merging with Weather Data

```sql
SELECT r.datetime, r.consommation, w.temperature_2m
FROM energy_data r
LEFT JOIN weather_hourly w ON r.datetime = w.datetime AND w.city = 'Paris'
WHERE r.date >= '2024-01-01'
LIMIT 10;
```
- Only rows with actual consumption data (`Consommation` not null) are stored
- Database uses `INSERT OR REPLACE` to handle duplicate timestamps automatically
- Null/missing values (NaN, 'ND', empty) are stored as NULL in database
