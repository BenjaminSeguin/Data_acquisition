# ENTSO-E Transparency Platform API

Python script to fetch energy data from the ENTSO-E Transparency Platform API and store it in a SQLite database.

## Requirements

```bash
pip install requests pandas lxml
```

## Files

- `transparency_api.py` - Main script to fetch and store energy data
- `transparency_verifications.py` - Script to check database integrity
- `utils.py` - Dictionary definitions for parameter codes

## Quick Start

Run the main script to create the database:

```bash
python transparency_api.py
```

This will create `energy_transparency_data.db` with energy forecasts, prices, and load data.

You can specify custom date ranges, under YYYYMMDDHHMM format

```bash
python transparency_api.py --start_entsoe_api 202401010000 --end_entsoe_api 202412312300
```

Then verify data quality:

```bash
python transparency_verifications.py
```

## Main Functions

### 1. `get_transp_api()`

Retrieves data from the ENTSO-E API and returns a consolidated Pandas DataFrame that automatically handles single or multiple TimeSeries.

**Parameters:**
- `req_params` (dict) - Required parameters (e.g., `{"documentType": "A69", "processType": "A01"}`)
- `opt_params` (dict) - Optional parameters (if empty, returns all options)
- `domains` (dict) - Domain arguments (keys vary by sub-dataset: Market, Load, or Generation)
- `periodStart` (str) - Start period in `YYYYMMDDHHMM` format
- `periodEnd` (str) - End period in `YYYYMMDDHHMM` format
- `security_token` (str) - Your API security token (generate from ENTSO-E Transparency Platform)
- `return_url` (bool) - If True, returns the constructed URL instead of data

**Notes:**

- The French domain code is `10YFR-RTE------C`. For other countries/zones, see the [ENTSO-E Area Codes documentation](https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html#_areas).
- A security token must be generated from the ENTSO-E Restful Transparency API
- Available Parameters: https://transparencyplatform.zendesk.com/hc/en-us/articles/15856744319380-Available-Parameters


**Returns:** Pandas DataFrame with hourly data

**How it works:**

1. **Constructs the API URL** from the entered parameters
2. **Retrieves the XML response** and parses it using XPath
3. **Checks for errors** in the XML document and retrieves error messages if present
4. **Identifies the value element name** (e.g., "quantity", "price.amount") which varies by data type
5. **Detects metadata differences** between TimeSeries by comparing metadata tags
6. **Retrieves raw data** for each TimeSeries:
   - Extracts time interval (start/end) and resolution (60 or 15 minutes)
   - Converts positions to actual timestamps using the resolution
   - Adds metadata columns that vary between TimeSeries
   - Adds parameter metadata describing all TimeSeries
7. **Handles mixed resolutions** (special case where 15-minute and 60-minute data coexist):
   - Separates 15-minute and 60-minute data
   - Aggregates 15-minute data to hourly
   - Merges both resolutions, prioritizing 60-minute data when available
8. **Filters for exact hours** to ensure clean hourly data

### 2. `process()`

Takes multiple DataFrames from `get_transp_api()` and outputs one consolidated DataFrame in wide format where each timestamp appears only once.

**Parameters:**
- `df_list` (list) - List of DataFrames returned by `get_transp_api()`

**Returns:** Pandas DataFrame in wide format with one row per timestamp

**How it works:**

1. **Translates parameter codes** to definitions (e.g., "B19" → "Wind Onshore", "A94" → "Wind generation") using dictionaries from `utils.py`
2. **Creates description columns** from metadata:
   - Combines all metadata columns into a single description string
   - Includes resolution if multiple resolutions exist
3. **Converts to wide format**:
   - Each unique description becomes a column header
   - Each timestamp appears only once as a row
   - Values are aggregated (taking first value if duplicates exist)
4. **Converts to Paris timezone** and removes timezone information
5. **Sorts chronologically** and resets index


## To add more Time Series

To add more datasets, add new API calls following this pattern:

```python
req_params = {"documentType": "A44"}
opt_params = {}
domains = {"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"}

data_market = get_transp_api(
    req_params=req_params, 
    opt_params=opt_params, 
    domains=domains,
    periodStart=periodStart, 
    periodEnd=periodEnd,
    security_token=security_token
)
```

```python
req_params = {"documentType": "A69", "processType": "A01"}
opt_params = {"psrType": "B16"} # Can leave this dictionnary empty, would still work
domains = {"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"}

new_data = get_transp_api(req_params=req_params, opt_params=opt_params, domains=domains,
                          periodStart=periodStart, periodEnd=periodEnd,
                          security_token=security_token)
```

Then add it to the processing list:
```python
data_list = [data_generation, data_market, data_load, new_data]
```

