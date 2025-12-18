import requests
import re
import pandas as pd
from lxml import etree
from datetime import datetime, timedelta
from utils import all_dics
import argparse
import os
import sqlite3

def get_transp_api(req_params, opt_params, 
                   domains, 
                   periodStart, periodEnd,
                   security_token,
                   return_url=False):
    """
    Retrieves data from ENTSO-E API and returns a consolidated Pandas DataFrame.
    Automatically handles single or multiple TimeSeries.

    Takes as input parameters of the query, retrieves an XML document from the API, and parse it using XPath.
    req_params and opt_params are required and optional parameters respectively, under dictionnary format. 
    If you don't enter optional parameters when it is possible, the request will return the time series for all options of these parameters.

    domains is a dictionnary containing one or several arguments regarding domains of the request. The keys of this dictionnary stay the same 
    within each sub-dataset (ex Market, Load or Generation), but might vary between each sub-dataset.

    A security token has to be generated from Restful Transparency API in order to use this function.
    """
    try:
        
        # Constructing the url from the entered parameters
        url = "https://web-api.tp.entsoe.eu/api?"
        params = {**req_params, **opt_params, **domains}
        url += "&".join([f"{k}={v}" for k, v in params.items()])
        url += f"&periodStart={periodStart}&periodEnd={periodEnd}&securityToken={security_token}"

        if return_url:
            return url
        
        # Retrieving the response and converting it to a format parsable with XPath
        response = requests.get(url)
        if not response.content:
            print("Error: Empty response from API")
            return None
            
        root = etree.fromstring(response.content)
        
        # Retrieving the namespace of the XML document
        tag_match = re.search(r"\{(.+)\}", root.tag)
        if not tag_match:
            print("Error: Could not parse XML namespace")
            return None
        ns = {'ns': tag_match.group(1)}

        # Checking if the XML document indicates an error, and if yes retrieving the error message
        code = root.xpath("//ns:Reason/ns:code/text()", namespaces=ns)
        if code:
            error_msg = root.xpath("//ns:Reason/ns:text/text()", namespaces=ns)
            print(f"API Error {code[0]}: {error_msg[0] if error_msg else ''}")
            return None
        
        timeseries_nodes = root.xpath("//ns:TimeSeries", namespaces=ns)
        if not timeseries_nodes:
            print("No TimeSeries data found.")
            return None

        # Determining the name of the element containing the values (might change for different data)
        first_point_children = root.xpath("//ns:TimeSeries[1]//ns:Point[1]/ns:*", namespaces=ns)
        if len(first_point_children) < 2:
            return None
        value_name = first_point_children[1].tag.split('}')[-1]

        # If there are more than 1 TimeSeries, we identify how the differ, by looking at the metadata describing each TimeSeries,
        # and detecting where this metadata varies
        differing_elements = []
        if len(timeseries_nodes) > 1:
            all_tags = set()
            for ts in timeseries_nodes:
                for child in ts.xpath("./ns:*", namespaces=ns):
                    tag = child.tag.split('}')[-1]
                    if tag not in ["mRID", "Period"]:
                        all_tags.add(tag)

            for tag in all_tags:
                values = [ts.xpath(f"string(ns:{tag})", namespaces=ns).strip() for ts in timeseries_nodes]
                if len(set(values)) > 1:
                    differing_elements.append(tag)

        ### Retrieving the raw data:
        # 1) Retrieving the time interval (start and end) and the resolution (60 minutes or 15 minutes) to go from "position" to an actual date
        # 2) Retrieving the data for each timestamp (linked to a position), storing the value and the corresponding resolution
        # 3) Adding columns for the metadata that varies between each TimeSeries (to be able to differentiate between TimeSeries when joining everything)
        # 4) Adding columns for the metadata describing all the TimeSeries (entered as parameters of the function)

        all_dfs = []
        for ts in timeseries_nodes:
            start_str = ts.xpath(".//ns:Period/ns:timeInterval/ns:start/text()", namespaces=ns)[0]
            resolution = ts.xpath(".//ns:Period/ns:resolution/text()", namespaces=ns)[0]
            res_mins = int(''.join(filter(str.isdigit, resolution)))
            
            # Keep timezone aware (+00:00)
            start_dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
            
            positions = ts.xpath(".//ns:Point/ns:position/text()", namespaces=ns)
            data_values = ts.xpath(f".//ns:Point/ns:{value_name}/text()", namespaces=ns)

            date_times = [start_dt + timedelta(minutes=(int(p) - 1) * res_mins) for p in positions]

            df_ts = pd.DataFrame({
                'date': date_times,
                value_name: data_values,
                'resolution': resolution
            })

            for tag in differing_elements:
                df_ts[tag] = ts.xpath(f"string(ns:{tag})", namespaces=ns).strip()
            for key, value in req_params.items():
                df_ts[key] = value
            for key, value in opt_params.items():
                df_ts[key] = value
                
            all_dfs.append(df_ts)

        raw_df = pd.concat(all_dfs, ignore_index=True)
        raw_df[value_name] = pd.to_numeric(raw_df[value_name])

        ### Handling special case where data is present in 2 different resolutions for the same timestamp in the same document

        # Separate 15 and 60 minutes data
        df_15 = raw_df[raw_df['resolution'] == 'PT15M'].copy()
        df_60 = raw_df[raw_df['resolution'] == 'PT60M'].copy()
        
        # Sum 15-minutes data to obtain hourly data
        if not df_15.empty:
            group_cols = differing_elements + list(req_params.keys()) + list(opt_params.keys())
            if group_cols:
                df_15_hourly = df_15.groupby(
                    group_cols + [pd.Grouper(key='date', freq='1h')]
                )[value_name].sum().reset_index()
            else:
                # If no metadata (only one TimeSeries in the XML document), just resample by time
                df_15_hourly = df_15.groupby(
                    pd.Grouper(key='date', freq='1h')
                )[value_name].sum().reset_index()
        
            df_15_hourly['resolution'] = 'PT60M_aggregated'
        else:
            df_15_hourly = pd.DataFrame()

        # Merging the data, keeping only one value for each timestamp
        # We assign a priority to each resolution: if we have PT60M resolution for a given timestamp we keep this value,
        # otherwise we use PT60M_aggregated data

        df_60['priority'] = 1
        if not df_15_hourly.empty:
            df_15_hourly['priority'] = 2
            combined_df = pd.concat([df_60, df_15_hourly], ignore_index=True)
            
            # Look for duplicates (same timestamp and metadata, but two different rows)
            subset_cols = ['date'] + differing_elements + list(req_params.keys()) + list(opt_params.keys()) 
                       
            # Sort by date/priority and keep only the resolution with first priority
            combined_df = combined_df.sort_values(by=['date', 'priority'])
            final_df = combined_df.drop_duplicates(subset=subset_cols, keep='first').copy()
            final_df = final_df.drop(columns=['priority'])

        else:
            final_df = df_60.drop(columns=['priority'])

        # Filter for exact hours (just in case)
        final_df = final_df[
            (final_df['date'].dt.minute == 0) & 
            (final_df['date'].dt.second == 0)
        ].copy()

        return final_df

    except Exception as e:
        print(f"Error: {e}")
        return None


def process(df_list):
    """
    Takes as input several dataframes obtained from get_trans_api() and outputs one big dataframe collecting all the data 
    in wide format, where each timestamp appears only once. This is done in several steps:

    1) Create "description" column from all the metadata, and transforms it from parameters like "B19 | A94" 
    to some understandable definitions
    2) For each input dataframe, creates a new dataframe in wide format, where each timestamp appears only once, 
    with one column for each different possible metadata
    3) Join all new wide dataframes into one global dataframe (joining on "date", the timestamps)

    """

    unified_dfs = []
    
    for i, df in enumerate(df_list):
        if df is None or df.empty:
            continue
            
        temp_df = df.copy()
        
        # Replace the parameter values (B16, A94, etc.) by the corresponding definitions
        # (found here: https://transparencyplatform.zendesk.com/hc/en-us/articles/15856744319380-Available-Parameters)
        for col in temp_df.columns:
            if col in all_dics:
                temp_df[col] = temp_df[col].map(all_dics[col]).fillna(temp_df[col])
        
        val_col_name = temp_df.columns[1] # This should be 'quantity' or 'price.amount'
        
        # Build Description
        metadata_cols = [c for c in temp_df.columns[2:] if c != 'resolution'] # Start with standard metadata
        # Add resolution to the metadata if there are several resolutions
        if temp_df['resolution'].nunique() > 1:
            metadata_cols.append('resolution')
        
        if len(metadata_cols) > 0:
            temp_df['description'] = temp_df[metadata_cols].astype(str).agg(' - '.join, axis=1)
        else:
            temp_df['description'] = 'Value'
            
        temp_df = temp_df.rename(columns={val_col_name: 'value'})
        unified_dfs.append(temp_df[['date', 'value', 'description']]) # Keeps only the date, value and corresponding description
    
    if not unified_dfs:
        return pd.DataFrame()

    combined_long_df = pd.concat(unified_dfs, ignore_index=True)

    # Convert to Paris Time
    combined_long_df['date'] = combined_long_df['date'].dt.tz_convert('Europe/Paris').dt.tz_localize(None)

    # Aggregate all tables (taking the first value if duplicates are encountered, but that should normally have been dealt with)
    final_df = combined_long_df.pivot_table(index='date', columns='description', values='value', aggfunc='first')

    return final_df.sort_index().reset_index()


def save_to_sqlite(df, table_name, db_path):
    """
    Save DataFrame to SQLite database.
    
    Args:
        df (pd.DataFrame): DataFrame to save
        table_name (str): Name for database table
        db_path (str): Path to SQLite database file
    """
    if df is None or df.empty:
        print("No data to save")
        return
    
    directory = os.path.dirname(db_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    conn.commit()
    conn.close()
    



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ENTSO-E Energy Data Extraction Tool")    
    parser.add_argument('--start_entsoe_api', 
                        type=str, 
                        default="202312312300", 
                        help="Start period in YYYYMMDDHHMM format (default: 202312312300)")
    
    parser.add_argument('--end_entsoe_api', 
                        type=str, 
                        default="202412312300", 
                        help="End period in YYYYMMDDHHMM format (default: 202412312300)")

    args = parser.parse_args()
    periodStart = args.start_entsoe_api
    periodEnd = args.end_entsoe_api

    db_path = 'energy_transparency_data.db'
    security_token = "be61c343-c4e4-4fcc-8b1f-5ebc0ea66a6f"

    ### Generation: Generation Forecast for Wind and Solar
    req_params = {"documentType": "A69", "processType": "A01"}
    domains = {"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"}

    data_generation = get_transp_api(req_params=req_params, opt_params={}, domains=domains,
                            periodStart=periodStart, periodEnd=periodEnd,
                            security_token=security_token)

    ### Market: Energy prices
    req_params = {"documentType": "A44"}
    domains = {"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"}
    data_market = get_transp_api(req_params=req_params, opt_params={}, domains=domains,
                            periodStart=periodStart, periodEnd=periodEnd,
                            security_token=security_token)

    ### Load: Day-ahead total load forecast
    req_params = {"documentType": "A65", "processType": "A01"}
    domains_load = {"outBiddingZone_Domain": "10YFR-RTE------C"}

    data_load = get_transp_api(req_params=req_params, opt_params={}, domains=domains_load,
                            periodStart=periodStart, periodEnd=periodEnd,
                            security_token=security_token)

    # Process all data
    print("\nProcessing energy data...")
    data_list = [data_generation, data_market, data_load]
    final_data = process(data_list)

    # Renaming some columns to shorten names and/or make them clearer
    rename_dict = {
        "Price Document": "Energy prices",
        "System total load - Day ahead": "Total load forecast",
        "Solar generation - Solar - Wind and solar forecast - Day ahead": "Solar generation forecast",
        "Wind generation - Wind Offshore - Wind and solar forecast - Day ahead": "Wind offshore generation forecast",
        "Wind generation - Wind Onshore - Wind and solar forecast - Day ahead": "Wind onshore generation forecast"
    }
    final_data = final_data.rename(columns=rename_dict)

    # Save to database
    print("Saving to database...")
    save_to_sqlite(final_data, 'energy_data', db_path)

    # To save to Excel as backup, if needed
    # final_data.to_excel('data_transparency_api.xlsx', index=False)

    print("\nData processing complete!")


