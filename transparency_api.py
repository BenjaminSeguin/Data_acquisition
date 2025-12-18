import requests
import re
import pandas as pd
from lxml import etree
from datetime import datetime, timedelta
import sqlite3
import os

#Dictionnaries to link parameter codes with actual definitions
dic_documentType = {"A69": "Wind and solar forecast",
               "A44": "Price Document", 
               "A65": "System total load"}

dic_processType = {"A01": "Day ahead"}

dic_businessType = {"A93": "Wind generation",
                    "A94": "Solar generation"}

dic_MktPSRType = {"B16": "Solar",
                  "B18": "Wind Offshore",
                  "B19": "Wind Onshore"}

all_dics = {"documentType": dic_documentType,
            "processType": dic_processType,
            "businessType": dic_businessType,
            "MktPSRType": dic_MktPSRType}


def get_transp_api(req_params, opt_params, 
                   domains, 
                   periodStart, periodEnd,
                   security_token,
                   return_url=False):
    """
    Retrieves data from ENTSO-E API and returns a consolidated Pandas DataFrame.
    Automatically handles single or multiple TimeSeries and converts positions to dates.

    Takes as input parameters of the query, and outputs the root of an XML document, that can be parsed using XPath.
    req_params and opt_params are required and optional parameters respectively, under dictionnary format. 
    If you don't enter optional parameters when it is possible, the request will return the time series for all options of these parameters.

    domains is a dictionnary containing one or several arguments regarding domains of the request. The keys of this dictionnaries stay the same 
    within each sub-dataset (ex Market, Load or Generation), but might vary between each sub-dataset.

    A security token has to be generated from Restful Transparency API in order to use this function.
    """

    try:
        # --- 1. Build URL ---
        # Note: Using '?' for the first parameter is safer than concatenation with '&'
        url = "https://web-api.tp.entsoe.eu/api?"
        
        # Combine parameters
        for key, value in req_params.items():
            url += f"&{key}={value}"
        for key, value in opt_params.items():
            url += f"&{key}={value}"
        for key, value in domains.items():
            url += f"&{key}={value}"

        url += f"&periodStart={periodStart}&periodEnd={periodEnd}&securityToken={security_token}"

        if return_url:
            return url
        
        response = requests.request("GET", url=url, headers={}, data={})
        root = etree.fromstring(response.content)

        #Checking that the xml document retrieved does contain values, and that no error was encountered
        ns = {'ns': re.search(r"\{(.+)\}", root.tag).group(1)}

        #Retrieve the error code and message, if they exist
        code = root.xpath("//ns:Reason/ns:code/text()", namespaces = ns)
        error_msg = root.xpath("//ns:Reason/ns:text/text()", namespaces = ns)
        
        if code != []:
            print (f"Error: code {int(code[0])}: \n {error_msg[0]}")
            return None
        
        timeseries_nodes = root.xpath("//ns:TimeSeries", namespaces=ns)
        if not timeseries_nodes:
            print("No TimeSeries data found in the response.")
            return None

        # Determine the dynamic value node name based on the first Point available in the document
        first_point_elements = root.xpath("//ns:TimeSeries//ns:Point[1]/ns:*", namespaces=ns)
        value_name = first_point_elements[1].tag.split('}')[-1]

        # Identify metadata fields that differ across TimeSeries (if more than one)
        differing_elements = []
        if len(timeseries_nodes) > 1:
            all_tags = set()
            for ts in timeseries_nodes:
                for child in ts.xpath("./ns:*", namespaces=ns):
                    tag = child.tag.split('}')[-1]
                    if tag not in ["mRID", "Period"]:
                        all_tags.add(tag)

            for tag in all_tags:
                values = [node.xpath(f"string(ns:{tag})", namespaces=ns).strip() for node in timeseries_nodes]
                if len(set(values)) > 1:
                    differing_elements.append(tag)

        # Building DataFrame
        all_dfs = []
        for ts in timeseries_nodes:
            # Time calculation metadata
            start_str = ts.xpath(".//ns:Period/ns:timeInterval/ns:start/text()", namespaces=ns)[0]
            resolution = ts.xpath(".//ns:Period/ns:resolution/text()", namespaces=ns)[0]
            res_mins = int(''.join(filter(str.isdigit, resolution)))
            
            # Parse start and keep timezone offset
            start_dt = datetime.fromisoformat(start_str.replace('Z', '+00:00'))

            # Extract data points
            positions = ts.xpath(".//ns:Point/ns:position/text()", namespaces=ns)
            data_values = ts.xpath(f".//ns:Point/ns:{value_name}/text()", namespaces=ns)

            # Map positions to dates
            date_times = [start_dt + timedelta(minutes=(int(p) - 1) * res_mins) for p in positions]

            df_ts = pd.DataFrame({
                'date': date_times,
                value_name: data_values
            })

            # Add columns for differing metadata
            for tag in differing_elements:
                df_ts[tag] = ts.xpath(f"string(ns:{tag})", namespaces=ns).strip()

            all_dfs.append(df_ts)

        # Combine, finalize types, and filter for exact hours
        final_df = pd.concat(all_dfs, ignore_index=True)
        final_df[value_name] = pd.to_numeric(final_df[value_name])
        
        # Add a column for each required parameter
        for key, value in req_params.items():
            final_df[key] = value
        
        # Add a column for each optional parameter
        for key, value in opt_params.items():
            final_df[key] = value

        return final_df

    except Exception as e:
        print(f"Error: {e}")
        return None
    


def process(df_list):
    """
    Translates codes, combines metadata, and merges multiple DataFrames 
    into a single wide-format table indexed by date.
    """
    unified_dfs = []
    
    for df in df_list:
        if df is None or df.empty:
            continue
            
        temp_df = df.copy()
        
        # 1. Translate metadata columns using provided dictionaries
        for col in temp_df.columns:
            if col in all_dics:
                temp_df[col] = temp_df[col].map(all_dics[col]).fillna(temp_df[col])
        
        # 2. Identify main value column (index 1) and metadata (index 2 onwards)
        val_col_name = temp_df.columns[1]
        metadata_cols = temp_df.columns[2:]
        
        # 3. Create the 'description' string
        if len(metadata_cols) > 0:
            temp_df['description'] = temp_df[metadata_cols].astype(str).agg(' - '.join, axis=1)
        else:
            temp_df['description'] = 'Value'
            
        # 4. Standardize column names for concatenation
        # Rename the specific value column (e.g., 'quantity') to a generic 'value'
        temp_df = temp_df.rename(columns={val_col_name: 'value'})
        
        # Keep only date, value, and description for the merge
        unified_dfs.append(temp_df[['date', 'value', 'description']])
    
    if not unified_dfs:
        return pd.DataFrame()

    # 5. Combine all DataFrames vertically
    combined_long_df = pd.concat(unified_dfs, ignore_index=True)

    # Converting to Paris Timezone
    combined_long_df['date'] = combined_long_df['date'].dt.tz_convert('Europe/Paris').dt.tz_localize(None)

    # 6. Pivot to wide format
    # index='date' ensures each timestamp appears only once
    # columns='description' turns each unique metadata string into a column header
    final_df = combined_long_df.pivot(index='date', columns='description', values='value')

    # 7. Sort chronologically and reset index to make 'date' a column again
    final_df = final_df.sort_index().reset_index()
    
    return final_df


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
    
    # Ensure directory exists
    directory = os.path.dirname(db_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    # Connect to database
    conn = sqlite3.connect(db_path)
    
    # Save DataFrame to SQL
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    
    conn.commit()
    conn.close()
    
    print(f"{len(df)} records saved to table '{table_name}' in {db_path}")


if __name__ == "__main__":

    # Define database path in the same folder as the script
    db_path = 'energy_transparency_data.db'

    ### Generation: Generation Forecast for Wind and Solar
    req_params = {"documentType": "A69",
                "processType": "A01"}
    opt_params = {}
    domains = {"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"}

    data_generation = get_transp_api(req_params=req_params, opt_params=opt_params, domains=domains,
                            periodStart="202401010000", periodEnd="202401020000",
                            security_token="be61c343-c4e4-4fcc-8b1f-5ebc0ea66a6f",
                            return_url=False)

    ### Market: Energy prices
    req_params = {"documentType": "A44"}
    opt_params = {}
    domains = {"in_Domain": "10YFR-RTE------C", "out_Domain": "10YFR-RTE------C"}

    data_market = get_transp_api(req_params=req_params, opt_params=opt_params, domains=domains,
                            periodStart="202401010000", periodEnd="202401020000",
                            security_token="be61c343-c4e4-4fcc-8b1f-5ebc0ea66a6f",
                            return_url=False)

    ### Load: Day-ahead total load forecast
    req_params = {"documentType": "A65",
                "processType": "A01"}
    domains = {"outBiddingZone_Domain": "10YFR-RTE------C"}

    data_load = get_transp_api(req_params=req_params, opt_params={}, domains=domains,
                            periodStart="202401010000", periodEnd="202401050000",
                            security_token="be61c343-c4e4-4fcc-8b1f-5ebc0ea66a6f",
                            return_url=False)

    # Process all data
    print("\nProcessing energy data...")
    data_list = [data_generation, data_market, data_load]
    final_data = process(data_list)

    # Save to database
    print("Saving to database...")
    save_to_sqlite(final_data, 'energy_data', db_path)

    # # Also save to Excel as backup
    # final_data.to_excel('data.xlsx', index=False)
    # print(f"Backup saved to data.xlsx")

    print("\nData processing complete!")

    

# ### Retrieves Actual Generation per production Type, France, realised, per 15 minutes
# req_params = {"documentType": "A75",
#             "processType": "A16"}
# opt_params = {}
# domains = {"in_Domain" : "10YFR-RTE------C", "out_Domain" : "10YFR-RTE------C"}

# data = get_transp_api(req_params=req_params, opt_params=opt_params, domains=domains,
#                            periodStart="202401010000", periodEnd="202401020000",
#                            security_token = "be61c343-c4e4-4fcc-8b1f-5ebc0ea66a6f",
#                            return_url=False)
