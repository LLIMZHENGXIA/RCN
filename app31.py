import os
import streamlit as st
import pandas as pd
import re
from io import StringIO, BytesIO
import csv
import oracledb
import time

# Function Definitions
def search_files(directory, keywords):
    matching_files = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.RPT') or file.endswith('.mcd'):
                continue
            if all(keyword.lower() in file.lower() for keyword in keywords):
                matching_files.append(os.path.join(root, file))
    return matching_files

def clean_cst_file(file_path):
    try:
        with open(file_path, 'r') as file:
            lines = file.readlines()
        data_lines = [line for line in lines if not line.startswith('#')]
        csv_file_path = 'cleaned_data.csv'
        with open(csv_file_path, 'w', newline='') as csv_file:
            writer = csv.writer(csv_file)
            for line in data_lines:
                writer.writerow(line.split())
        st.success(f"Cleaned data has been saved to {csv_file_path}")
    except FileNotFoundError:
        st.error(f"File not found: {file_path}. Please check the file path and try again.")

def clean_csv_file(file_path, output_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    cleaned_lines = []
    skip = False
    for line in lines:
        if re.match(r"^(Date/Time:|Film name:|Stage group:|Lot ID:|Wafer ID:|Cassette recipe name:|Wafer recipe name:|Stage recipe name:)", line):
            continue
        if re.match(r"^(Statistics|Max|Min|Range|Mean|StdD|%StdD|%NonU|CTE)", line):
            skip = True
        if not skip:
            cleaned_lines.append(line)
        if skip and line.strip() == "":
            skip = False
    cleaned_data = ''.join(cleaned_lines)
    df = pd.read_csv(StringIO(cleaned_data))
    keywords = ["Statistics", "Max", "Min", "Range", "Mean", "StdD", "%StdD", "%NonU", "CTE"]
    df = df[~df.apply(lambda row: row.astype(str).str.contains('|'.join(keywords)).any(), axis=1)]
    df.to_csv(output_path, index=False)
    print(f"The cleaned dataset has been saved to '{output_path}'.")

def remove_specified_part_from_csv(input_file_path, output_file_path):
    try:
        with open(input_file_path, 'r') as file:
            lines = file.readlines()
        try:
            End_Header_Data_index = lines.index('End Header Data\n')
            remaining_data = lines[End_Header_Data_index + 1:]
        except ValueError:
            remaining_data = lines
        with open(output_file_path, 'w', newline='') as file:
            file.writelines(remaining_data)
        st.success(f"Specified part removed and data saved to {output_file_path}")
    except FileNotFoundError:
        st.error(f"File not found: {input_file_path}. Please check the file path and try again.")

def filter_columns(data, keywords):
    for keyword in keywords:
        data = data.loc[:, data.columns.str.contains(keyword, case=False)]
    return data

def filter_data(data, keywords):
    for keyword in keywords:
        data = data[data.apply(lambda row: row.astype(str).str.contains(keyword, case=False).any(), axis=1)]
    return data

def extract_p_number(col_name):
    match = re.search(r'P(\d+)', col_name)
    return int(match.group(1)) if match else float('inf')

def compare_columns(raw_data, sigma_data, column_name_raw, column_name_sigma):
    raw_data_col = raw_data[column_name_raw].reset_index(drop=True)
    sigma_data_col = sigma_data[column_name_sigma].reset_index(drop=True)

    max_rows = max(len(raw_data_col), len(sigma_data_col))
    raw_data_col = raw_data_col.reindex(range(max_rows))
    sigma_data_col = sigma_data_col.reindex(range(max_rows))

    comparison_results = pd.DataFrame({
        'Site': range(1, max_rows + 1),
        f'Sigma history data ({column_name_sigma})': sigma_data_col,
        f'Raw data from Tool database ({column_name_raw})': raw_data_col,
        'Matched?': raw_data_col == sigma_data_col
    })

    comparison_results['Matched?'] = comparison_results['Matched?'].replace({True: 'Yes', False: 'No'})
    return comparison_results

# Streamlit app configuration
st.set_page_config(page_title="Data Filtering and Comparison App", layout="wide")
st.title('📊 RCN Data Filtering and Comparison App')

# Raw Data Section
st.sidebar.header("Raw Data")
directory = st.sidebar.text_input("Enter the directory path")
search_keywords = st.sidebar.text_input("Enter keywords to search for files separated by commas").split(',')

if directory and search_keywords:
    with st.spinner('Searching for files...'):
        start_time = time.time()  # Record the start time

        matching_files = search_files(directory, search_keywords)

        end_time = time.time()  # Record the end time
        loading_time = end_time - start_time  # Calculate the elapsed time
        st.sidebar.write(f"Loading time: {loading_time:.2f} seconds")  # Display the loading time

    st.sidebar.write("Matching Files:")
    selected_file = st.sidebar.selectbox("Select a file to use as Raw_data", matching_files)

    if selected_file:
        original_selected_file = selected_file  # Store the original file path
        if selected_file.endswith('.CST'):
            with st.spinner('Cleaning CST file...'):
                clean_cst_file(selected_file)
                selected_file = 'cleaned_data.csv'
        elif selected_file.endswith('.csv'):
            # Apply both cleaning systems for CSV files
            intermediate_file = 'intermediate_cleaned_data.csv'

            # Try to clean the dataset
            try:
                with st.spinner('Cleaning CSV file...'):
                    clean_csv_file(selected_file, intermediate_file)
            except Exception:
                intermediate_file = selected_file  # Use the original file if cleaning fails

            # Try to remove the specified part from the CSV
            try:
                with st.spinner('Removing specified part from CSV...'):
                    remove_specified_part_from_csv(intermediate_file, 'cleaned_data.csv')
            except Exception as e:
                st.warning(f"remove_specified_part_from_csv function failed: {e}")
                if intermediate_file != selected_file:
                    selected_file = intermediate_file  # Use the intermediate file if the second cleaning fails
                else:
                    selected_file = 'cleaned_data.csv'  # Use the cleaned data file if both cleanings fail

            selected_file = 'cleaned_data.csv'

        Raw_data = pd.read_csv(selected_file)
        st.sidebar.write(f"Selected file : {original_selected_file}")
        st.dataframe(Raw_data)

# Sigma Data Section
st.sidebar.header("Sigma Data")
lot_id = st.sidebar.text_input("Enter LotId").strip()
wafer_scribe = st.sidebar.text_input("Enter WaferScribe").strip()
metric_tool_id = st.sidebar.text_input("Enter ToolId").strip()
mfg_process_step = st.sidebar.text_input("Enter MFG_PROCESS_STEP").strip()
wafer_spec_id = st.sidebar.text_input("Enter WaferSpecId").strip()  # New input for WaferSpecId

Sigma_data = pd.DataFrame()
if lot_id and metric_tool_id and wafer_scribe and mfg_process_step and wafer_spec_id:
    with st.spinner('Fetching Sigma data from database...'):
        start_time = time.time()  # Record the start time

        oracledb.init_oracle_client()
        cnxn = oracledb.connect(host='FSRACPROD09', port=1521, service_name='FSRACPROD09.MICRON.COM',
                                user='RDR_F10_YIELD_D', password='Urmothersmellverynice1@')

        query = f"""
        SELECT 
            sw.LOT_ID,
            sw.WAFER_ID,
            sw.WAFER_SCRIBE,
            sm.METRIC_TOOL_ID,
            sw.MFG_PROCESS_STEP,
            sw.WAFER_SPEC_ID,
            std.COMMON_TEST_ID,
            sp.TEST_VALUE
            FROM 
                SIGMA_POINT sp
            INNER JOIN 
                SIGMA_TEST_DEF std ON sp.TEST_DWID = std.TEST_DWID
            INNER JOIN 
                SIGMA_WAFER sw ON sp.RUN_COMPLETE_DATETIME = sw.RUN_COMPLETE_DATETIME
            INNER JOIN 
                SIGMA_MEASUREMENT sm ON sp.MEASUREMENT_OID = sm.MEASUREMENT_OID
            WHERE 
            sw.LOT_ID = '{lot_id}' 
            AND sw.WAFER_SCRIBE = '{wafer_scribe}'
            AND sm.METRIC_TOOL_ID = '{metric_tool_id}'
            AND sw.MFG_PROCESS_STEP = '{mfg_process_step}'
            AND sw.WAFER_SPEC_ID = '{wafer_spec_id}'  
        """

        if cnxn.ping() is None:
            cursor = cnxn.cursor()
            cursor.execute(query)
            Sigma_data = pd.DataFrame.from_records(data=[row for row in cursor],
                                                   columns=[row[0] for row in cursor.description])

            end_time = time.time()  # Record the end time
            loading_time = end_time - start_time  # Calculate the elapsed time
            st.sidebar.write(f"Loading time: {loading_time:.2f} seconds")  # Display the loading time

        else:
            st.error('Connection is NOT VALID!')

    keywords_input = st.sidebar.text_input("Enter keywords for Sigma_data separated by commas").strip()
    if keywords_input:
        keywords = [keyword.strip() for keyword in keywords_input.split(',')]
        filtered_Sigma_data = filter_data(Sigma_data, keywords)
    else:
        filtered_Sigma_data = Sigma_data

st.dataframe(filtered_Sigma_data)

# Initialize or load the comparison results
if 'comparison_results' not in st.session_state:
    st.session_state.comparison_results = pd.DataFrame()

# Comparison Section
st.header("Comparison Section")
if not Raw_data.empty and not Sigma_data.empty:
    raw_column = st.selectbox("Select column from Raw_data to compare", Raw_data.columns.tolist())
    sigma_column = st.selectbox("Select column from Sigma_data to compare", Sigma_data.columns.tolist())

    if st.button("Compare"):
        if raw_column and sigma_column:
            comparison_results = compare_columns(Raw_data, Sigma_data, raw_column, sigma_column)

            # Concatenate and reset column names to avoid duplicate column names error
            st.session_state.comparison_results = pd.concat([st.session_state.comparison_results, comparison_results],
                                                            axis=1)
            st.session_state.comparison_results.columns = [f"{col}_{i}" for i, col in
                                                           enumerate(st.session_state.comparison_results.columns)]

# Display and save the comparison results
if not st.session_state.comparison_results.empty:
    st.subheader("Comparison Results")
    st.dataframe(st.session_state.comparison_results)

    # Save the comparison results to an Excel file
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        st.session_state.comparison_results.to_excel(writer, index=False, sheet_name='Data Comparison')
    output.seek(0)

    st.sidebar.download_button(
        label="Download Comparison Results",
        data=output,
        file_name='comparison_results.xlsx',
        mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

    st.success('Comparison results updated and saved to comparison_results.xlsx')

# Button to create a new Excel file
if st.sidebar.button("Create New Excel File"):
    st.session_state.comparison_results = pd.DataFrame()
    st.success('New Excel file created. You can start a new comparison.')