import warnings 
warnings.filterwarnings('ignore')
from datetime import datetime
import os
import pandas as pd
import numpy as np
import re


def create_folder_struct(output_path:str)->(tuple):
    """
    Creates directories for transformed data. Narratives saved as
    `accepted` by `transform_data` are passed on for auto-extraction 
    with spaCy. Narratives that are not accepted are `diverted` for
    human review and/or special preprocessing.
    
    Args:
    output_path (str): path for session outputs where directories
                        will be created

    Returns:
    accepted_path (str): path to data accepted for autoextraction
    diverted_path (str): path to data diverted for human review
    """
    # if `./transformed/` does not exist, create it
    transformed_dir = os.path.join(output_path, "transformed")
    if not os.path.exists(transformed_dir):
        os.mkdir(transformed_dir)
        print(f"Directory `transformed` created at {transformed_dir}.")

    # create a directory for accepted and diverted data
    accepted_dir = os.path.join(transformed_dir, "accepted")
    diverted_dir = os.path.join(transformed_dir, "diverted")
    if not os.path.exists(accepted_dir):
        os.makedirs(accepted_dir)
        os.makedirs(diverted_dir)
        return accepted_dir, diverted_dir
    else:
        return accepted_dir, diverted_dir


def define_quants(data:pd.DataFrame, col:str, n:int,
                log:pd.DataFrame)->(pd.DataFrame, pd.DataFrame):
    """
    Accepts narratives in a pandas dataframe, and returns a
    modified dataframe with `narr_length` (narrative character
    length) and `qCut` (quantiles for narrative length).

    Args:
    data (pd.DataFrame): pandas dataframe of narratives
    col (str): name of narrative column
    n (int): number of quantiles for narrative character length
    log (pd.Dataframe): session log

    Returns:
    data (pd.DataFrame): modified data with `qCut` feature added
    log (pd.Dataframe): session log
    """
    # 1. Measure character length
    data["narr_length"] = data[col].apply(
                                lambda x: len(str(x)))
    
    # 2. Create labels for each quantile
    N = n+1
    labels = [f"Q{q}" for q in range(1, N)]
    
    # 3. Create `qCut` column and display distribution
    data["qCut"] = pd.qcut(data["narr_length"], n, labels=labels)
    print("[*] Displaying distribution of quantiles...")
    display(data["qCut"].value_counts())
    return data, log


def save_transformation(data:pd.DataFrame, log:pd.DataFrame,
                        output_path:str)->(None):
    """
    [1] Create directories for accepted and diverted data.
    [2] Save all data to `complete_datapath`.
    [3] Save data accepted for auto-extraction to `accepted_datapath`.
        - Data is accepted if narrative length in characters (`narr_length`)
        is in the 2, 3, 4 of 5 quantiles (`qCut`).
        - Data is diverted if it is in quanitles 1 or 5.
    
    Args:
    data (pd.DataFrame): Dataframe of narratives
    log (pd.DataFrame): Session log
    
    Return:
    data (pd.DataFrame): Dataframe of narratives prepped for extraction
    log (pd.DataFrame): Session Log
    """
    
    # 1. Call on `create_folder_struct` to create output folder
    print("[*] Creating directories, obtaining paths...")
    accepted_path, diverted_path = create_folder_struct(output_path)
    accepted_filepath = f"{accepted_path}/q2xq3xq4.csv"
    
    # 2. Accept for auto-extraction or divert for human review
    print("[*] Accepting and/or diverting narratives by character length...")
    
    # ---> Q2, Q3, Q4 are accepted for auto-extraction
    # For dataframes with more than 100k rows, check the 'accepted' data
    # for edge cases in human review before running the auto extraction.
    accepted = data.loc[data["qCut"].isin(["Q2", "Q3", "Q4"])]
    data.to_csv(accepted_filepath, index=False)
    print(f"[!] {len(accepted)} rows accepted for auto-extraction.")
    print(f"[!] Accepted narratives saved to: {accepted_path}")
    
    # ---> Q1, Q5 are diverted for human review
    diverted_filepath = f"{diverted_path}/q1xq5.csv"
    diverted = data.loc[data["qCut"].isin(["Q1", "Q5"])]
    diverted.to_csv(diverted_filepath, index=False)
    print(f"[!] {len(diverted)} rows diverted to human review. {diverted_filepath}.")
    print(f"[!] Diverted narratives saved to: {diverted_filepath}.\n")
    return accepted_path, log


def transform_data(input_path:str, narr_col:str, 
                   log:pd.DataFrame)->(pd.DataFrame, pd.DataFrame):
    """
        [JOB 1: Transforms data for spaCy extraction engine]
    [1] Creates log; loads data from input path; saves metadata to log.
    [2] Checks data completeness. Replaces NaNs with `0` (as a string).
        - This allows use to handle missing values, as well as normalize
        the data type to a string, for the narrative column.
    [3] Checks data uniqueness. Eliminates any true duplicates.
    [4] Measures character length of narrative text. 
        - The `qCut` column is created to divide the text into 
        five quantiles based on narrative length.
        - Narratives that are too short are usually uninformative. 
        - Narratives that are too long likely contain non 'natural language' text. 
        - Both will decrease the performance of the scripts 
        and accuracy of your results.
    [5] Data that is accepted from Q2, Q3, and Q4 is passed along to the pipeline.
    [6] Data from Q1-Q5 is diverted to Phase 2 Preprocessing.
    
    Args:
    input_path (str): Path to input data CSV file
    narr_col (str): Column in the Pandas dataframe that contains narrative text
    log (pd.DataFrame): Session log
    
    Return:
    data (pd.DataFrame): Dataframe of narratives prepped for extraction
    log (pd.DataFrame): Session Log
    """
    print("-"*72)    
    print(f"[JOB 1.1] Read Data; Initialize Log")
    log = pd.DataFrame(columns=["Description"])
    data = pd.read_csv(input_path)
    
    # 1. Collecting meta-data
    data["TRUE_DUPLICATE"] = data.duplicated()
    mask = data["TRUE_DUPLICATE"] == True
    TRUE_DUPLICATES = len(data[mask])
    data["NARR_DUPLICATE"] = data[narr_col].duplicated()
    mask = data["NARR_DUPLICATE"] == True
    NARR_DUPLICATES = len(data[mask])
    TOTAL_NULL = data[narr_col].isna().sum()

    # 2. Populating log
    print(f"[*] {len(data)} Total Rows in Data.")
    log.loc["UUID Column"] = "ID"
    log.loc["Narrative Column"] = narr_col
    log.loc["New Narrative Column"] = "narratives"
    log.loc["Number of Records"] = len(data)
    log.loc["Number of True Duplicates"] = TRUE_DUPLICATES
    log.loc["Number of Narrative Duplicates"] = NARR_DUPLICATES
    log.loc["Total Missing Values"] = TOTAL_NULL
    print("-"*72)
    
    # 3. Imputing missing values with `0`
    print(f"[JOB 1.2] Count and Resolve Missing Values")
    data["narratives"] = data[narr_col].fillna("0")
    print(f"[!] {TOTAL_NULL} Missing Values Replaced with `0`.")
    print("--- Imputing with zero as a string:")
    print("----- [1] Normalizes the datatype for the column.")
    print("----- [2] Reduces the burden on the auto-extractor.")
    log.loc["NullValues"] = TOTAL_NULL
    print("-"*72)

    print(f"[JOB 1.3] Count and Resolve Duplicate Values")
    print(f"[!] {TRUE_DUPLICATES} True Duplicates Dropped from Data.")
    data = data.drop_duplicates(keep="first")
    print(f"[!] {NARR_DUPLICATES} Narrative Duplicates Dropped from Data.")
    data = data.drop_duplicates(subset=[narr_col], keep="first")
    print(f"[!] {len(data)} Unique Narratives Remaining.")
    print("-"*72)
    
    print(f"[JOB 1.4] Identify Quantile Length")
    print("--- [*] Identify Narrative Character Length Quantiles to Detect Edge Cases")
    print("--- [*] Character length of each narrative identifies extra long or extra short narratives.")
    print("-----  [1] Quantile 1 narratives will yeild few entities.")
    print("-----  [2] Quantile 5 narratives will often be dirty and contain:")
    print("------------> HTML Tags, unsupported character encodings, semi-tabular data.")
    print("------------> [!] These clog the auto-extractor because they are not natural language.")
    data, log = define_quants(data, "narratives", 5, log)
    return data, log