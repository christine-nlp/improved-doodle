import warnings 
warnings.filterwarnings('ignore')
from datetime import datetime

import os
import pandas as pd
import numpy as np
import re


def detect_short_narrs(data:pd.DataFrame, thresh:int, 
    log:pd.DataFrame)->(pd.DataFrame, pd.DataFrame):
    """   [JOB 2.1 Detect Short Narratives]
    [1] Measures length of narrative by characters.
    [2] Marks characters with less than 100 characters
        as short.
    
    Args
    data (pd.DataFrame): dataframe of narratives
    thresh (int): character threshold for `SHORT` narratives.
    log (pd.DataFrame): session log
    
    Returns
    data (pd.DataFrame): modified dataframe of narratives,
                         with indicators for edge cases
    log (pd.DataFrame): session log
    """
    print("-"*72)    
    print(f"[JOB 2.1] Detect Short Narratives")
    data["SHORT"] = np.where(data["narr_length"] < thresh, 1, 0)
    mask = data["SHORT"] == 1
    print(f"{len(data[mask])} narratives with length < 100.")
    print(f"{len(data[~mask])} narratives with length > 100.")
    return data, log


def detect_dirty_data(data:pd.DataFrame, 
    log:pd.DataFrame)->(pd.DataFrame, pd.DataFrame):
    """    [JOB 2.2 Detect Dirty Data]
    Accepts a pandas dataframe of narratives, then
    scans the narratives for common edge cases that are
    incompatible with the entity recognizer.
    
    [1] Convert text to lower case.
    [2] Detect HTML; create `HTML` indicator.
    [3] Detect image tags; create `IMG` indicator.
    [4] Detect website form; create `FORM_1`.
    [5] Creates an indicator for `HTML`, `IMG` and
        `FORM_1` to identify each edge case.
    [6] Creates the column `DIRTY` to indicate any
        row with any of those three indicators.
    
    Args
    data (pd.DataFrame): dataframe of narratives
    log (pd.DataFrame): session log
    
    Returns
    data (pd.DataFrame): modified dataframe of narratives,
                         with indicators for edge cases
    log (pd.DataFrame): session log
    """
    print("-"*72)    
    print(f"[JOB 2.2] Detect Dirty Data")  
    data["narratives"] = data["narratives"].apply(
                        lambda x: (str(x).lower()
                        ).replace("   ", " ").strip())

    html_pattern = r"<(?:\"[^\"]*\"['\"]*|'[^']*'['\"]*|[^'\">])+>"
    data["DETECT_HTML"] = data["narratives"].apply(
                        lambda x: [n for n in re.findall(html_pattern, 
                                str(x)) if len(n) > 0] if x else False)
    data["HTML"] = data["DETECT_HTML"].apply(
                        lambda x: 1 if len(x) > 0 else 0)

    img_pattern = r"<img alt="
    data["DETECT_IMG"] = data["narratives"].apply(
                        lambda x: [n for n in re.findall(img_pattern, 
                                str(x)) if len(n) > 0] if x else False)
    data["IMG"] = data["DETECT_IMG"].apply(
                        lambda x: 1 if len(x) > 0 else 0)

    form_pattern = r"\**"
    data["DETECT_FORM_1"] = data["narratives"].apply(
                        lambda x: [n for n in re.findall(form_pattern, 
                                str(x)) if len(n) > 0] if x else False)
    data["FORM_1"] = data["DETECT_FORM_1"].apply(
                        lambda x: 1 if len(x) > 0 else 0)
    
    data["DIRTY"] = data[["HTML", "IMG", "FORM_1"]].sum(axis=1)
    mask = data["DIRTY"] > 0
    dirty_data = data[mask]
    print(f"{len(dirty_data)} narrative marked as dirty data.")
    
    dirty_indicators = ["HTML", "IMG", "FORM_1"]
    dirty_data.loc["TOTAL_DIRTY_BY_TYPE"] = data[["HTML", "IMG", "FORM_1"]].sum(axis=0)
    print("Total Rows Marked as `DIRTY` by Type:")
    display(dirty_data.loc["TOTAL_DIRTY_BY_TYPE", 
                           ["HTML", "IMG", "FORM_1"]].astype(int))
    return data, log


def detect_repeat_sources(data:pd.DataFrame, thresh:int, 
    log:pd.DataFrame)->(pd.DataFrame, pd.DataFrame):
    """       [JOB 2.3: Detect Repeat Sources]
    Uses the first 60 characters and the last 60 characters to
    identify "repeat sources" that have signatures, etc that can be 
    removed to improve processing performance and accuracy. 
    
    [TIP] It can help you create a log of complaintants to exclude.
    
    [1] Check the first sixty characters of each narrative for the
        same salutation. Creates an an indicator.
    [2] Check the last sixty characters of each narrative for the
        same signature. Creates an an indicator.
    [3] Creates an indicator, `SAME_SRC` that indicates if either 
        `SAME_SRC_1` or `SAME_SRC_2` is indicated.
        
    Args
    data (pd.DataFrame): dataframe of narratives
    thresh (int): character threshold for `SHORT` narratives.
    log (pd.DataFrame): session log
    
    Returns
    data (pd.DataFrame): modified dataframe of narratives,
                         with indicators for edge cases
    log (pd.DataFrame): session log
    """
    print("-"*72)    
    print(f"[JOB 2.3] Detect Repeat Sources") 
    # check the first 60 characters
    data["FIRST_60"] = data["narratives"].apply(
                lambda x: str(x).lower()[:60])
    data["SAME_SRC_1"] = data["FIRST_60"].duplicated()
    
    # check the last 60 characters
    data["LAST_60"] = data["narratives"].apply(
                lambda x: str(x).lower()[-60:])
    data["SAME_SRC_2"] = data["LAST_60"].duplicated()
    
    same1, same2 = data["SAME_SRC_1"].sum(), data["SAME_SRC_2"].sum()
    data["SAME_SRC"] = data["SAME_SRC_1"] + data["SAME_SRC_2"]
    mask = data["SAME_SRC"] == True
    same_source = data.copy()[mask]
    
    print(f"Same Source 1 (detected by salutation): {same1}")
    print(f"Same Source 2 (detected by signature): {same2}")
    print(f"{len(same_source)} total narratives marked as repeat sources.")
    return data, log


def EdgeCaseDetector(data:pd.DataFrame, 
    log:pd.DataFrame)->(pd.DataFrame, pd.DataFrame):
    """      [JOB 2 Identify Edge Cases]
    Detects edge cases that will clog the spaCy pipeline.
    [2.1] Short Narratives `detect_short_narrs`.
    [2.2] Dirty Data `detect_dirty_data`.
    [2.3] Detect Repeat Sources `detect_repeat_sources`.
    
    Args
    data (pd.DataFrame): dataframe of narratives
    thresh (int): character threshold for `SHORT` narratives.
    log (pd.DataFrame): session log
    
    Returns
    data (pd.DataFrame): modified dataframe of narratives,
                         with indicators for edge cases
    log (pd.DataFrame): session log
    """
    
    # apply each edge case function
    data, log = detect_short_narrs(data, 100, log)
    data, log = detect_dirty_data(data, log)
    data, log = detect_repeat_sources(data, 5, log)
    
    # create a general indicator that will flag any edge case
    edge_cases = ["SHORT", "DIRTY", "SAME_SRC"]
    data["EDGE_CASE"] = data[edge_cases].sum(axis=1)
    TOTAL_EDGE_CASES = data["EDGE_CASE"].sum(axis=0)
    log.loc["EdgeCases"] = data["EDGE_CASE"]
    log.loc["TotalEdgeCases"] = TOTAL_EDGE_CASES
        
    print("")
    print(f"[!] {int(TOTAL_EDGE_CASES)} Total Edge Cases Detected.")
    
    return data, log