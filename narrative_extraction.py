import warnings 
warnings.filterwarnings('ignore')
from datetime import datetime

import os
import pandas as pd
import numpy as np
from transform import *
from spacy_nlp import *


def begin_session()->(str):
    """Records the start time of the session.
    Creates a datetime stamped directory to save
    all outputs.
    """
    session_time = datetime.now()
    session_time_ = (str(session_time)[:19])
    print(f"[START TIME: {session_time_}]\n")
    session_path = (str(session_time)[:19]
                       ).replace("-", ""
                       ).replace(" ", "_"
                       ).replace(":", "")
    
    if not os.path.exists("./logs/"):
        os.mkdir("./logs/")

    if not os.path.exists(f"./{session_path}/"):
        os.mkdir(session_path)
        log = pd.DataFrame(columns=["SessionTime", "SessionPath"])
        log["SessionTime"] = session_time_
        log["SessionPath"] = session_path
        log.to_json(f"{session_path}/log.json")
        log.to_json(f"./logs/{session_path}.json")

    else:
        print("Session Path Already Exists. Run `begin_session()` again to generate new session path.")
    
    return session_time, session_path, log


def extraction_pipeline(input_path, model)->(None):
    """
    1. Accepts an input path and a spaCy model.
    2. Data is loaded and prepped with transform.py.
    3. Edge cases are diverted to human review.
    4. Accepted cases are passed to spaCy engine for
        auto extraction.
    5. While you run the auto-extraction, check the human review
        directory for edge cases.
    6. See transform.py and spacy_nlp.py for detailed information
        about how each job works.
    7. Check the time outputs to measure runtime performance.
    8. See log.txt for meta-data for your session.
    
    [JOB 1] Data Preparation (transform.py)
        [1.1] Load Data
        [1.2] Resolve Missing Values
        [1.3] Resolve Duplicates
        [1.4] Divide into Quantiles
        
    [JOB 2] Detect Edge Cases (transform.py)
        [2.1] Detect Short Narratives
        [2.2] Detect Dirty Data
        [2.3] Detect Repeat Sources
        
    [JOB 3] spaCy Extraction Pipeline (spacy_nlp.py)
        [3.1] Transform to spaCy Docs
        [3.2] Serialize Outputs
        [3.3] Extract Entities
    """
    session_time, session_path, log = begin_session()
    
    print("__________[SBA Hotline - spaCy Entity Extraction Engine]____________")
    print("="*68)
    print("")
    
    print("______________________[JOB 1 - Data Preparation]_____________________")
    data, log = transform_data(input_path, "C_CASE_SUMMARY", log)
    print("<======================[   JOB 1 - COMPLETE   ]======================>")
    print("")
    
    print("______________________[JOB 2 - Detect Edge Cases]_____________________")
    data, log = EdgeCaseDetector(data, log)
    input_path, log = save_transformation(data, log, session_path)
    print("<======================[   JOB 2 - COMPLETE   ]======================>")
    print("")

    print("____________________[JOB 3 - Auto-Extract Entities]___________________")
    choice = input("Press [1] to start auto-extraction. Press [ENTER] to end pipeline.")
    if str(choice) == '1':
        extraction_engine(input_path, model)
    else:
        pass
    print("<======================[   JOB 3 - COMPLETE   ]======================>")
    print("")
    
    end_time = datetime.now()
    print(f"[END TIME: {end_time}]\n")
    duration = (str(end_time - session_time))[:19]
    print(f"[TOTAL RUNTIME: {duration}]")
    log.loc["EndTime"] = datetime.now()
    log.loc["Duration"] = duration
    log.to_json(f"{session_path}/log.json")
    log.to_json(f"./logs/{session_path}.json")
    return log

if __name__ == "__main__":
    input_path = None
    model = None
    
    while input_path == None:
        input_path = input("INPUT_PATH:")
        model = "./nlp_model/bootstrapModel"
        extraction_pipeline(input_path, model)