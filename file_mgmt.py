import os
import json
from datetime import datetime
from pathlib import Path
import pandas as pd


def banner_(text:str)->(None):
    """Prints a simple banner."""
    print("="*68)
    print("")
    print(text)
    print("-"*68)
    print("")
    return


def unpack_params(configFile:str)->(dict, dict, dict):
    """Unpacks params to assign variables in pipeline
    according to a JSON config file.
    
    Args
    configFile (dict): Session parameters.
    
    Returns
    fileParams (dict):  input/output directories, filenames, etc.
    spaCyParams (dict): base model, custom , etc.
    dataParams (dict): index column, document column, etc.
    """
    with open(configFile, 'r') as f:
        params = json.load(f)
    fileParams = params["FileMgmtParams"]
    spaCyParams = params["SpaCyParams"]
    dataParams = params["DataParams"]
    return fileParams, spaCyParams, dataParams


def metadata_(logFile:dict, 
            metadata:list)->(dict):
    logFile["SessionData"] = {
            "SessionDate": metadata[0],
            "SessionTime": metadata[1],
            "SessionPath": metadata[2]}
    return logFile


def write_to_log(sessionPath:str, logFile:dict)->(dict):
    """Write to session log file."""
    with open(f"{sessionPath}log.txt", 'w') as f:
        f.write(str(logFile))
        return logFile
        
        
def init_log(fileParams:dict, sessionPath:str)->(dict):
    """Initializes entity extraction session log.
        
    Args:
    fileParams (dict): Input/Output paths, etc.
    sessionPath (str): Path to new session directory.
    """
    input_dir = fileParams["input_dir"]
    fileName = fileParams["input_filename"]
    filePath = f"{input_dir}{fileName}"
    _logFile = {"Input": filePath}
    logFile = write_to_log(sessionPath, _logFile)
    return _logFile


def unpack_data(fileParams:dict, dataParams:dict,
              logFile:dict)->(pd.DataFrame, dict):
    """Reads a csv file of case summaries into a 
    Pandas DataFrame, and creates a log file.
    
    Args:
    fileParams (dict): Input/Output paths, etc.
    dataParams (dict): Column names, etc.
    logFile (dict): File with session metadata.
    
    Returns:
    (pd.DataFrame): Dataframe of case summaries.   
    """ 
    print("[*] Upacking input params...")
    input_dir = fileParams["input_dir"]
    filename = fileParams["input_filename"]
    filePath = f"{input_dir}{filename}"
    
    print(f"[*] Unpacking data from {filePath}")
    sessionPath = logFile["SessionData"]["SessionPath"]
    smryCol = dataParams["smryCol"]
    idCol = dataParams["idCol"]
    
    data = pd.read_csv(filePath, low_memory=False, 
               usecols=[idCol, smryCol])

    print("[*] Updating log...")
    shp = data.shape
    cols = idCol, smryCol
    logFile["InputShape"] = shp
    logFile["InputFields"] = {"Documents": smryCol, 
                                  "Indexes": idCol}
    logFile = write_to_log(sessionPath, logFile)
    return data, logFile


def session_data(fileParams:dict)->(str, dict):
    """Initialize session by creating datestamped/timestamped
    session directory and log file.
    
    Args:
    fileParams (dict): Input/Output paths, etc.
    
    Returns:
    sessionPath (str): Path to new session directory.
    logFile (dict): Log file of session data.
    """
    output_dir = fileParams["output_dir"]
    now = datetime.now()
    sessionDate = datetime.now().strftime("20%y_%m_%d")
    startTime = datetime.now().strftime("%H:%M:%S")
    sessionTime = startTime[:5].replace(':', '.')
    sessionPath = f"{output_dir}/{sessionDate}/{sessionTime}/"
    input_dir = fileParams["input_dir"]
    input_file = fileParams["input_filename"]
    inputPath = f"{input_dir}/{input_file}"
    metadata = [sessionDate, sessionTime, sessionPath]
    
    while not os.path.exists(sessionPath):
        Path(sessionPath).mkdir(parents=True, exist_ok=False)
        # logger should be a class
        logFile = init_log(fileParams, sessionPath)
        logFile = metadata_(logFile, metadata)
        logFile = write_to_log(sessionPath, logFile)
        print(f"[*] Session beginnging at {startTime}")
        return inputPath, sessionPath, logFile
    
    if os.path.exists(sessionPath):
        logFile = init_log(fileParams, sessionPath)
        logFile = metadata_(logFile, metadata)
        logFile = write_to_log(sessionPath, logFile)
        print(f"[*] Session beginnging at {startTime}")
        return inputPath, sessionPath, logFile