import warnings 
warnings.filterwarnings('ignore')
from datetime import datetime

import os
import sys
root_dir = "../../NarrativeExtraction/enhanced/"
sys.path.append(root_dir)
from fileMgmt import *
from spacyNLP import *

configFile = f"{root_dir}configs/extractConfig.json"
smryCols = ["ID", "C_CASE_SUMMARY"]

if __name__ == "__main__":
    _time = datetime.now()
    banner_("XYZ Hotline - Narrative Entity Extraction")
    fileParams, spaCyParams, dataParams = unpack_params(configFile)
    
    print("[*] Collecting session meta data")
    inputPath, sessionPath, sessionLog = session_data(fileParams)
    
    print("[*] Initializing document serialization...")
    data, sessionLog = unpack_data(fileParams, dataParams, sessionLog)
    data_ = data[0:100]
    _docs = [row for row in data_[smryCols[1]].fillna("0")]
 
    docBinFile = serialize_nlp(_docs, sessionPath, 
                               option=f"{root_dir}/bootstrapModel/")

    endTime = datetime.now()
    sessionLog["SerializationEndTime"] = endTime
    logFile = write_to_log(sessionPath, sessionLog)
    
    print(f"[*] End Time: {endTime}")
    time = pd.DataFrame()
    duration = (endTime -_time)
    print(f"[*] Total Serialization Runtime: {duration}")
    
    
if __name__ == "__main__":
    banner_("spaCy NLP - Entity Extraction")
    print("[*] Reloading spaCy Model")
    nlp = spacy.load(f"{root_dir}/bootstrapModel/")
    doc_bin = DocBin().from_disk(docBinFile)
    Docs = list(doc_bin.get_docs(nlp.vocab))

    print("[*] Collecting Entity Labels...")
    cols = nlp.get_pipe("ner").labels
    cols += nlp.get_pipe("entity_ruler").labels

    def deserialize_nlp(docBinFile:str)->(list):
        """Unpack spaCy binaries."""
        doc_bin = DocBin().from_disk(docBinFile)
        print(f"[*] Deserializing {len(doc_bin)} docs..")
        return list(doc_bin.get_docs(nlp.vocab))

    # documents are unpacked
    Docs = deserialize_nlp(docBinFile)

    print("[*] Extracting Entities...")
    extracts = extract_nlp(Docs)

    # Extract table is created
    print("[*] Creating Extract Table")
    extractTable = pd.DataFrame()
    extractTableClean = pd.DataFrame
    for col in cols:
        extractTable[col] = [[e for e in extract[1] 
                              if e[0]==col] 
                            for extract in extracts]

    print("[*] Linking extracts back to narratives...")
    summaries = data_[smryCols]
    extractSummaries = pd.concat([extractTable, summaries], axis=1)
    
    cols = list(extractSummaries.columns[:-2])
    for col in cols:
        extractSummaries[col] = extractSummaries[col].fillna("0").apply(
                                lambda x: x[0] if len(x)>0 else 0)
        extractSummaries[col] = extractSummaries[col].fillna("0").apply(
                                lambda x: x[3] if type(x)==list else x)
        
    outputFile = "extractSummaries.json"
    extractSummaries.to_json(outputFile)
    print(f"[*] Output file located at {outputFile}")