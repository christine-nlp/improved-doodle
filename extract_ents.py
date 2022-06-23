import warnings 
warnings.filterwarnings('ignore')
from datetime import datetime

import os
import sys
from fileMgmt import *
from spacyNLP import *

if __name__ == "__main__":
    _time = datetime.now()
    banner_("SBA Hotline - Narrative Entity Extraction")
    fileParams, spaCyParams, dataParams = unpack_params(configFile)
    
    print("[*] Collecting session meta data")
    inputPath, sessionPath, sessionLog = session_data(fileParams)
    
    print("[*] Initializing document serialization...")
    data, sessionLog = unpack_data(fileParams, dataParams, sessionLog)
    data_ = data[dataRange[0]:dataRange[1]]
    _docs = [row for row in data_[smryCols[1]].fillna("0")]
 
    docBinFile = serialize_nlp(_docs, sessionPath, option="2")

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
    nlp = spacy.load(model)
    doc_bin = DocBin().from_disk(docBinFile)
    Docs = list(doc_bin.get_docs(nlp.vocab))

    print("[*] Collecting Entity Labels...")
    cols = nlp.get_pipe("ner").labels
    cols += nlp.get_pipe("entity_ruler").labels

    def deserialize_nlp(docBinFile:str)->(list):
        """Unpacks spaCy binaries."""
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
    for col in cols:
        extractTable[col] = [[e for e in extract if e[0]==col] 
                        for extract in extracts]

    print("[*] Linking extracts back to narratives...")
    summaries = data_[smryCols]
    extractSummaries = pd.concat([extractTable, summaries], axis=1)
    extractSummaries.to_json(outputFile)
    print(f"[*] Output file located at {outputFile}")

    print("[*] Linking extracts back to narratives...")
    summaries = data_[smryCols]
    extractSummaries = pd.concat([extractTable, summaries], axis=1)
    extractSummaries.to_json(outputFile)
    print(f"[*] Output file located at {outputFile}")