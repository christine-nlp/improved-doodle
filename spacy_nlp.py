import spacy
from spacy.tokens import DocBin
import os
import pandas as pd
from transform import *


def config_patterns(patterns:list, model:str)->(str):
    """Configures Entity Ruler by creating a pattern file
    from spaCy patterns defined in the list.
    See pattern_config.ipynb for more information.
    
    Args:
    patterns (list): List of dictionaries containing
                    patterns for spaCy entity ruleer.
    model (str): Name of custom model for patterns.
    
    Returns:
    pattern_path (str): path to JSON file containing
                        spaCy patterns.
    """
    nlp = spacy.load("en_core_web_trf")
    config = {"overwrite_ents": True, "validate": True}
    ruler = nlp.add_pipe("entity_ruler", config=config)
    ruler.add_patterns(patterns)
    pattern_path = f"{model}/patterns.jsonl"
    ruler_path = f"{model}/ruler"
    nlp.to_disk(model)
    ruler.to_disk(pattern_path)
    ruler.to_disk(ruler_path)
    return pattern_path, ruler_path


def save_extracts(data:pd.DataFrame, model:str, 
                      output_path:str)->(None):
    """Converts lists of entity extracts stored in 
    a pandas dataframe to JSON files.
    [1] Loads selected spaCy model with `model`.
    [2] Exclude irrelevant entities. Use the remaining
        entity labels to create columns.
    [3] For each column, create a new column that contains a tuple
        with the document ID, and any entities from the entity 
        extracts where the label matches the column.

    Args:
    data (pd.DataFrame): dataframe of entity extracts
    model (str): Path to custom spaCy model.
    output_path (str): Path to deposit outputs.
    
    Returns:
    (None)
    """
    print("[JOB 3.3] Save Extracts to Entity Tables")
    # [1] Loads selected spaCy model with `model`.
    nlp = spacy.load(model)
    cols = nlp.get_pipe('entity_ruler').labels
    cols += nlp.get_pipe('ner').labels
    
    # [2] Exclude irrelevent entity labels and make column names
    drop = ["WORK_OF_ART", "ORDINAL", "CARDINAL", "LANGUAGE", 
            "LAW", "PRODUCT", "PERCENT"]
    cols = [col for col in cols if col not in drop]

    # [3] For each column, create a new column that contains a tuple
    # with the document ID, and any entities from the entity extracts
    # where the label matches the column
    for n, col in enumerate(list(set(cols))):
        print(f"[*] {n}. Creating {col} DataFrame")
        data[col] = list(zip(data["ID"], [[(ent) for ent in ents if ent[0]==col]
            for ents in data["entities"]]))
        data[col] = data[col].apply(lambda x: [[x[0], list(x[1][n])] 
            for n in range(len(x[1]))] if len(x[1]) > 0 else "0")
        extracts = pd.DataFrame([([items[0]] + items[1]) 
            for row in data[col] for items in row if items != "0"])
        extracts.to_json(f"{output_path}/{col}.json")
        print(f"----> {len(extracts)} {col} Found.")
    return


def entity_recognizer(docs:list, session_path:str, 
                      model:str)->(list, list):
    """[JOB 3: Entity Recognizer]
    Accepts a corpus of text data, converts
    each document to a spaCy Doc object, and saves
    as a spaCy object using the spaCy pipe.
    Entities are extracted from each Doc and
    passed as a list.
    
    Args:
    docs (list): corpus of text documents, as a list
    session_path (str): Path to deposit outputs.
    model (str): Path to custom spaCy model.
    
    Returns:
    docs (list): spaCy documents
    extracts (list): spaCy entity extracts from docs
    """
    print("[JOB 3.1] Converting SpaCy to Docs and DocBins")
    
    # [1] Load spaCy model
    nlp = spacy.load(model)
    print(f"[*] Using spaCy model: {model}")
    
    # [2] Disabling unneeded pipes
    disable = ["tok2vec", "tagger", "parser", 
               "attribute_ruler", "lemmatizer"]
    print(f"[*] Disabling pipes: {disable}")

    # [3] Creating container and directory for Doc Bins
    doc_bin = DocBin()
    doc_bin_dir_path = f"{session_path}/doc_bins/"
    while not os.path.exists(doc_bin_dir_path):
        os.mkdir(doc_bin_dir_path)
     
    # [4] Process text into spaCy Docs
    # Toggle batch size for performance.
    # Depending on your CPU, you may want to
    # change `n_process`.
    for doc in nlp.pipe(docs, batch_size=2000, 
                 n_process=4, disable=disable):
        doc_bin.add(doc)
        
    # [5] Save as a spacy file, return the filepath
    # This step protects your results and allows you to 
    # retrieve your results without running the pipeline again
    doc_bin_path = f"{doc_bin_dir_path}/doc_bin.spacy"
    doc_bin.to_disk(doc_bin_path)

    print("[JOB 3.2] Retrieving DocBins and Extracting Entities")
    doc_bin = DocBin().from_disk(doc_bin_path)
    docs = list(doc_bin.get_docs(nlp.vocab))
    extracts = [[(ent.label_, ent.id_, ent.text, 
                    ent.start_char, ent.end_char) 
                    for ent in doc.ents] for doc in docs]
    return docs, extracts


def extraction_engine(input_path:str, model:str, 
        session_path:str, log:pd.DataFrame)->(None):
    """ JOB 2A: Retrieves accepted narratives, performs 
    extraction, and saves Docs and extracts to disk.
    
    [1] Extraction pipeline retrieves narrative accepted for
    auto-extraction as by `JOB 1: DATA PREPARATION` as JSON
    files. Reads files in with `pd.read_json`.
    
    [2] Narrative data is selected from the dataframe in
    the `narrative` column. 
    - This passed to the `entity_recognizer`. 
    - Narratives are returned as a tuple, containing the 
    spaCy Doc and the extracts.
    
    [3] New columns are added to the dataframe that contain
    the spaCy documents, and the extracts.
    
    [4] Updated is passed to `save_extracts`, and carries with
    it the session path and model.
    
    Args:
    input_path (str): path to json file of accepted narratives
    model (str): path to selected spaCy model
    session_path (str): path generated for outputs
    log (pd.DataFrame): session log
    
    Returns:
    log (pd.DataFrame): session log
    """
    accepted_path = f"{session_path}/transformed/accepted.csv"
    data = pd.read_csv(accepted_path)
    data = data.sample(200)
    docs = data["narratives"]
    
    docs, extracts = entity_recognizer(docs, session_path, model)
    
    data["documents"] = docs
    data["entities"] = extracts
    
    save_extracts(data, model, session_path)
    
    return log