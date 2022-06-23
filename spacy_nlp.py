import spacy
from spacy.tokens import DocBin
from fileMgmt import *
disable = ["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"]

def serialize_nlp(docs:list, sessionPath:str, 
                              option="2")->(str):
    """Initialize spaCy model for entity extraction.    
    Args:
    data (list): List/array-like collection of documents.
    sessionPath (str): Path for session results.
    
    Returns:
    (str): Path to results.
    """    
    if option == "2":
        model = "bootstrapModel/"
    elif option != "2":
        model = option
        
    banner_("spaCy NLP - Document Serialization")
    print("[*] Initializing DocBin...")
    doc_bin = DocBin()
    sessionPath = sessionPath.replace(r"//", "/")
    docBinPath = f"{sessionPath}docBins"
    while not os.path.exists(docBinPath):
        Path(docBinPath).mkdir(parents=True, exist_ok=False)
                  
    print(f"[*] Loading {model}...")
    nlp = spacy.load(model)
    doc_bin = DocBin()
    
    print(f"[*] Applying {model} to text data...")
    for doc in nlp.pipe(docs, batch_size=1000, n_process=4, 
            disable=disable):
        doc_bin.add(doc)
        
    docBinFile = f"{docBinPath}/doc_bin.spacy"
    print(f"[*] Writing results to {docBinFile}...")
    doc_bin.to_disk(docBinFile)
    return docBinFile


def deserialize_nlp(docBinFile:str)->(list):
    """Deserializes (unpacks) spaCy Doc binaries."""
    doc_bin = DocBin().from_disk(docBinFile)
    return list(doc_bin.get_docs(nlp.vocab))


def extract_nlp(docs:list)->(list):
    """Extract entity labels and text from spaCy Docs."""
    corp_sz = list(range(len(docs)))
    extracts = [[(ent.label_, ent.start_char,
                  ent.end_char, ent.text,) 
                  for ent in doc.ents] for doc in docs]
    return list(zip(corp_sz, extracts))