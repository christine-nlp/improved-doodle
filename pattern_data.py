patterns = {}
# <==== ENTITY ATTRIBUTES ====>
ATTRS = {}

## <==== ATTRS: PERSONAL ID INFO ====>
ATTRS["SSN"] = {}
ATTRS["SSN"]["REGEX"] = r"^(?!000|666)[0-8][0-9]{2}-(?!00)[0-9]{2}-(?!0000)[0-9]{4}$"

ATTRS["EIN"] = {}
ATTRS["EIN"]["REGEX"] = "^((?!11-1111111)(?!22-2222222)(?!33-3333333)(?!44-4444444)(?!55-5555555)(?!66-6666666)(?!77-7777777)(?!88-8888888)(?!99-9999999)(?!12-3456789)(?!00-[0-9]{7})([0-9]{2}-[0-9]{7}))*$"

## <==== ATTRS: PUBLIC FACING INFO ====>
ATTRS["PHONE"] = {}
ATTRS["PHONE"]["REGEX"] = [r"^[1-9]\d{2}-\d{3}-\d{4}", r"^\(\d{3}\)\s\d{3}-\d{4}",
                           r"^[1-9]\d{2}\s\d{3}\s\d{4}", r"^[1-9]\d{2}\.\d{3}\.\d{4}",
    r"^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"]

ATTRS["ONLINE"] = {}
ATTRS["ONLINE"]["SOCIAL_MEDIA"] = ["facebook", "instagram", "linkedin", "snapchat", "twitter"]

# <==== ADDRESSESS ====>
ADDRESS = {}
ADDRESS["LINE1"] = {"alley": "aly", "anex": "anx", "arcade": "arcade", "avenue": "ave", 
                    "bend": "bend", "boulevard": "blvd", "bridge": "bridge", 
                    "brook": "brook", "burg": "burg", "burgs": "burgs", "bypass": "bypass",
                    "center": "ctr", "centers": "ctrs", "circle": "cir", "circles": "cirs", 
                    "common": "cmn", "commons": "cmns", "corner": "cor", "corners": "cors",
                    "court": "ct", "courts": "cts", "crescent": "cres", "crest": "crst",
                    "crossing": "xing", "crossroad": "crossroad", "crossroads": "crossroads",
                    "curve": "cv", "drive": "dr", "drives": "drs", 
                    "expressway": "expy", "extensions": "extension",
                    'field': 'field', 'fields': 'fields', 'fork': 'fork', 'forks': 'forks',
                    'freeway': "fwy", 'gateway': 'gateway', 'green': 'green', 'greens': 'greens',
                    'grove': 'grove', 'groves': 'groves', 'harbor': 'harbor', 'harbors': 'harbors',
                    'haven': 'haven', 'highway': 'hwy', 'hill': 'hill', 'knoll': 'knoll', 'knolls': 'knolls',
                    'lane': 'ln', 'loop': 'loop', 'meadow': 'mdw', 'meadows': 'mdws', 'mount': 'mt',
                    'mnt': 'mountain', 'mnts': 'mountains','meadows': 'meadows', 'mews': 'mews',
                    'motorway': 'motorway', 'mount': 'mount', 'oval': 'oval', 'overpass': 'overpass',
                    'parkway': 'pkwy', 'parkways': 'pkwys', 'pike': 'pk', 'place': 'pl',
                    'plaza': 'plaza', 'passage': 'psge', 'path': 'path', 'point': 'pt', 'points': 'pts',
                    'ridge': 'rdg', 'ridges': 'rdgs', 'road': 'rd', 'roads': 'rds', 'route': 'rt',
                    'spur': 'spur', 'spurs': 'spurs', 'square': 'sq', 'squares': 'squares', 
                    'station': 'station', 'streets': 'sts', 'street': 'st', 'summit': 'summit', 
                    'terrace': 'terrace', 'throughway': 'throughway', 'trace': 'trace', 'track': 'track', 
                    'trafficway': 'trafficway', 'trail': 'trail', 'tunnel': 'trl', 'turnpike': 'turnpike', 
                    'valley': 'vly', 'view': 'view', 'views': 'views', 'vista': 'vista', 
                    'way': 'way', 'ways': 'ways'}

ADDRESS["LINE2"] = ["apt", "bldg", "branch", "dept", "floor", "suite", "ste"]

# <===== Entities ====>
ORGS = {}

## Businesses
ORGS["bus.ORGS"] ={}
ORGS["bus.ORGS"]["REGEX"] = [r"\b[A-Z]\w+(?:\.com?)?(?:[ -]+(?:&[ -]+)?[A-Z]\w+(?:\.com?)?){0,2}[,\s]+(?i:ltd|llc|inc|plc|co(?:rp)?|group|holding|gmbh)\b"]
ORGS["bus.EIN"] = {}
ORGS["bus.EIN"]["REGEX"] = ["^((?!11-1111111)(?!22-2222222)(?!33-3333333)(?!44-4444444)(?!55-5555555)(?!66-6666666)(?!77-7777777)(?!88-8888888)(?!99-9999999)(?!12-3456789)(?!00-[0-9]{7})([0-9]{2}-[0-9]{7}))*$"]

## Financial
ORGS["fin.ORGS"] = {}
ORGS["fin.ORGS"]["fin.SVC"] = ["cgh", "experian", "equifax", 
                                "lifelock", "lexis nexis", 
                                "nacha", "myidcare", "transunion", 
                                "teslar", "kroll"]

ORGS["fin.CRYPTO"] = ["bitcoin", "coinbase"]

ORGS["fin.ORGS"]["fin.APP"] = ["applepay", "cashapp", "paypal", "venmo", "zelle"]

ORGS["fin.ORGS"]["fin.LENDER"] = ["bankable", "blue acorn", "blue vine", 
                                  "fountainhead", "fundbox", 
                                  "kabbage", "keybank", "lendio", 
                                  "medibank", "metabank", "paypal", 
                                  "revenued", "smartbiz", "sunbank", 
                                  "suntrust", "webbank", "womply"]

ORGS["fin.BANK"] = {}
ORGS["fin.BANK"]["BANK"] = ["bank of america", "chase", "discover", "m&t bank", "td bank"]
ORGS["fin.BANK"]["REGEX"] = [r"^.*[Bb]ank$",
                                         r"^.*[Cc]redit [Uu]nion$", 
                                         r"^.*[Ff]inancial.*$",
                                         r"^.*[Ff]unding.*$", 
                                         r"^.*[Ii]nvestments.*$"]

# <=== GOVT ORGANIZATION ===>
ORGS["govt.ORG"] = {"ftc": "federal trade commission",
                                 "irs": "internal revenue service",
                                 "oig": "office of the inspector general",
                                 "oda": "office of disaster assistance",
                                 "sba": "small business administration",
                                 "ssa": "social security administration"}

ORGS["govt.PROG"] = {"eidl": "economic impact disaster loan",
                                      "medicaid": "medicaid",
                                      "medicare": "medicare",
                                      "ppp": "paycheck protection program",
                                      "unemployment": "unemployment"}

ORGS["govt.INVSTG"] = {"national center for disaster relief fraud": "ncdf",
                       "prac": "pandemic response accountability committee",
                       "cigie": "Council of the Inspectors General on Integrity and Efficiency"}

# <==== MONEY ====>
MONEY = {}
MONEY["REGEX"] = [r'^\$(\d*(\d\.?|\.\d{1,2})).*$']

# ================================
# ===== DECLARING PATTERNS =======
# ================================

patterns["ATTRS"] = ATTRS
patterns["ADDRS"] = ADDRESS
patterns["ORGS"] = ORGS
patterns["MONEY"] = MONEY

MONEY_rgx = patterns["MONEY"]


# Individual Entity Names
prefixes = sorted(["mr", "mrs", "ms", "miss",
                   "dr", "governor", "judge", 
                   "mayor", "president"])

suffixes = sorted(["jr", "sr", "ii", "iii", "esq", "md", 
                   "phd", "professor", "dds", "cpa"])

# Business Entity Names
busORGS_rgx = ORGS["bus.ORGS"]["REGEX"]

# Financial Entity Names
finORG = [lender.split(" ") for lender in ["fin.LENDER"]]
finORG_1  = [lnd[0] for lnd in finORG if len(lnd) == 1]
finORG_2 = [lnd for lnd in finORG if len(lnd) > 1]
finORG_rgx = ORGS["fin.BANK"]["REGEX"]
finBANK = ORGS["fin.BANK"]["BANK"]

finEVENTS_1A = ["credit", "cafs", "hard"]
finEVENTS_1B  = ["check", "inquiry", "report"] 

finEVENTS_2A = ["ach", "atm", "sbad"]
finEVENTS_2B = ["cash", "deposit", "transfer", "withdraw"]

finACCTS = ["business", "checking", "credit", "debit", 
             "investment", "personal", "saving"]
finDETAILS = ["in the amount of", "total amount funded"]
finPRODUCT = ["application", "loan"]

finSVC = ORGS["fin.ORGS"]["fin.SVC"]
finAPP = ORGS["fin.ORGS"]["fin.APP"]
finCRYPTO = ORGS["fin.CRYPTO"]

# Government Entity Names
govtORGS_1A = list(patterns["ORGS"]["govt.ORG"].keys())
govtORGS_1B = list(patterns["ORGS"]["govt.ORG"].values())
govtORGS_2A = [org for org in govtORGS_1A if len(org.split(" ")) == 1]
govtORGS_2B = [org.split(" ") for org in govtORGS_1B if len(org.split(" ")) > 1]
govtORGS_1 = [*govtORGS_1A, *govtORGS_2A]
govtORGS_2 = govtORGS_2B

govtPROGS_1A = list(patterns["ORGS"]["govt.PROG"].keys())
govtPROGS_1B = list(patterns["ORGS"]["govt.PROG"].values())
govtPROGS_2A = [org for org in govtPROGS_1A if len(org.split(" ")) == 1]
govtPROGS_2B = [org.split(" ") for org in govtPROGS_1B if len(org.split(" ")) > 1]
govtPROGS_1 = [*govtPROGS_1A, *govtPROGS_2A]
govtPROGS_2 = govtPROGS_2B

govtINVSTG_1A = list(patterns["ORGS"]["govt.INVSTG"].keys())
govtINVSTG_1B = list(patterns["ORGS"]["govt.INVSTG"].values())
govtINVSTG_2A = [org for org in govtINVSTG_1A if len(org.split(" ")) == 1]
govtINVSTG_2B = [org.split(" ") for org in govtINVSTG_1B if len(org.split(" ")) > 1]
govtINVSTG_1 = [*govtINVSTG_1A, *govtINVSTG_2A]
govtINVSTG_2 = govtINVSTG_2B

## Attributes: Public
attrPHONE_rgx = ATTRS["PHONE"]["REGEX"]

attrSocMed = ATTRS["ONLINE"]["SOCIAL_MEDIA"]
attrSocMed += [(n.replace(n, f"https://www.{n}.com/")) for n in attrSocMed]

ADDR_1 = ADDRESS["LINE1"]
addr1 = *ADDR_1.keys(), *ADDR_1.values()
addr2 = ADDRESS["LINE2"]

## Attributes: Private
busEIN_rgx = ORGS["bus.EIN"]["REGEX"]
indvSSN_rgx = ATTRS["SSN"]["REGEX"]

import pandas as pd

def review_extracts(data:pd.DataFrame, n:int)->(int):
    """Outputs text and entities for review from a row
    in a pandas dataframe. 
    """
    import pandas as pd
    import spacy
    nlp = spacy.load("demo")
    text = nlp(data[smryCol].iloc[n])
    
    print("="*54)
    print(f"Narrative Number: {n}")
    print("-"*54)
    print("Entity Extracts")
    print("-"*54)

    for ent in text.ents:
        print(ent.label_, ent.text)
    print("-"*54)
    print("")
    print("="*54)
    print("Narrative Text")
    print("-"*54)
    print(text)
    return