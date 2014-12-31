from __future__ import division
import pandas as pd
import numpy as np
import datetime
import glob
import gzip
import simplejson
import pickle
from collections import Counter

print "Loading all dataset..."
dataset = pd.read_csv("allAOL.txt.gz", sep="\t", compression="gzip")
#Shape: (36.389.567, 5)

#print "Removing dash queries..."
# Remove queries for "-": ( 1.000.375 removed queries ) 
#dataset = dataset[ dataset.Query != "-" ]
#dataset = dataset[dataset.Query != "-"]
# Just set it as a dash, not excluding
dataset["dash"] = dataset.Query == "-"

#Shape: (35.389.192, 5) 

print "Sorting by time..."
#sort by time:
dataset.sort("QueryTime", inplace=True)

#Adding information about the last query, to further  be able to create sessions
idDateCache = {}
def getLastQueryById(row):
    id = row["AnonID"]
    time = row["QueryTime"]
    if id in idDateCache:
        returnValue = idDateCache[id]
        idDateCache[id] = time
        return returnValue
    idDateCache[id] = time
    return np.nan

print "Getting the last query time..."
# it works only because I am using the global var idDateCache
dataset["LastQueryTime"] = dataset.apply(getLastQueryById, axis=1)

#Creating sessions:
idSessionCounter = {}
dataset["QueryTime"] = pd.to_datetime(dataset.QueryTime)
dataset["LastQueryTime"] = pd.to_datetime(dataset.LastQueryTime)

def getSessionCounter(row):
    id = row["AnonID"]
    qTime = row["QueryTime"] - row["LastQueryTime"]
    if id not in idSessionCounter:
        idSessionCounter[id] = 1
        return idSessionCounter[id]

    #if the difference is a time:
    if type(qTime) == datetime.timedelta:
        if qTime.total_seconds() > 1800:  # 30 Minutes = 1200 secs
            # it is another session
            idSessionCounter[id] += 1
    return idSessionCounter[id]

print "Getting the session counter..."
dataset["SessionNum"] = dataset.apply(getSessionCounter, axis=1)


def loadODP(path="/data/palotti/logAnalysisDataSets/dmoz/"):
    files = glob.glob("/data/palotti/logAnalysisDataSets/dmoz/*.out.gz") 
    # In this directory there are two health files: health and healthNoAnimals.
    # I decided to use only healthNoAnimals, therefore I am removing this one:
    files = set(files) - set([path + "health.out.gz"])

    healthFile = [path + "health-noAnimal.out.gz"]
    nonHealthFiles = set(files) - set(healthFile)
    
    hurl = set()
    for f in healthFile:
        fopened = gzip.open(f)
        for row in fopened:
            hurl.add(simplejson.loads(row)["url"].rstrip("/"))

    nhurl = set()
    for f in nonHealthFiles:
        fopened = gzip.open(f)
        for row in fopened:
            nhurl.add(simplejson.loads(row)["url"].rstrip("/"))

    return hurl, nhurl
    

print "Loading ODP data..."
hurl, nhurl = loadODP()

""" Some possibilities"""
# Remove intersection...
nhurl_uniq = nhurl - hurl 

def cleanURL(url):
    if type(url) == str:
        if url.startswith("http://"):
            return url.split("/")[2]
        else:
            return url.split("/")[0]
    return url
dataset["ClickURL"] = dataset["ClickURL"].apply(cleanURL)

# Remove http:// and take main domain for health sites: 
h1 = []
for h in hurl:
    h1.append( cleanURL(h) )
hurl_domain = set(h1)

# Remove http:// and take main domain for non-health sites: 
h1 = []
for h in nhurl_uniq:
    h1.append( cleanURL(h) )
nhurl_domain = set(h1)

listOfSpecialNames = ["care", "med", "md", "health", "aid", "mayo", "cdc", "nlm", "cancer", "nurs", "chil", "food",
                      "diet", "smoke", "life", "liver", "help", "dea", "trials", "research", 
                      "drug", "diabet", "dr", "doctor", "flu", "pain", "phys", "heal", "calorie", "pregnancy"] 

domain_to_keep = set()
for domain in hurl_domain.intersection(nhurl_domain):
    for name in listOfSpecialNames:
        if name in domain:
            domain_to_keep.add(domain)
domain_to_remove = hurl_domain.intersection(nhurl_domain) - domain_to_keep
hurl_domain = hurl_domain - domain_to_remove
nhurl_domain = nhurl_domain - domain_to_keep

#final attribuition:
hurl = hurl_domain
nhurl = nhurl_domain
pickle.dump(hurl, open("hurl.odp.pickle","wb"))
pickle.dump(nhurl, open("nhurl.odp.pickle","wb"))

def checkODP(url):
    if type(url) == str:
        if url in hurl:
            return "H"
        elif url in nhurl:
            return "NH"
    return np.nan

dataset["ODP"] = dataset["ClickURL"].apply(checkODP)
dataset.to_pickle("aolAll_odp.pickle")

grouped = dataset.groupby(["AnonID","SessionNum"])
idSessionType = {}
idSessionTypeExcludeNan = {}

for name, group in grouped:
    odpc = Counter(group.ODP.values)
    idSessionType[name] = odpc["H"] / sum(odpc.values())
    if (odpc["H"] + odpc["NH"]) > 0:
        idSessionTypeExcludeNan[name] = odpc["H"] / (odpc["H"] + odpc["NH"])
    else:
        idSessionTypeExcludeNan[name] = 0.0

totalQueries = dataset.shape[0]
# Without ignoring the np.nan
print "Using np.nan"
for healthThr in [0.0001, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
    healthCounter = 0
    for name, value in idSessionType.iteritems():
        if value >= healthThr:
            healthCounter += 1
    
    hqueries = 0
    for item in dataset[["AnonID","SessionNum"]].values:
         hqueries += int( idSessionType[tuple([item[0],item[1]])] > healthThr )

    print healthThr, healthCounter, healthCounter/ len(idSessionType), hqueries, hqueries / totalQueries

print "Ignoring np.nan"
# Ignoring the np.nan
for healthThr in [0.0001, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]:
    healthCounter = 0
    for name, value in idSessionTypeExcludeNan.iteritems():
        if value >= healthThr:
            healthCounter += 1
    hqueries = 0
    for item in dataset[["AnonID","SessionNum"]].values:
         hqueries += int( idSessionTypeExcludeNan[tuple([item[0],item[1]])] > healthThr )
    print healthThr, healthCounter, healthCounter/ len(idSessionType), hqueries, hqueries/ totalQueries

"""
Using np.nan
print healthThr, Sessions, %Sessions, Queries, %Queries
0.0001 242118 0.0210993283575 1931506 0.0530785650733
0.1 220352 0.0192025343107 1117018 0.0306961058371
0.2 181786 0.0158417073691 672288 0.0184747457973
0.3 139138 0.0121251552921 449486 0.0123520568409
0.4 107700 0.00938549659299 265977 0.00730915539611
0.5 99013 0.00862846958367 131265 0.00360721522188
0.6 59890 0.00521910298007 108158 0.00297222552827
0.7 49305 0.00429667511158 76844 0.0021117041596
0.8 46731 0.00407236435735 61603 0.0016928753233
0.9 45695 0.00398208232884 59107 0.00162428423509
1.0 45680 0.00398077515662 0 0.0
After modifications, and before changing sessions to 30 min:
0.0001 260462 0.0226979128468 2042604 0.0561315829892
0.1 239129 0.020838852505 1230432 0.0338127683685
0.2 201572 0.0175659546819 793642 0.0218096027359
0.3 158607 0.0138217776985 563279 0.0154791344453
0.4 125290 0.0109183738917 353162 0.00970503441275
0.5 114632 0.00998958445169 186130 0.0051149275835
0.6 71087 0.00619486347545 146616 0.00402906690261
0.7 57033 0.00497013024315 102076 0.00280508971156
0.8 52924 0.00461205219765 75720 0.00208081618558
0.9 51010 0.00444525702143 70726 0.00194357904836
1.0 50988 0.0044433398355 0 0.0

Ignoring np.nan
print healthThr, Sessions, %Sessions, Queries, %Queries
0.0001 242118 0.0210993283575 1931506 0.0530785650733
0.1 239687 0.0208874793118 1532254 0.0421069588435
0.2 230734 0.0201072717817 1306771 0.0359105949241
0.3 213627 0.0186164854287 1156900 0.0317920793067
0.4 194281 0.0169305818346 944229 0.0259477943225
0.5 189265 0.0164934634417 664329 0.0182560292625
0.6 147274 0.0128341655082 606002 0.0166531797424
0.7 131940 0.0114978869125 500444 0.0137524032644
0.8 126504 0.0110241676973 428422 0.0117732096125
0.9 123201 0.0107363283728 406202 0.0111625950372
1.0 123109 0.0107283110498 0 0.0
CPU times: user 29min 12s, sys: 13.1 s, total: 29min 25s
Wall time: 29min 28s


---Sessions of 30 minutes:
Using np.nan
0.0001 253277 0.0235256122254 2171707 0.0596793855777
0.1 228758 0.0212481670323 1261767 0.0346738668256
0.2 189430 0.0175951891559 791436 0.0217489809648
0.3 146820 0.0136373629936 549004 0.0150868516792
0.4 114827 0.0106656959574 337821 0.00928345753606
0.5 104227 0.00968111587477 174583 0.00479761135932
0.6 63952 0.00594017598533 134539 0.00369718606435
0.7 50957 0.00473313653497 92467 0.0025410305102
0.8 47150 0.00437952366944 67754 0.00186190728788
0.9 45378 0.0042149316028 63061 0.00173294175223
1.0 45353 0.00421260947996 0 0.0
Ignoring np.nan
0.0001 253277 0.0235256122254 2171707 0.0596793855777
0.1 248608 0.0230919325644 1688893 0.0464114618347
0.2 235157 0.0218425375935 1384844 0.0380560725001
0.3 213018 0.0197861584945 1192169 0.0327612856729
0.4 190086 0.0176561216592 943967 0.0259405944566
0.5 182924 0.016990879909 652045 0.0179184599806
0.6 138133 0.0128304717504 574055 0.0157752632781
0.7 119713 0.0111195316445 453390 0.0124593403379
0.8 112456 0.0104454658276 365193 0.0100356511524
0.9 107881 0.0100205173485 336729 0.00925344893497
1.0 107720 0.0100055628775 0 0.0

"""

pickle.dump(idSessionTypeExcludeNan, open("idSessionTypeExcludeNan","wb"))
pickle.dump(idSessionType, open("idSessionType","wb"))

def filterForHealthContent(row, healthThr = 0.5):
    if idSessionTypeExcludeNan[tuple([row["AnonID"], row["SessionNum"]])] > healthThr:
        return 1
    return 0
dataset["health"] = dataset.apply(filterForHealthContent, axis=1)
#TODO: test time:
dataset["health"] = dataset[["AnonID","SessionNum"]].apply(filterForHealthContent, axis=1)


"""Merging code"""

#Write health queries:
aolNHv5 = pd.read_csv("../../aolNotHealth/aolNotHealthNoAnimal-noDash.v5.dataset.gz", names=["QueryTime", "AnonID", "Query", "previousQuery", "mesh", "semantic", "num", "b1", "b2", "b3", "v1" ], compression="gzip")

aolHv5 = pd.read_csv("../../aolh/aolh.gz", names=["QueryTime", "AnonID", "Query", "previousQuery", "mesh", "semantic", "num", "b1", "b2", "b3", "v1","ig1","ig2","ig3","ig4"], compression="gzip")

h2 = aolHv5[["QueryTime", "AnonID", "Query", "previousQuery", "mesh", "semantic", "num", "b1", "b2", "b3", "v1"]]
hconcat = pd.concat([h2,aolNHv5], ignore_index=True)
hconcat["QueryTime"] = pd.to_datetime(hconcat.QueryTime)

h1g = hconcat.groupby(["QueryTime","AnonID","Query"]).first()
h1g = h1g.reset_index()
h1g.to_pickle("h1g.pickle")

merged = pd.merge(dataset, h1g, how="left", on=["QueryTime","AnonID","Query"])
merged.to_pickle("merged.pickle")

merged["num"] = merged["num"].fillna(0).astype(np.int32)
merged["v1"] = merged["v1"].fillna(0.0).astype(np.float64)
mhealth = merged[merged.health == 1]
mhealth.to_csv("healthq", columns=["QueryTime","AnonID","Query","previousQuery","mesh", "semantic", "num", "b1", "b2", "b3", "v1"], index=False, header=False)

mnhealth = merged[merged.health == 0]
mnhealth.to_csv("nhealthq", columns=["QueryTime","AnonID","Query","previousQuery","mesh", "semantic", "num", "b1", "b2", "b3", "v1"], index=False, header=False)
#TODO: remember to run removeDash after all of it

#pd.merge(aolNHv5, aolHv5, on=" )
#["QueryTime", "AnonID", "Query", "previousQuery", "mesh", "semantic", "num""b1", "b2", "b3", "v1" ]

#*** TODO ****
# * merge aolv5 with dataset on all the relevant fields
# *
# *


