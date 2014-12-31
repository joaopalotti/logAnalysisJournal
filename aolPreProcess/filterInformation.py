import csv, gzip, ast, sys
from datetime import datetime

filename = sys.argv[1]
outputFileName = sys.argv[2]
reversedFilter=False
clickedInfoOnly=True

def readAOLdataSet(filename, filterSites, writer):
    data = []
    
    if filename.endswith(".gz"):
        csvfile = gzip.open(filename, 'rb')
    else:
        csvfile = open(filename, 'rb')

    reader = csv.reader(csvfile, delimiter='\t', quotechar='|')
    
    # ignore first line
    headerline = reader.next()

    for row in reader:
        found = False
        if row and row[4] and row[4] in filterSites:
            found = True
            #print "FOUND!"
        
        if reversedFilter == False and found == True:
            writer.writerow(row)
            #print "Found and saved"


        elif reversedFilter == True and found == False:
            if clickedInfoOnly and row[4]:
                #print "writing row", row
                writer.writerow(row)
            elif not clickedInfoOnly:
                writer.writerow(row)

    csvfile.close()
    return data


def loadFilterInformationFromDmoz(filename):
    
    if filename.endswith(".gz"):
        f = gzip.open(filename, 'rb')
    else:
        f = open(filename, 'rb')

    webSites = set()
    for line in f.readlines():
        url = ast.literal_eval(line)["url"]
        if url.endswith("/"):
            webSites.add(url[:-1])
        webSites.add(url)
    
    f.close()
    return webSites


if __name__ == "__main__":

    listOfFilteredSites = loadFilterInformationFromDmoz(filename)
    
    csvfile = open(outputFileName, "w")
    writer = csv.writer(csvfile, delimiter='\t', quotechar='|', quoting=csv.QUOTE_MINIMAL)

    for i in range(1,10):
        readAOLdataSet("user-ct-test-collection-0"+ str(i) + ".txt.gz", listOfFilteredSites, writer)
    
    readAOLdataSet("user-ct-test-collection-10.txt.gz", listOfFilteredSites, writer)
    csvfile.close()

