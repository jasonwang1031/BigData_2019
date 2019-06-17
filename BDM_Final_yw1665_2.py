def createIndex(shapefile):
    #create rtree                                                                           
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        #There is broken geometry in the geojson file                                       
        try:
            if zones.geometry[idx].contains(p):
                return True
        except:
            pass
    return False

def wordcheck(tweets, drugTerm):
    #check if there any drug related term in the tweets                                     
    #use package regular expression to match both individual word and phrases                                                         
    import re
    words_re = re.compile(r"\b" + r"\b|\b".join(drugTerm)+r"\b")
    if words_re.search(tweets):
        return True
    return False

def generateFreq(pid, records):
    import csv
    reader = csv.reader(records, delimiter='|')
    for row in reader:
        if len(row) == 7:
            for word in filter(None, row[6].split(' ')):
                yield (word.lower(), 1)

def tweetsFilter(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    import fiona.crs
    import geopandas as gpd

    with open('drug_sched2.txt') as f:
        term1 = f.read().splitlines()
    with open('drug_illegal.txt') as f:
        term2 = f.read().splitlines()
    drugTerm = term1 + term2

    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)
    index, zones = createIndex('500cities_tracts.geojson')

    reader = csv.reader(records, delimiter='|')
    for row in reader:
        if len(row) == 7:
            try:
                p = geom.Point(proj(float(row[2]), float(row[1])))
                if findZone(p, index, zones):
                    if wordcheck(row[5].lower(), drugTerm):
                        yield(row[6], 'f')
            except:
                continue

def findTopWord(rows):
    freq_dict = dict(freq)

    for row in rows:
        word_list = []
        for i in filter(None, row[0].split(' ')):
            word_list.append((freq_dict.get(i), i))
        top = sorted(word_list)[:3]
        for (value,key) in top:
            yield(key,1)

def processFormat(rows):
    for row in rows:
        yield (row[1], row[0])

if __name__ == '__main__':
    from pyspark import SparkContext
    sc = SparkContext()
    rdd = sc.textFile('/tmp/bdm/tweets-100m.csv')

    freq = rdd.mapPartitionsWithIndex(generateFreq)\
            .reduceByKey(lambda x,y: x+y)\
            .collect()

    drugTweets = rdd.mapPartitionsWithIndex(tweetsFilter)\
                 .mapPartitions(findTopWord)\
                 .reduceByKey(lambda x,y: x+y)\
                 .mapPartitions(processFormat)\
                 .sortByKey(ascending = False).take(100)
    print(drugTweets)


#for my code, I firstly generate a list of tuple to store the frequency of each word        
#then I filtered out tweets do not in the 500 cities and do not contain drug-related words.
#I used the second to last column to check whether the drug-related term is in it, but I    
#used the last column to calculated the frequency and also to generate the top 3 word.      
#because the last column has cleaned tweet message, which do not have hashtag, @, and lots of
#meaningless words.                                                                         
#eventually, I sorted all the top 3 words in each tweet, then obtain the top 100            




