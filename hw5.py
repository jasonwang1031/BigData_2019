def createIndex(shapefile):
    #develop r tree                                                                                                                         
    import rtree
    import fiona.crs
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)

def findZone(p, index, zones):
    #return the index of zones for each row                                                                                                 
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return str(idx)
    return None

def processTrips(pid, records):
    #map each pickup and dropoff of each trip to neighborhood and boro                                                                      
    #only use one geojson file because it's faster                                                                                          
    import csv
    import pyproj
    import shapely.geometry as geom
    import fiona.crs
    import geopandas as gpd

    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)

    nh_index, nh_zones = createIndex('neighborhoods.geojson')
    nh_shape = gpd.read_file('neighborhoods.geojson').to_crs(fiona.crs.from_epsg(2263))

    reader = csv.reader(records)
    for row in reader:
        try:
            dropoff = geom.Point(proj(float(row[9]), float(row[10])))
            pickup = geom.Point(proj(float(row[5]), float(row[6])))
        except:
            continue

        boro = findZone(dropoff, nh_index, nh_zones)
        neighborhoods = findZone(pickup, nh_index, nh_zones)

        if boro and neighborhoods:
            yield ((nh_shape['borough'][int(boro)], nh_shape['neighborhood'][int(neighborhoods)]), 1)

def processNeighborhoods1(rows):
    #select rides ended at Staten Island                                                                                                    
    for row in rows:
        boro, neighborhoods = row[0]
        if boro == 'Staten Island':
            yield(row[1], (neighborhoods, boro))

def processNeighborhoods2(rows):
    #select rides ended at Brooklyn                                                                                                         
    for row in rows:
        boro, neighborhoods = row[0]
        if boro =='Brooklyn':
            yield(row[1], (neighborhoods, boro))

def processNeighborhoods3(rows):
    #select rides ended at Mahattan                                                                                                         
    for row in rows:
        boro, neighborhoods = row[0]
        if boro =='Manhattan':
            yield(row[1], (neighborhoods, boro))

def processNeighborhoods4(rows):
    #select rides ended at Queens                                                                                                           
    for row in rows:
        boro, neighborhoods = row[0]
        if boro =='Queens':
            yield(row[1], (neighborhoods, boro))

def processNeighborhoods5(rows):
    #select rides ended at Bronx                                                                                                            
    for row in rows:
        boro, neighborhoods = row[0]
        if boro =='Bronx':
            yield(row[1], (neighborhoods, boro))

def processFormat(rows):
#editing the format of the output                                                                                                           
    for row in rows:
        neighborhoods, boro = row[1]
        yield (boro, [neighborhoods])

if __name__ == '__main__':
    from pyspark import SparkContext
    sc = SparkContext()
    rdd = sc.textFile('/tmp/bdm/yellow_tripdata_2011-05.csv')
    trips = rdd.mapPartitionsWithIndex(processTrips)\
    .reduceByKey(lambda x,y: x+y)
    n1 = trips.mapPartitions(processNeighborhoods1)\
        .sortByKey(ascending = False).take(3)
    n2 = trips.mapPartitions(processNeighborhoods2)\
        .sortByKey(ascending = False).take(3)
    n3 = trips.mapPartitions(processNeighborhoods3)\
        .sortByKey(ascending = False).take(3)
    n4 = trips.mapPartitions(processNeighborhoods4)\
        .sortByKey(ascending = False).take(3)
    n5 = trips.mapPartitions(processNeighborhoods5)\
        .sortByKey(ascending = False).take(3)
    final = sc.parallelize(n1+n2+n3+n4+n5)\
        .mapPartitions(processFormat)\
        .reduceByKey(lambda x,y: list(x) + list(y))\
        .collect()
    print(final)
    #sc.parallelize([final]).saveAsTextFile('hw5_ouput')

