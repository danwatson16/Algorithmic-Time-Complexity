def mapper_parallel(document):
    output=[]
    for word,freq in document.items():
        output.append((word,freq))
    return output


def reducer_parallel(item):
    (word,counts)=item
    intersection = 0
    if len(counts)==2:
        intersection += min(counts)
        
    return [intersection]

def jaccard_mapper_parallel(document):
    output=[]
    for i in range(len(document)):
        for j in range(len(document)):
            if (i!=j):
                js = jaccard(document[i],document[j])
                output.append((js,(i,j)))
    
    return output

def jaccard_reducer_parallel(item):
    output=[]
    (js, counts)=item
    output.append((js, counts[0]))
    return output


from collections import defaultdict
import operator
from multiprocessing import Pool

def map_reduce_parallel(inputs,mapper,reducer,mapprocesses=1,reduceprocesses=1):
    
    collector=defaultdict(list)  #this dictionary is where we will store intermediate results
                                 #it will map keys to lists of values (default value of a list is [])
                                 #in a real system, this would be stored in individual files at the map nodes
                                 #and then transferred to the reduce nodes
    mappool = Pool(processes=mapprocesses)
    #map stage

    mapresults=mappool.map(mapper,inputs)
    mappool.close()
    print(mapresults)
    for mapresult in mapresults:
        for (key, value) in mapresult:     #pass each input to the mapper function and receive back each key,value pair yielded
            collector[key].append(value)     #append the value to the list for that key in the intermediate store
    
    
    #reduce stage - 1 reducer for each key
    outputs=[]
    reducepool = Pool(processes=reduceprocesses)
    
    reduceresults=reducepool.map(reducer,collector.items())
    
    reducepool.close()
    for reduceresult in reduceresults:
        outputs+=reduceresult
    
    return outputs


def wc_mapper_parallel(document):
    output=[]
    for word in document:
        output.append((word,1))
        
    return output

def wc_reducer_parallel(item):
    output=[]
    (word,counts)=item
    output.append((word,sum(counts)))
    return output

def mapreduce(document,mp=3,rp=2):
    
    items=[]
    
    for i in range(len(document)):
            for j in range(len(document)):
                    js = jaccard(document[i],document[j])
                    items.append((js, (i,j)))
    return map_reduce_parallel(items,wc_mapper_parallel,wc_reducer_parallel,mapprocesses=mp,reduceprocesses=rp)


    



def cosine_dict(dict1,dict2):
    intersection = 0
    a = 0 #Initializing variables at 0 to keep a running score.
    b = 0
    for key,value in dict1.items():
        intersection += value*dict2.get(key,0.0) #Search for each individual key. If it finds it, returns the frequency
        #of the word. If not, assume 0.
        a += value*value
    for value in dict2.values():
        b += value*value
    return intersection/math.sqrt(a*b)


                    
                    
def maketotal(dict1):
    total=0
    for item in dict1:
        total += dict1[item]
    return total

def jaccard(dict1,dict2):
    intersection={}
    for item in dict1.keys(): #Loop over all keys in the document, increasing time complexity. 
        if item in dict2.keys():
            intersection[item]=min(dict1[item],dict2[item])
            
    intersectiontot=maketotal(intersection) #Finding the total number of intersected words and their frequencies.
    union = maketotal(dict1)+maketotal(dict2)-intersectiontot
    return intersectiontot/union

import time
def timeit(somefunc,*args,repeats=1000,**kwargs):
    times=[]
  
    while repeats>0:
        starttime=time.time()
        ans=somefunc(*args,**kwargs)
        endtime=time.time()
        timetaken=endtime-starttime
        times.append(timetaken)
        repeats-=1
    
    mean=np.mean(times)
    stdev=np.std(times)
    error=stdev/(len(times)**0.5)
 
    return ans, mean, error
