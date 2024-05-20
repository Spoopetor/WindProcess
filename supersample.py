import numpy as np
import scipy as sp
import json
import math
import sys
import os
import multiprocessing
from itertools import repeat
from tqdm import tqdm
import glob

def parseData(a):
    #return [int(a[0]) if a[0] != "null" else np.nan, int(a[1]) if a[1] != "null" else np.nan, int(a[2]) if a[2] != "null" else np.nan,int(a[3]) if a[3] != "null" else np.nan]
    return [float(a[0]) if a[0] != "null" else np.nan, float(a[1]) if a[1] != "null" else np.nan]

def getAvgHourly(inpath, outpath):

    srate = 30
    with open(inpath, 'r') as f:
        data = json.load(f)
    
    data = {int(k): parseData(v) for (k,v) in data.items()}

    sortedKeys = list(data.keys())
    sortedKeys.sort()

    tStart = int(sortedKeys[0])
    tEnd = int(sortedKeys[-1])

    aLen = int(math.floor((tEnd-tStart) / 120) + 1)

    fullTimes = [tStart + (i*120) for i in range(aLen)]
    
    t1 = set(fullTimes)
    t2 = set(sortedKeys)

    missingTimes = t1 - t2
    allData = data

    for t in missingTimes:
        if t not in t2:
            #allData.update({t: [np.nan, np.nan, np.nan, np.nan]})
            allData.update({t: [np.nan, np.nan]})

    times = np.array(list(allData.keys()))
    sortedIndices = times.argsort()
    
    timesIO = np.array(list(allData.keys()), dtype=float)[sortedIndices]
    dataIO = np.array(list(allData.values()))[sortedIndices]

    padLen = 0
    while (timesIO.size + padLen) % srate != 0:
        padLen+=1

    timesIO = np.pad(timesIO, (0, padLen), mode='constant', constant_values=(0, np.nan))

    timesIO = timesIO.reshape(timesIO.size//srate, srate)
    
    wd = dataIO[:, 0]
    ws = dataIO[:, 1]
    

    wd = np.radians(wd)
    

    wd = np.pad(wd, (0, padLen), mode='constant', constant_values=(0, np.nan))
    ws = np.pad(ws, (0, padLen), mode='constant', constant_values=(0, np.nan))
    
    
    wd = wd.reshape((wd.size//srate, srate))
    ws = ws.reshape((ws.size//srate, srate))
    
    
    wd = sp.stats.circmean(wd, axis=1, nan_policy='omit')
    ws = np.nanmean(ws, axis=1)
    

    data = np.vstack((wd, ws)).transpose()

    outDict = {k: list(v) for (k, v) in zip(timesIO[:,0], data[:])}

    filename = inpath.split("\\")[-1]

    with open(outpath + f"\\hourlyMeanWind-{filename}", "w") as b:
        json.dump(outDict, b)

def supersample(inpath, outpath, srate):
    with open(inpath, 'r') as f:
        data = json.load(f)
    
    data = {int(k): parseData(v) for (k,v) in data.items()}

    sortedKeys = list(data.keys())
    sortedKeys.sort()

    tStart = int(sortedKeys[0])
    tEnd = int(sortedKeys[-1])

    aLen = int(math.floor((tEnd-tStart) / 120) + 1)

    fullTimes = [tStart + (i*120) for i in range(aLen)]
    
    t1 = set(fullTimes)
    t2 = set(sortedKeys)

    missingTimes = t1 - t2
    allData = data

    for t in missingTimes:
        if t not in t2:
            #allData.update({t: [np.nan, np.nan, np.nan, np.nan]})
            allData.update({t: [np.nan, np.nan]})

    times = np.array(list(allData.keys()))
    sortedIndices = times.argsort()
    
    timesIO = np.array(list(allData.keys()), dtype=float)[sortedIndices]
    dataIO = np.array(list(allData.values()))[sortedIndices]

    padLen = 0
    while (timesIO.size + padLen) % 30 != 0:
        padLen+=1

    timesIO = np.pad(timesIO, (0, padLen), mode='constant', constant_values=(0, np.nan))

    #timesIO = timesIO.reshape(timesIO.size//srate, srate)
    timesIO = timesIO.reshape(timesIO.size//30, 30)
    
    wd = dataIO[:, 0]
    ws = dataIO[:, 1]
    #gd = dataIO[:, 2]
    #gs = dataIO[:, 3]

    wd = np.radians(wd)
    #gd = np.radians(gd)

    wd = np.pad(wd, (0, padLen), mode='constant', constant_values=(0, np.nan))
    ws = np.pad(ws, (0, padLen), mode='constant', constant_values=(0, np.nan))
    #gd = np.pad(gd, (0, padLen), mode='constant', constant_values=(0, np.nan))
    #gs = np.pad(gs, (0, padLen), mode='constant', constant_values=(0, np.nan))
    
    wd = wd.reshape((wd.size//srate, srate))
    ws = ws.reshape((ws.size//srate, srate))
    #gd = gd.reshape((gd.size//srate, srate))
    #gs = gs.reshape((gs.size//srate, srate))
    wd = sp.stats.circmean(wd, axis=1, nan_policy='omit')
    ws = np.nanmean(ws, axis=1)
    
    wd = wd.reshape((wd.size//(30//srate), (30//srate)))
    ws = ws.reshape((ws.size//(30//srate), (30//srate)))
    
    ws = np.nan_to_num(ws, nan=-9999999)
    wi = np.nanargmax(ws, axis=1)
    ws = np.where(ws==-9999999, np.nan, ws)
    
    wsOut = []
    wdOut = []
    for i in range(len(wi)):
        wsOut.append(ws[i, wi[i]])
        wdOut.append(wd[i, wi[i]])
    
    #gd = sp.stats.circmean(gd, axis=1, nan_policy='omit')
    #gs = np.nanmean(gs, axis=1)
    wsOut = np.array(wsOut)
    wdOut = np.array(wdOut)
    
    #data = np.vstack((wd, ws, gd, gs)).transpose()
    data = np.vstack((wdOut, wsOut)).transpose()
    

    outDict = {k: list(v) for (k, v) in zip(timesIO[:,0], data[:])}

    filename = inpath.split("\\")[-1]

    with open(outpath + f"\\{2*srate}_min-{filename}", "w") as b:
        json.dump(outDict, b)
        
def processData(inpath, outpath):
    getAvgHourly(inpath, outpath)
    splits = [1, 2, 3, 5, 6, 10, 15, 30]
    for s in splits:
        supersample(inpath, outpath, s)

def multiprocessData(inpath, outpath):
    station = os.path.basename(inpath).split("_")[1]
    fullOut = f"{outpath}\\{station}\\"
    if not os.path.exists(fullOut):
        os.makedirs(fullOut)
    if len(glob.glob(fullOut+"60_min*")) < 1:
        try:
            processData(inpath, fullOut)
        except:
            with open(f"O:\\FINAL-supersampled\\{station}.failed", "w") as f:
                f.write(inpath)
    


def main():
    if len(sys.argv) != 4:
        print("Usage: <input directory path> <output directory path> <-s (single file) / -m (multi)>")

    inpath = sys.argv[1]
    outpath = sys.argv[2]
    flag = sys.argv[3]

    if(not os.path.exists(inpath)):
        print("Invalid Input Path!")
        sys.exit(0)

    if(not os.path.exists(outpath)):
        print("Invalid Output Path!")
        sys.exit(0)

    if(flag not in ["-s", "-m"]):
        print("Invalid Flag!")
        sys.exit(0)

    if flag == "-s":
        processData(inpath, outpath)
            
    else:

        fullpaths = glob.glob(inpath+"\\*.json")
        
        with multiprocessing.Pool(12) as pool:
            pool.starmap(multiprocessData, tqdm(zip(fullpaths, repeat(outpath)), total=len(fullpaths)))
        
                

if __name__ == "__main__":
    main()