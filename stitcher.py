import json
import glob
import os
import sys
import multiprocessing
from itertools import repeat


def stitch(inpath, outpath):

    jsonpaths = glob.glob(inpath+"\\*.json")
    jsonpaths.sort()

    if len(jsonpaths) == 0:
        print("No .json files in Directory!")
        sys.exit(0)

    outputjson = {}

    for j in jsonpaths:
        with open(j, 'r') as f:
            data = json.load(f)
        outputjson.update(data)

    stationName = jsonpaths[0].split("\\")[-1][5:9]
    startDate = jsonpaths[0].split(".")[0][-6:]
    endDate = jsonpaths[-1].split(".")[0][-6:]

    with open(outpath + f"\\stitched_{stationName}_{startDate[4:6]}.{startDate[0:4]}-{endDate[4:6]}.{endDate[0:4]}.json", "w") as b:
        json.dump(outputjson, b)

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
        stitch(inpath, outpath)
    else:

        fullpaths = [inpath+f"\\{x}\\" for x in os.listdir(inpath)]

        with multiprocessing.Pool(12) as pool:
            pool.starmap(stitch, zip(fullpaths, repeat(outpath)))
                

if __name__ == "__main__":
    main()

