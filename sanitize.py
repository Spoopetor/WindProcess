import sys
import os
import multiprocessing
from itertools import repeat

def sanitize(inpath, outpath):
    

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
        supersample(inpath, outpath, 2)
    else:

        """fullpaths = [inpath+f"\\{x}\\" for x in os.listdir(inpath)]

        with multiprocessing.Pool(12) as pool:
            pool.starmap(stitch, zip(fullpaths, repeat(outpath)))"""
                

if __name__ == "__main__":
    main()