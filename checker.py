import glob
import os

def sizeof(filename):
    tag = filename.split("\\")[-1].split(".")[0]
    size = os.path.getsize(filename)
    return (tag, size);

jsonfiles = glob.glob("O:\\JSONS-raw\\**\\*.json", recursive=True)
rawfiles = glob.glob("O:\\ASOS\\SEEDdata\\*.dat")

jsonsizes = {k:v for (k, v) in list(map(sizeof, jsonfiles))}
rawsizes = {k:v for (k, v) in list(map(sizeof, rawfiles))}

diff = []
for key in jsonsizes.keys():
    if jsonsizes[key]/rawsizes[key] < .148:
        diff.append(str((key, jsonsizes[key]/rawsizes[key]))+"\n")

with open("diff.txt", "w") as f:
    f.writelines(diff)