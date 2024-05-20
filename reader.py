import json
import matplotlib as mpl
import glob


with open('O:\\KGFL-JSONS-stitched\\stitched_KGFL_01.2018-06.2022.json', 'r') as f:
    data = json.load(f)

sortedKeys = list(data.keys())
sortedKeys.sort()

with open('ordered.txt', 'w') as f:
    for k in sortedKeys:
        f.write(f"{k}: {data[k]}\n")




