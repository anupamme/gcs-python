import sys
import json

def addOrInsert(arr, key, val):
    if key not in arr:
        arr[key] = {}
    for minKey in val:
        arr[key][minKey] = val[minKey]
    return arr

if __name__ == "__main__":
    count = 1
    output = {}
    while count < 4:
        inputFile = sys.argv[count]
        fr = open(inputFile, 'r')
        frText = fr.read()
        inputData = frText.split('\n')
        for line in inputData:
            sub = line.split('\t')
            if len(sub) < 2:
                continue
            print ('length: ' + str(len(sub)))
            locKey = sub[0]
            print ('locKey: ' + locKey)
            data = sub[1]
            obj = json.loads(data)
            output = addOrInsert(output, locKey, obj)
    f = open('job4-format.json', 'w')
    f.write(json.dumps(output))
    f.close()