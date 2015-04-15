def addOrInsert(res, key):
    if key in res:
        res[key] = res[key] + 1
    else:
        res[key] = 1
    return res

def areEqual(dic1, dic2):
    for key in dic1:
        if key not in dic2:
            return False
        if dic1[key] != dic2[key]:
            return False
    for key in dic2:
        if key not in dic1:
            return False;
    return True


if __name__ == "__main__":
    numtestcases = int(raw_input())
    count = 0
    testcasesArr = []
    while count < numtestcases:
        testcasesArr.append(raw_input())
        count += 1
    for testcasesStr in testcasesArr:
        testcases = testcasesStr.split(' ')
        prev = {}
        current = {}
        count = 0
        isYes = True
        for test in testcases:
            if count == 0:
                for ch in test:
                    prev = addOrInsert(prev, ch)
            else:
                for ch in test:
                    next = addOrInsert(current, ch)
                if not areEqual(prev, current):
                    print 'NO'
                    isYes = False
            count += 1
        if isYes:
            print 'YES'