'''input is in following format:
PARIS:FRANCE	{"g187147-d4800782":[
    [
        {"sentiment":"Negative","worddetails":[{"pos":"PRP","word":"I"},{"pos":"VBP","word":"have"},{"pos":"VBN","word":"visited"},{"pos":"NNP","word":"Paris"},{"pos":"RB","word":"just"},{"pos":"IN","word":"for"},{"pos":"DT","word":"a"},{"pos":"JJ","word":"short"},{"pos":"NN","word":"weekend"},{"pos":"NN","word":"break"},{"pos":"IN","word":"before"},{"pos":"NNP","word":"Xmas"},{"pos":"CD","word":"2014"},{"pos":".","word":"."}]},
        {"sentiment":"Negative","worddetails":[{"pos":"NN","word":"Hotel"},{"pos":"IN","word":"as"},{"pos":"JJ","word":"such"},{"pos":"VBZ","word":"is"},{"pos":"VBN","word":"located"},{"pos":"IN","word":"on"},{"pos":"DT","word":"a"},{"pos":"JJ","word":"perfect"},{"pos":"NN","word":"place"},{"pos":":","word":"-"},{"pos":"VBG","word":"walking"},{"pos":"NN","word":"distance"},{"pos":"TO","word":"to"},{"pos":"NNP","word":"Montmartre"},{"pos":",","word":","},{"pos":"NNP","word":"Champs"},{"pos":"NNP","word":"Elysees"},{"pos":",","word":","},{"pos":"NNP","word":"Eiffel"},{"pos":"NN","word":"tower"},{"pos":":","word":"..."},{"pos":"CC","word":"And"},{"pos":"RB","word":"easily"},{"pos":"JJ","word":"accessible"},{"pos":"IN","word":"from"},{"pos":"NNP","word":"Orly"},{"pos":"NN","word":"airport"},{"pos":"-LRB-","word":"-LRB-"},{"pos":"NN","word":"taxi"},{"pos":"RB","word":"approx"},{"pos":".","word":"."}]}
    ],
    [
    {"sentiment":"Negative","worddetails":[{"pos":"PRP","word":"I"},{"pos":"VBD","word":"chose"},{"pos":"TO","word":"to"},{"pos":"NN","word":"book"},{"pos":"DT","word":"this"},{"pos":"NN","word":"hotel"},{"pos":"VBN","word":"based"},{"pos":"IN","word":"on"},{"pos":"DT","word":"the"},{"pos":"NNP","word":"Trip"},{"pos":"NNP","word":"Advisor"},{"pos":"NNS","word":"rankings"},{"pos":"CC","word":"and"},{"pos":"VBP","word":"am"},{"pos":"JJ","word":"pleased"},{"pos":"TO","word":"to"},{"pos":"VB","word":"say"},{"pos":"IN","word":"that"},{"pos":"PRP","word":"it"},{"pos":"VBZ","word":"lives"},{"pos":"RP","word":"up"},{"pos":"TO","word":"to"},{"pos":"DT","word":"the"},{"pos":"NN","word":"ranking"},{"pos":".","word":"."}]},
    {"sentiment":"Negative","worddetails":[{"pos":"PRP","word":"I"},{"pos":"RB","word":"recently"},{"pos":"VBD","word":"traveled"},{"pos":"IN","word":"on"},{"pos":"NN","word":"business"},{"pos":"TO","word":"to"},{"pos":"NNP","word":"Paris"},{"pos":"CC","word":"and"},{"pos":"VBD","word":"was"},{"pos":"VBG","word":"looking"},{"pos":"IN","word":"for"},{"pos":"DT","word":"a"},{"pos":"NN","word":"hotel"},{"pos":"WDT","word":"that"},{"pos":"MD","word":"would"},{"pos":"VB","word":"fit"},{"pos":"IN","word":"into"},{"pos":"PRP$","word":"my"},{"pos":"NN","word":"budget"},{"pos":"CC","word":"but"},{"pos":"MD","word":"would"},{"pos":"RB","word":"also"},{"pos":"VB","word":"be"},{"pos":"JJ","word":"easy"},{"pos":"TO","word":"to"},{"pos":"VB","word":"use"},{"pos":"IN","word":"as"},{"pos":"DT","word":"a"},{"pos":"NN","word":"base"},{"pos":"IN","word":"for"},{"pos":"PRP$","word":"my"},{"pos":"JJ","word":"short"},{"pos":"NN","word":"trip"},{"pos":".","word":"."}]}
    ]
]}

'''

import json
import logging
import math
import sys
import os


inputFileName = 'job3-in.json'
attributeCloud = 'attributeCloud.json'
outputDir = 'out-job4'
attributeMapForward = {}
attributeMapBackward = {}
hotelSentimentMap = {}
sentimentMap = {}

def initialize(attrPath):
    #1. build attribute map: back and forth.
    global attributeMapForward
    global attributeMapBackward
    lines = json.loads(open(attrPath, 'r').read())
    for attr in lines:
        tokens = lines[attr]
        count = 0
        while count < len(tokens):
            tok = normalize(tokens[count])
            tok_zero = normalize(tokens[0])
            # forward map
            if count == 0:
                attributeMapForward[tok] = [tok]
            else:
                attributeMapForward[tok_zero].append(tok)
            attributeMapBackward[tok] = tok_zero
            count += 1

def normalize(val):
    return val.lower().strip()

def findAttr(tokens):           # tokens is [{}]
    if type(tokens) == list:
        for detail in tokens:
            if detail['pos'] == 'NN' or detail['pos'] == 'NNP':
                word = detail['word']
                print 'pos tag found : ' + word.encode('utf-8')
                prospect = attributeMapBackward.get(normalize(word))
                if prospect != None:
                    return prospect
    return None

def findNumericalSentimentValue(sentiment):
    if 'Very negative' == sentiment:
        return 0
    if 'Negative' ==  sentiment:
        return 1
    if 'Neutral' == sentiment:
        return 2
    if 'Positive' == sentiment:
        return 3
    if 'Very positive' == sentiment:
        return 4
    raise Exception('Illegal argument: ' + sentiment) 

def buildSentimentWeightedMap(json_ob, hotelid, reviewid):
    sentiWMap = {}
    for sentence in json_ob:            # json_ob is [{}]
        sentiment = findNumericalSentimentValue(sentence['sentiment'])
        worddetails = sentence['worddetails']
        attr = findAttr(worddetails)
        if attr == None:
            print 'error: ' + 'Attribute not found for ' + str(hotelid) + ' : ' + str(reviewid) + ' : '+ json.dumps(worddetails)
        else:
            current_val = sentiWMap.get(attr)
            if current_val == None:
                sentiWMap[attr] = {'a': sentiment, 'c': 1}
            else:
                val  = current_val['a']
                num = current_val['c']
                sentiWMap[attr] = {'a': int(val) + int(sentiment), 'c': int(num) + 1}
    return sentiWMap

def buildSentimentMap(loc, json_ob, hotelid, reviewid):              # json_ob is [{}]
    # 1. build sentimentMap
    global hotelSentimentMap
    global sentimentMap
    sentiWMap = buildSentimentWeightedMap(json_ob, hotelid, reviewid)
    sentiMap = {}
    #unify the map.
    for val in sentiWMap:
        key = val
        value = sentiWMap[val]
        num = value['a']
        count = value['c']
        sentiMap[key] = math.ceil(int(num) / int(count))
    for val in sentiMap:
        att = val
        sen = sentiMap[val]
        metaVal = sentimentMap[loc].get(str((att, sen)))
        if metaVal == None:
            sentimentMap[loc][str((att, sen))] = {}
            sentimentMap[loc][str((att,sen))][hotelid] = [reviewid]
        else:
            if hotelid in sentimentMap[loc][str((att, sen))]:
                sentimentMap[loc][str((att, sen))][hotelid].append(reviewid)
            else:
                sentimentMap[loc][str((att, sen))][hotelid] = [reviewid]
        
        if hotelid in hotelSentimentMap[loc]:
            if att in hotelSentimentMap[loc][hotelid]:
                hotelSentimentMap[loc][hotelid][att].append(sen)
            else:
                hotelSentimentMap[loc][hotelid][att] = [sen]
        else:
            hotelSentimentMap[loc][hotelid] = {}
            hotelSentimentMap[loc][hotelid][att] = [sen]
        
def storeMapInFileSystem(outputDir):
    f = open(outputDir + 'sentiment.json', 'w')
    print sentimentMap
    strTo = json.dumps(sentimentMap)
    print('strTo: '+ strTo)
    f.write(strTo)
    f.close()
    
    f = open(outputDir + 'hotel_sentiment.json', 'w')
    f.write(json.dumps(hotelSentimentMap))
    f.close()
    
    f = open(outputDir + 'att_forward.json', 'w')
    f.write(json.dumps(attributeMapForward))
    f.close()
    
    f = open(outputDir + 'att_backward.json', 'w')
    f.write(json.dumps(attributeMapBackward))
    f.close()

if __name__ == "__main__":
    global attributeCloud
    initialize(attributeCloud)
    global hotelSentimentMap
    global sentimentMap
    cityArr = open(inputFileName, 'r').read().split('\n')
    for line in cityArr:
        if '\t' not in line:
            continue
        loc = line[:line.index('\t')]
        sentimentMap[loc] = {}
        hotelSentimentMap[loc] = {}
        hotelDetails = json.loads(line[line.index('\t'):])  # hotelId: [[{}]]
        for hotelId in hotelDetails:
            reviewArr = hotelDetails[hotelId]                 # array of array [[{}]]
            reviewCount = 0
            for review in reviewArr:                            # array [{}]
#                sentiment = sentence['sentiment']
#                wordDetails = sentence['worddetails']        # array of objects [{}]
#                    # do your magic here.
                buildSentimentMap(loc, review, hotelId, reviewCount)
                reviewCount += 1
    global outputDir
    storeMapInFileSystem(outputDir)