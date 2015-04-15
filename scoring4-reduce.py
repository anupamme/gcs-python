import json
import sys
import os

out_subattrbuteIndex = {}
out_hotelAttributeIndex = {}

def convertSentimentToInt(sentiment):
    sentiment_case = sentiment.lower().replace(' ', '')
    if 'verynegative' == sentiment_case:
        return '0'
    if 'negative' == sentiment_case:
        return '1'
    if 'neutral' == sentiment_case:
        return '2'
    if 'positive' == sentiment_case:
        return '3'
    if 'verypositive' == sentiment_case:
        return '4'

def addOrInsert(locationKey, attr, subAttr, hotelId, reviewId, probability, sentiment):
    sentimentInt = convertSentimentToInt(sentiment)
    
    if locationKey not in out_subattrbuteIndex:
        out_subattrbuteIndex[locationKey] = {}
    if attr not in out_subattrbuteIndex[locationKey]:
        out_subattrbuteIndex[locationKey][attr] = {}
    if subAttr not in out_subattrbuteIndex[locationKey][attr]:
        out_subattrbuteIndex[locationKey][attr][subAttr] = {}
    if hotelId not in out_subattrbuteIndex[locationKey][attr][subAttr]:
        out_subattrbuteIndex[locationKey][attr][subAttr][hotelId] = []
    
    out_subattrbuteIndex[locationKey][attr][subAttr][hotelId].append(str((reviewId, probability, sentimentInt)))
    
    # do it for hotelid
    if locationKey not in out_hotelAttributeIndex:
        out_hotelAttributeIndex[locationKey] = {}
    if hotelId not in out_hotelAttributeIndex[locationKey]:
        out_hotelAttributeIndex[locationKey][hotelId] = {}
    if attr not in out_hotelAttributeIndex[locationKey][hotelId]:
        out_hotelAttributeIndex[locationKey][hotelId][attr] = {}    
    if subAttr not in out_hotelAttributeIndex[locationKey][hotelId][attr]:
        out_hotelAttributeIndex[locationKey][hotelId][attr][subAttr] = {}
    if sentimentInt not in out_hotelAttributeIndex[locationKey][hotelId][attr][subAttr]:
        out_hotelAttributeIndex[locationKey][hotelId][attr][subAttr][sentimentInt] = 0.0
    out_hotelAttributeIndex[locationKey][hotelId][attr][subAttr][sentimentInt] = out_hotelAttributeIndex[locationKey][hotelId][attr][subAttr][sentimentInt] + float(probability)

if __name__ == "__main__":
    inputDir = sys.argv[1]
    listFiles = os.listdir(inputDir)
    basePath = os.path.join(os.getcwd(), inputDir)
    for fName in listFiles:
        path = os.path.join(basePath, fName)
        data = json.loads(open(path, 'r').read())
        for key in data:
            hotelDictionary = data[key]
            for hotelId in hotelDictionary:
                reviewDictionary = hotelDictionary[hotelId]
                for reviewId in reviewDictionary:
                    attributeDictionary = reviewDictionary[reviewId]
                    for attr in attributeDictionary:
                        metaprobability, subAttrDetails = attributeDictionary[attr]
                        for subAttrDet in subAttrDetails:
                            #print ('subAttrDetails: ' + str(subAttrDet))
                            subAttr = subAttrDet['subAttr']
                            sentiment = subAttrDet['sentiment']
                            probability = subAttrDet['probability']
                            sentimentInt = convertSentimentToInt(sentiment)
    #                        strKey = str((subAttr, sentimentInt))
    #                        if strKey not in out_subattrbuteIndex:
    #                            out_subattrbuteIndex[strKey] = {}
                            #print('prob, metaprob: ' + str(probability) + ' : ' + str(metaprobability))
                            #print ('types: ' + str(type(strKey)) + ' : ' + str(type(hotelId)))
    #                        out_subattrbuteIndex[strKey][hotelId] = [str((reviewId, probability*metaprobability))]
                            addOrInsert(key, attr, subAttr, hotelId, reviewId, probability*metaprobability, sentiment)
    
                
    f = open('sentiment.json', 'w')
    f.write(json.dumps(out_subattrbuteIndex))
    f.close()
    f = open('hotel.json', 'w')
    f.write(json.dumps(out_hotelAttributeIndex))
    f.close()