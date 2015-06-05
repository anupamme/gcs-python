'''
specs: 
    input: output of mapreduce job.
    output: data in the format which can be put into db.
    
    input example: 
        {"PARIS:FRANCE":
            {
                "g187147-d4800782":
                    [[{"sentiment":"Negative","worddetails":[{"pos":"PRP","word":"I"},{"pos":"VBP","word":"have"},{"pos":"VBN","word":"visited"},{"pos":"NNP","word":"Paris"},{"pos":"RB","word":"just"},{"pos":"IN","word":"for"},{"pos":"DT","word":"a"},{"pos":"JJ","word":"short"},{"pos":"NN","word":"weekend"},{"pos":"NN","word":"break"},{"pos":"IN","word":"before"},{"pos":"NNP","word":"Xmas"},{"pos":"CD","word":"2014"},{"pos":".","word":"."}]}]]
                    
    output format:
        subattribute, sentiment -> {hotelid, [(reviewid, probability)]}
        hotelid -> {(subattribute -> {sentiment -> numberOfWeightedMentions})}

'''

attributeMap = {
    'purpose': ['honeymoon', 'business', 'solo', 'friends', 'backpacking'],
    'amenity' : ['internet', 'staff', 'wifi', 'bar', 'breakfast', 'parking', 'suite', 'air_conditioning', 'wheelchair_access', 'fitness_center', 'airport', 'pool', 'spa', 'pets'],
}

hierarchicalAttributeMap = {
    'food': ['indian', 'french', 'japanese', 'thai', 'italian'],
    'view': ['mountain', 'downtown', 'ocean', 'forest']
}

foodTypeMap = {
    'indian' : ["dosa", "butter_chicken", "lentil", "dal", "samosa", "roti", "naan", "biryani", "momos", "idli", "masala"],
    'french' : ["baguette", "crepes", "croissant", "macarons", "madeleine", "lamb_curry", "patisserie", "quiches", "wine", "cheese"],
    'thai': ["papaya_salad", "pad_thai", "green_curry", "curry", "fried_rice", "roast_duck", "beef_salad", "coconut_cream", "shrimp", "soup"],
    'japanese': ["sushi", "ramen", "unagi", "tempura", "kaiseki", "soba", "teriyaki", "wasabi", "udon", "noodles"],
    'italian': ["pasta", "pizza", "spaghetti", "pesto", "bruschetta", "focaccia", "margherita", "risotto", "pomodoro", "tiramisu", "parmigiana", "lasagna", "tomato_sauce", "olives"]
}

import gensim
from gensim.models import word2vec
import nltk
import os
import sys
import json
import re
import string
import operator

pattern = re.compile('\d')
exclude = set(string.punctuation)

out_subattrbuteIndex = {}
out_hotelAttributeIndex = {}

error_hash = {}
unicode_hash = {}

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
    

def buildWordCloud(model1, model2):
    subAttributeCloud = {}
    attributeCloud = {}
    for attr in attributeMap:
        for subAttr in attributeMap[attr]:
            similarAttributes1 = model1.most_similar(subAttr)
            similarAttributes2 = model2.most_similar(subAttr)
            if attr not in subAttributeCloud:
                subAttributeCloud[attr] = {}
            subAttributeCloud[attr][subAttr] = similarAttributes1 + similarAttributes2
            
    for attr in hierarchicalAttributeMap:
        similarAttributes1 = model1.most_similar(attr)
        similarAttributes2 = model2.most_similar(attr)
        attributeCloud[attr] = similarAttributes1 + similarAttributes2
        for subAttr in hierarchicalAttributeMap[attr]:
            similarAttributes1 = model1.most_similar(subAttr)
            similarAttributes2 = model2.most_similar(subAttr)
            if attr not in subAttributeCloud:
                subAttributeCloud[attr] = {}
            subAttributeCloud[attr][subAttr] = similarAttributes1 + similarAttributes2
    return attributeCloud, subAttributeCloud

def normalize(word):
    word = word.decode('utf-8')
    wordWithoutNumerals = re.sub(pattern, '', word).replace('-', ' ').replace('.', ' ')
    wordWithoutPunctuation = ''.join(ch for ch in wordWithoutNumerals if ch not in exclude)
    return wordWithoutPunctuation.lower().strip()

def findMaxOnSentence(reviewDetails, similarAttributes, model, webSize):
    sumValue = {}
    for detail in reviewDetails:
        if detail['word'] in unicode_hash:
            continue
        pos = detail['pos']
        word = ''
        try:
            word = detail['word'].encode('utf-8')
        except UnicodeError:
            print ('unicode error: ' + str(detail['word'].encode('utf-8')))
            unicode_hash[detail['word']] = True
            continue
        if pos == 'NP' or pos == 'NN':
            n_word = normalize(word)
            if n_word in error_hash:
                continue
            weightedSum = []
            for simAttr in similarAttributes:
                try:
                    weightedSum.append(model.similarity(n_word, simAttr[0])*simAttr[1])   # hotel
                except KeyError:
                    print ('Key Error: ' + n_word)
                    error_hash[n_word] = True
                    break
            length = len(weightedSum)

            weightedSum.sort()
            subsetSortedWeightedSum = weightedSum[length - webSize:]
            sumSubsetSortedWeightedSum = sum(subsetSortedWeightedSum)
            sumValue[n_word] = sumSubsetSortedWeightedSum/webSize
    return sumValue

def findSubAttributes(model, reviewDetails, attr, subAttrArray, numberOfQualifiedResults, webSize, subAttributeCloud):
    # steps: for each subattribute, pick their word cloud.
    # find the distance of subattr-cloud with the cloud of reviewdetails - find cloud of reviewdetails.
    # pick the subattributes which are most similar to the reviewdetails.
    # output: [(subattribute, probability)]
    output = []
    for subAttr in subAttrArray:
        similarAttributes = subAttributeCloud[attr][subAttr]
        sumValue = findMaxOnSentence(reviewDetails, similarAttributes, model, webSize)
        sortedSumValue = sorted(sumValue.items(), key=operator.itemgetter(1))
        subsetSortedSumValue = sortedSumValue[len(sortedSumValue) - webSize:]
        sumSubsetSortedSumValue = sum(list({x[1] for x in subsetSortedSumValue}))
        
        output.append((subAttr, sumSubsetSortedSumValue/webSize))
    return output
    

def findMostProbableSentiment(subAttrDetails, sentenceDetails, model, webSize):
    #input format: subAttrDetails: [(subattribute, probability)]
    # sentenceDetails: [(worddetails, sentiment)]
    # output: {subattribute -> sentiment}
    output = {}
    for subAttdetail in subAttrDetails:
        subAttr, prob = subAttdetail
        similarAttributes = model.most_similar(subAttr)
        maxVal = -1
        selectedSentiment = -1
        for sentence in sentenceDetails:
            wordDetail, sentiment = sentence
            sumValue = findMaxOnSentence(wordDetail, similarAttributes, model, webSize) #format: word -> value
            sortedSumValue = sorted(sumValue.items(), key=operator.itemgetter(1))
            subsetSortedSumValue = sortedSumValue[len(sortedSumValue) - webSize:]
            sumSubsetSortedSumValue = sum(list({x[1] for x in subsetSortedSumValue}))
            if sumSubsetSortedSumValue > maxVal:
                maxVal = sumSubsetSortedSumValue
                selectedSentiment = sentiment
        output[subAttr] = selectedSentiment
    return output

def addOrInsert2(result, locationKey, hotelId, reviewId, attr, metaprobability, subAttrCompleteDetails):
    if locationKey not in result:
        result[locationKey] = {}
    if hotelId not in result[locationKey]:
        result[locationKey][hotelId] = {}
    if reviewId not in result[locationKey][hotelId]:
        result[locationKey][hotelId][reviewId] = {}
    result[locationKey][hotelId][reviewId][attr] = (metaprobability, subAttrCompleteDetails)
    return result

def findMetaProbability(model, reviewDetails, attr, webSize, attributeCloud):
    similarAttributes = attributeCloud[attr]
    sumValue = findMaxOnSentence(reviewDetails, similarAttributes, model, webSize)
    sortedSumValue = sorted(sumValue.items(), key=operator.itemgetter(1))
    subsetSortedSumValue = sortedSumValue[len(sortedSumValue) - webSize:]
    sumSubsetSortedSumValue = sum(list({x[1] for x in subsetSortedSumValue}))
    return sumSubsetSortedSumValue

def findMostProbableAttributes(inputData, model1, model2, attributeCloud, subAttributeCloud):
    webSize = 5
    result = {}
    for locationKey in inputData:
        print ('locationKey: ' + locationKey)
        hotelDetails = inputData[locationKey]
        for hotelKey in hotelDetails:
            print ('hotelKey: ' + hotelKey)
            reviewSet = hotelDetails[hotelKey]
#            reviewArr = reviewSet['reviews']
            reviewId = 0
            for review in reviewSet:
                reviewDetails = []
                sentenceDetails = []
                for sentence in review:    
                    sentiment = sentence['sentiment']
                    wordDetails = sentence['worddetails']
                    sentenceDetails.append((wordDetails, sentiment))
                    reviewDetails = reviewDetails + wordDetails
                # check here whether attribute type is present in the text.
                #steps: 1. find corresponding reviewid
                # step 2: for all attributes find metaprobability.
                for attr in attributeMap:
                    metaprobability = 1.0
                    # step 2.1: find subattributes.
                    numberOfQualifiedResults = 1
                    if attr == 'amenity':
                        numberOfQualifiedResults = 3
                    #print ('Finding sub-attributes for: ' + str(reviewDetails))
                    subAttrDetails = findSubAttributes(model, reviewDetails, attr, attributeMap[attr], numberOfQualifiedResults, webSize, subAttributeCloud)
                    #print ('Finding most-probable sentiment for: ' + str(sentenceDetails))
                    subAttrSentiment = findMostProbableSentiment(subAttrDetails, sentenceDetails, model, webSize)
                    subAttrCompleteDetails = []
                    for detail in subAttrDetails:
                        subAttr, prob = detail
                        sentiment = subAttrSentiment[subAttr]
                        subAttrCompleteDetails.append({'subAttr' : subAttr, 'sentiment': sentiment, 'probability': prob})
                    result = addOrInsert2(result, locationKey, hotelKey, reviewId, attr, metaprobability, subAttrCompleteDetails)
                
                for attr in hierarchicalAttributeMap:
                    # calculate metaprobability
                    metaprobability = findMetaProbability(model, reviewDetails, attr, webSize, attributeCloud)
                    numberOfQualifiedResults = 1
                    if attr == 'food':
                        numberOfQualifiedResults = 2
                    #print ('Finding hier-sub-attributes for: ' + str(reviewDetails))
                    subAttrDetails = findSubAttributes(model, reviewDetails, attr, hierarchicalAttributeMap[attr], numberOfQualifiedResults, webSize, subAttributeCloud)
                    #print ('Finding hier-most-probable sentiment for: ' + str(sentenceDetails))
                    subAttrSentiment = findMostProbableSentiment(subAttrDetails, sentenceDetails, model, webSize)
                    subAttrCompleteDetails = []
                    for detail in subAttrDetails:
                        subAttr, prob = detail
                        sentiment = subAttrSentiment[subAttr]
                        subAttrCompleteDetails.append({'subAttr' : subAttr, 'sentiment': sentiment, 'probability': prob})
                    result = addOrInsert2(result, locationKey, hotelKey, reviewId, attr, metaprobability, subAttrCompleteDetails)
                    
                reviewId += 1
            
    return result
                


def parseAndCreateSortedResults():
    inputfile = sys.argv[1]     # output of the stanford nlp routine.
    print ("input file: " + inputfile)
    inputData = json.loads(open(inputfile, 'r').read())
    modelFile = sys.argv[2]
    model1 = word2vec.Word2Vec.load_word2vec_format(modelFile, binary=True)
    modelFile2 = sys.argv[3]
    model2 = word2vec.Word2Vec.load_word2vec_format(modelFile2, binary=True)
    # iterate through all of attributes and hierarchical attributes.
    # step 1: build the word cloud
    attributeCloud, subAttributeCloud = buildWordCloud(model1, model2)
    # step 2: read the input file and find most probable attributes for each review.
    # input format: inputfile, model, attributeCloud, subAttributeCloud.
    # output format: cityKey -> hotelId -> {reviewId -> {attribute -> (metaprobability, [(subattribute, sentiment, probability)])}}
    sortedAttributesOnProbability = findMostProbableAttributes(inputData, model1, model2, attributeCloud, subAttributeCloud)
    return sortedAttributesOnProbability
    

# arguments: [input_model], input_text, output_file
if __name__ == "__main__":
    sortedResults = parseAndCreateSortedResults()   # format is {cityKey -> {hotelid -> {reviewId -> {attribute -> (metaprobability, [(subattribute, sentiment, probability)])}}}}
    # iterate over the results to transform them into desired form.
    for key in sortedResults:
        hotelDictionary = sortedResults[key]
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