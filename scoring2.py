import gensim
from gensim.models import word2vec
import nltk

''' input is in the format: location -> {hotelid -> [review]}, where review = [sentence] where
sentence = (sentiment, [word_pos]) where word_pos = (word, pos_tag)'''

''' output is in the format: review -> {attribute -> probabilty} and review -> {attribute -> sentiment}

intermediate data structure: review -> [(attribute, probability, sentiment)]'''

''' 
Goal: Find distance between a piece of text and a keyword. Keyword can be attribute or purpose e.g. honeymoon, indian cuisine, swimming

We can find the distance for the complete review or a sentence.
We find the keyword or attribute to which the piece of text is most closely associated with contextually.

Algorithm: 1. For each word in of special pos_tag: {NN, NNP} we find the distance from all the keywords of that category e.g. for purpose category, keywords are honeymoon, business, solo etc.
    2. Average the distance over all the words in 1.
    3. Select the keyword, subattribute for which the number is step 2 is max
    4. 

How to know whether a word belongs to a particular category or subcategory?
    1. 
    


'''

import os
import sys
import json
import re
import string
import operator

pattern = re.compile('\d')
exclude = set(string.punctuation)

attributeMap = {
    'purpose': ['honeymoon', 'business', 'solo', 'friends', 'backpacking'],
    'food': ['indian', 'french', 'japanese', 'thai', 'italian'],
    'location': ['airport', 'suburb', 'downtown'],
    'amenity' : ['internet', 'staff', 'wifi', 'bar', 'breakfast', 'parking', 'suite', 'air_conditioning', 'wheelchair_access', 'fitness_center', 'airport', 'pool', 'spa', 'pets'],
}

subAttributeCloud = {}  #purpose -> subattribute -> [(keyword, distance)]

def fillSubAttributeCloud(model, attr):         # use domain specific model for this.
    for subAttribute in attributeMap[attr]:
        print ('finding most similar words for: ' + subAttribute)
        similarAttributes = model.most_similar(subAttribute)
        print ('found most similar words.')
        if attr not in subAttributeCloud:
            subAttributeCloud[attr] = {}
        subAttributeCloud[attr][subAttribute] = similarAttributes

def normalize(word):
    word = word.decode('utf-8')
    wordWithoutNumerals = re.sub(pattern, '', word).replace('-', ' ').replace('.', ' ')
    wordWithoutPunctuation = ''.join(ch for ch in wordWithoutNumerals if ch not in exclude)
    return wordWithoutPunctuation.lower().strip()

def findAvgSimilarityCloud(model, attr, subAttr, wordDetails, cloudCount, wordCount):
    sumValue = {}
    weightedSum = {}
    similarAttributes = subAttributeCloud[attr][subAttr]
    for detail in wordDetails:
        pos = detail['pos']
        word = ''
        try:
            word = detail['word'].encode('ascii')
        except UnicodeError:
            print ('unicode error: ' + str(detail['word'].encode('utf-8')))
            continue
        if pos == 'NP' or pos == 'NN':
            n_word = normalize(word)
            weightedSum = {}
            for simAttr in similarAttributes:
                try:
                    weightedSum[n_word] = model.similarity(n_word, simAttr[0])*simAttr[1]
                except KeyError:
                    print ('Key Error: ' + n_word)
                    break
            length = len(weightedSum)
            
            sortedWeightedSum = sorted(weightedSum.items(), key=operator.itemgetter(1))
            subsetSortedWeightedSum = sortedWeightedSum[length - cloudCount:]
            sumSubsetSortedWeightedSum = sum(list({x[1] for x in subsetSortedWeightedSum}))
            
            sumValue[n_word] = sumSubsetSortedWeightedSum/cloudCount
    sortedSumValue = sorted(sumValue.items(), key=operator.itemgetter(1))
    subsetSortedSumValue = sortedSumValue[len(sortedSumValue) - wordCount:]
    sumSubsetSortedSumValue = sum(list({x[1] for x in subsetSortedSumValue}))
    return sumSubsetSortedSumValue/wordCount
                

def findAvgSimilarity(model, att, wordDetails):
    sum_value = 0
    for detail in wordDetails:
        pos = detail['pos']
        word = ''
        try:
            word = detail['word'].encode('ascii')
        except UnicodeError:
            print('unicode error' + detail['word'].encode('utf-8'))
            continue
        word = word.decode('ascii')
        #print 'word detail: ' + pos + ' : ' + word
        if pos == 'NP' or pos == 'NN':
            n_word = normalize(word)
            #print 'after normalize: ' + n_word
            if ' ' in n_word:
                sp_index = n_word.index(' ')
                n_word = n_word[:sp_index]
            val = 0
            try:
                val = model.similarity(att, n_word)
            except KeyError:
                print('Key Error: ' + n_word)
                break
            #print 'similarity: ' + str(val)
            sum_value = sum_value + val
    return sum_value/len(wordDetails)

def sortReviewsOnScore():
    

if __name__ == "__main__":
    inputfile = sys.argv[1]     # output of the stanford nlp routine.
    inputData = json.loads(open(inputfile, 'r').read())
    attributeType = sys.argv[2]
    selectionNumber = int(sys.argv[3])
    cloudSize = int(sys.argv[4])
    wordSize = int(sys.argv[5])
    attributes = attributeMap[attributeType]
    print('loading model data:')
    model = word2vec.Word2Vec.load_word2vec_format('vectors-phrase.bin', binary=True)
    print('start filling sub attribute cloud.')
    fillSubAttributeCloud(model, attributeType)
    print('end filling sub attribute cloud.')
    for locationKey in inputData:
        hotelDetails = inputData[locationKey]
        for hotelKey in hotelDetails:
            reviewSet = hotelDetails[hotelKey]
            for review in reviewSet:
                reviewDetails = []
                fullReview = ''
                for sentence in review:    
                    sentiment = sentence['sentiment']
                    wordDetails = sentence['worddetails']
                    fullsentence = ' '.join(x['word'] for x in wordDetails).encode('utf-8')
                    reviewDetails = reviewDetails + wordDetails
                    fullReview += fullsentence.decode('utf-8')
                attributeScore = {}
                for att in attributes:
                    localAvgSimilarity = findAvgSimilarityCloud(model, attributeType, att, reviewDetails, cloudSize, wordSize)
                    attributeScore[att] = localAvgSimilarity
                sorted_attributeScore = sorted(attributeScore.items(), key=operator.itemgetter(1))
                totalAttributes = len(sorted_attributeScore)
                if totalAttributes == 0:
                    print('Not able to find attribute for ' + fullReview)
                else:
                    selectedAttributes = sorted_attributeScore[totalAttributes - selectionNumber:]
                    selectedAttributesStr = '; '.join(str(x[0]) + ',' + str(x[1]) for x in selectedAttributes)
                    print('Found: ' + fullReview + ' : ' + selectedAttributesStr)