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

hierarchicalAttributeMap = {
    'food': ['indian', 'french', 'japanese', 'thai', 'italian'],
    'view': ['mountain', 'downtown', 'ocean', 'forest'],
    'amenity' : ['internet', 'staff', 'wifi', 'bar', 'breakfast', 'parking', 'suite', 'air_conditioning', 'wheelchair access', 'fitness_center', 'pool', 'spa', 'pets']
}

foodTypeMap = {
    'indian' : ["dosa", "butter_chicken", "lentil", "dal", "samosa", "roti", "naan", "biryani", "momos", "idli", "masala"],
    'french' : ["baguette", "crepes", "croissant", "macarons", "madeleine", "lamb_curry", "patisserie", "quiches", "wine", "cheese"],
    'thai': ["papaya_salad", "pad_thai", "green_curry", "curry", "fried_rice", "roast_duck", "beef_salad", "coconut_cream", "shrimp", "soup"],
    'japanese': ["sushi", "ramen", "unagi", "tempura", "kaiseki", "soba", "teriyaki", "wasabi", "udon", "noodles"],
    'italian': ["pasta", "pizza", "spaghetti", "pesto", "bruschetta", "focaccia", "margherita", "risotto", "pomodoro", "tiramisu", "parmigiana", "lasagna", "tomato_sauce", "olives"]
}

hierarchicalAttCloud = {}
hierarchicalSubAttCloud = {}

def fillHierarchicalAttributeCloud(model, attr):         # use domain specific model for this.
    #print ('finding most similar words for: ' + attr)
    similarAttributes = model.most_similar(attr)
    #print ('found most similar words.')
    hierarchicalAttCloud[attr] = similarAttributes
    
    if attr == 'food':
        hierarchicalSubAttCloud[attr] = {}
        for subattr in foodTypeMap:
            hierarchicalSubAttCloud[attr][subattr] = []
            keywords = foodTypeMap[subattr]
            for keyword in keywords:
                similarAttributes = model.most_similar(keyword.lower())
                hierarchicalSubAttCloud[attr][subattr] = hierarchicalSubAttCloud[attr][subattr] + similarAttributes
    else:
        for subAttribute in hierarchicalAttributeMap[attr]:
            similarAttributes = model.most_similar(subAttribute.lower())
            if attr not in hierarchicalSubAttCloud:
                hierarchicalSubAttCloud[attr] = {}
            hierarchicalSubAttCloud[attr][subAttribute] = similarAttributes
    

def normalize(word):
    word = word.decode('utf-8')
    wordWithoutNumerals = re.sub(pattern, '', word).replace('-', ' ').replace('.', ' ')
    wordWithoutPunctuation = ''.join(ch for ch in wordWithoutNumerals if ch not in exclude)
    return wordWithoutPunctuation.lower().strip()


def findAvgSimilarityCloud(model, attr, subAttr, wordDetails, cloudCount, wordCount):
    sumValue = {}
    weightedSum = {}
    similarAttributes = hierarchicalSubAttCloud[attr][subAttr]
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
            weightedSum = []        # of type double. will contain value of distance from n_word to similar words of the subattribute.
            for simAttr in similarAttributes:
                try:
                    weightedSum.append(model.similarity(n_word, simAttr[0])*simAttr[1])   # hotel
                except KeyError:
                    print ('Key Error: ' + n_word)
                    break
            length = len(weightedSum)
            
            weightedSum.sort()
            subsetSortedWeightedSum = weightedSum[length - cloudCount:]
            sumSubsetSortedWeightedSum = sum(subsetSortedWeightedSum)
            
            sumValue[n_word] = sumSubsetSortedWeightedSum/cloudCount
    sortedSumValue = sorted(sumValue.items(), key=operator.itemgetter(1))
    subsetSortedSumValue = sortedSumValue[len(sortedSumValue) - wordCount:]
    sumSubsetSortedSumValue = sum(list({x[1] for x in subsetSortedSumValue}))
    return sumSubsetSortedSumValue/wordCount
                

'''
Cloud algorithm: What is does is pretty simple. It has stored similar words to the attr.subattribute.

So it iterates all worddetails and for each NN or NP, it finds the distance with each of the similar words for this attribute, multiplied with the similarity index of the similar word. So weighted sum for each word.



'''
    
def findAvgSimilarityCloudMeta(model, attr, wordDetails, cloudCount, wordCount):
    sumValue = {}
    weightedSum = {}
    similarAttributes = hierarchicalAttCloud[attr]
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
            weightedSum = []        # of type double. will contain value of distance from n_word to similar words of the subattribute.
            for simAttr in similarAttributes:
                try:
                    weightedSum.append(model.similarity(n_word, simAttr[0])*simAttr[1])   # hotel
                except KeyError:
                    print ('Key Error: ' + n_word)
                    break
            length = len(weightedSum)
            #print (weightedSum)
            weightedSum.sort()
            #print (weightedSum)
            subsetSortedWeightedSum = weightedSum[length - cloudCount:]
            sumSubsetSortedWeightedSum = sum(subsetSortedWeightedSum)
            
            sumValue[n_word] = sumSubsetSortedWeightedSum/cloudCount
    
    sortedSumValue = sorted(sumValue.items(), key=operator.itemgetter(1))
    subsetSortedSumValue = sortedSumValue[len(sortedSumValue) - wordCount:]
    sumSubsetSortedSumValue = sum(list({x[1] for x in subsetSortedSumValue}))
    return sumSubsetSortedSumValue/wordCount


if __name__ == "__main__":
    inputfile = sys.argv[1]     # output of the stanford nlp routine.
    inputData = json.loads(open(inputfile, 'r').read())
    attributeType = sys.argv[2]
    selectionNumber = int(sys.argv[3])
    cloudSize = int(sys.argv[4])
    wordSize = int(sys.argv[5])
    attributes = hierarchicalAttributeMap[attributeType]
    #print('loading model data:')
    model = word2vec.Word2Vec.load_word2vec_format('vectors-phrase.bin', binary=True)
    #print('start filling sub attribute cloud.')
    fillHierarchicalAttributeCloud(model, attributeType)
    #print('end filling sub attribute cloud.')
    for locationKey in inputData:
        hotelDetails = inputData[locationKey]
        for hotelKey in hotelDetails:
            reviewSet = hotelDetails[hotelKey]
            result = []
            for review in reviewSet:
                reviewDetails = []
                for sentence in review:    
                    sentiment = sentence['sentiment']
                    wordDetails = sentence['worddetails']
                    reviewDetails = reviewDetails + wordDetails
                # check here whether attribute type is present in the text.
                attTypeSimilarity = findAvgSimilarityCloudMeta(model, attributeType, reviewDetails, cloudSize, wordSize)
                print('META Similarity: ' + attributeType + ' : ' + str(attTypeSimilarity))
                attributeScore = {}
                for att in attributes:
                    localAvgSimilarity = findAvgSimilarityCloud(model, attributeType, att, reviewDetails, cloudSize, wordSize)
                    attributeScore[att] = localAvgSimilarity
                sorted_attributeScore = sorted(attributeScore.items(), key=operator.itemgetter(1))
                totalAttributes = len(sorted_attributeScore)
                fullReview = ' '.join(x['word'] for x in reviewDetails)
                if type(fullReview) == bytes:
                    fullReview = fullReview.decode('utf-8')
                if totalAttributes == 0:
                    print('Not able to find attribute for ' + fullReview)
                else:
                    selectedAttributes = sorted_attributeScore[totalAttributes - selectionNumber:]
                    selectedAttributesStr = '; '.join(str(x[0]) + ',' + str(x[1]) for x in selectedAttributes)
                    if type(selectedAttributesStr) == bytes:
                        selectedAttributesStr = selectedAttributesStr.decode('utf-8')
                    
                    val = (attTypeSimilarity, fullReview, selectedAttributes)
                    result.append(val)
                    #print('Found: ' + fullReview + ' : ' + selectedAttributesStr)
            sortedResult = sorted(result, key=operator.itemgetter(0))
            print ('sorted results for hotel: ' + hotelKey + ' : ')
            print(sortedResult)