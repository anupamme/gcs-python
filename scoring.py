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

pattern = re.compile('\d')
exclude = set(string.punctuation)
space_pattern = re.compile('-.')

def normalize(word):
    wordWithoutNumerals = re.sub(pattern, '', word).replace('-', ' ').replace('.', ' ')
    wordWithoutPunctuation = ''.join(ch for ch in wordWithoutNumerals if ch not in exclude)
    return wordWithoutPunctuation.lower().strip()

def findMaxSimilarity(model, attributes, word):
    maxVal = -1
    maxAtt = None
    #print 'before normalize: ' + word
    n_word = normalize(word)
    #print 'after normalize: ' + n_word
    if ' ' in n_word:
        sp_index = n_word.index(' ')
        n_word = n_word[:sp_index]
    for att in attributes:
        #print 'checking: '+ att + ' : ' + n_word
        try:
            val = model.similarity(att, n_word)
        except KeyError:
            print 'Key Error: ' + n_word
            break
        #print 'similarity: ' + str(val)
        if val > maxVal:
            maxVal = val
            maxAtt = att
    return maxVal, maxAtt

if __name__ == "__main__":
    inputfile = sys.argv[1]     # output of the stanford nlp routine.
    inputData = json.loads(open(inputfile, 'r').read())
    attributes = ['honeymoon', 'business', 'solo', 'friends', 'backpacking']
    model = word2vec.Word2Vec.load_word2vec_format('vectors-phrase.bin', binary=True)
    for locationKey in inputData:
        hotelDetails = inputData[locationKey]
        for hotelKey in hotelDetails:
            reviewSet = hotelDetails[hotelKey]
            for review in reviewSet:
                for sentence in review:
                    #print 'sentence: ' + str(sentence)
                    sentiment = sentence['sentiment']
                    wordDetails = sentence['worddetails']
                    fullsentence = ' '.join(x['word'] for x in wordDetails).encode('utf-8')
                    maxSimilarity = -1
                    maxAtt = None
                    #print 'length: ' + str(len(wordDetails))
                    for detail in wordDetails:
                        pos = detail['pos']
                        word = ''
                        try:
                            word = detail['word'].encode('ascii')
                        except UnicodeError:
                            print('unicode error' + detail['word'].encode('utf-8'))
                            continue
                        #print 'word detail: ' + pos + ' : ' + word
                        if pos == 'NP' or pos == 'NN':
                            #print 'word, pos: ' + word + ' : ' + pos
                            localSimilarity, localAtt = findMaxSimilarity(model, attributes, word)
                            if localSimilarity > maxSimilarity:
                                maxSimilarity = localSimilarity
                                maxAtt = localAtt
                    if maxSimilarity == -1:
                        print('Not able to find attribute for ' + fullsentence)
                    else:
                        print('Found: ' + fullsentence + ' : ' + maxAtt + ' : ' + str(maxSimilarity) + ' : ' + str(sentiment))