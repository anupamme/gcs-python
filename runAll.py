'''
Runs the complete tasks from scraping to creating indices and storing them in db so that app can use them as it is.

Details:

Routine: Input source; Input format: Output destination: Output format.

1. scrape (map-reduce): Input: list of cities (1 in each line). Source: bucket. Output format: A big file with each line containing data about each city. Destination: bucket. SOLVED.

2. Routine which prepares the data for sentiment-analysis. input and source: output of step 1. output: json format for sentiment analyzer. destination: bucket. 

3. sentiment analyzer: runs sentiment analyzer (map-reduce). input: output of step 1. output: text file delimited by \n. Each line containing sentiment about 1 review. destination: bucket.

4. routine to put sentiment output together. input: output of step 3. output: indices of sentiment and hotel as required by hotel search routine.

5. mining of reviews for features like food type, etc (map-reduce). input: bag of words. source: bucket. output: index from food type etc to reviews. destination: bucket.

6. routine to put all the data from step 4 and 5 into datastore.
'''

