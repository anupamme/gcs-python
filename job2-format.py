'''
input: each line
"PARIS:FRANCE"	jsonobj where
    jsonobj has following structure:
        {
        "g187147-d4800782": {
            "reviews": [{"rating": "5 of 5 stars", "room_tip": "", "ReviewerName": "Racingsnakepete", "traveled_as": "Stayed November 2014, travelled as a couple", "title": "Superb little hotel with fantastic staff", "review": "This hotel is well worthy of its high ranking on Tripadvisor. A real little gem. Movie themed rooms are great and not \"kitsch\" in the slightest. Very comfy bed, great shower, ipod hifi and quality coffee maker. Also very quiet despite over-looking main road (Sebastapol Boulevard). Probably due to triple glazing or glass so thick that you couldn't throw a TV through it. Great bar on ground floor with some average priced cocktails. Small cinema downstairs. Well recommended.", "Place": "", "ReviewerImage": "http://media-cdn.tripadvisor.com/media/photo-l/01/2e/70/5a/avatar032.jpg", "Date": "12 December 2014", "management_response": "", "Badges": "2 reviews Reviews in 2 cities 5 helpful votes"}, {"rating": "5 of 5 stars", "room_tip": "", "ReviewerName": "CelticBhoy81", "traveled_as": "Stayed December 2014, travelled as a couple", "Service": "5 of 5 stars", "title": "lovely hotel with great little touches included", "Cleanliness": "4 of 5 stars", "review": "This hotel definitely deserves its place so high up the paris rankings. The check in was very smooth, Some lovely touches like the in room espresso machines, complimentary soft drinks in rooms and complimentary snacks and buffet during the day in the hotel. We had a few drinks in the bar as well at night and these were very well made cocktails and gin and tonics, breakfast was also excellent with everything you really need included. Over all a fantastic 1 night stay and we will def come back when next in Paris. This hotel really does run smoothly. Well worth the money.", "Place": "Scotland", "ReviewerImage": "http://media-cdn.tripadvisor.com/media/photo-l/03/e7/5c/83/celticbhoy81.jpg", "Date": "11 December 2014", "management_response": "", "Badges": "62 reviews 24 hotel reviews Reviews in 27 cities 71 helpful votes", "Rooms": "5 of 5 stars"}], 
            "details": {
                "images": ["http://c1.tacdn.com/img2/x.gif", "http://media-cdn.tripadvisor.com/media/photo-s/06/82/8c/47/le-123-sebastopol-astotel.jpg"], 
                "address": "123, boulevard de Sebastopol 123, boulevard de Sebastopol", 
                "locality": "75002 Paris 75002 Paris", 
                "amenties": [], 
                "title": "Le 123 Sebastopol - Astotel Hotel, Paris Le 123 Sebastopol - Astotel"}
            }
        }
'''

import json

inputFileName = 'job1-out.txt'
title = "colorsArray"

def encodeAll(reviewList):
    for review in reviewList:
        for detail in review:
            review[detail].encode('utf-8')
    return reviewList

if __name__ == "__main__":
    # read the file:
    cityArr = open(inputFileName, 'r').read().split('\n')
    global title
    final = []
    delim = '\t:\t'
    for line in cityArr:
        if delim not in line:
            continue
        loc = line[:line.index(delim)]
        hotelList = json.loads(line[line.index(delim) + len(delim):])
        for hotelId in hotelList:
            hotel = hotelList[hotelId]
            reviews = hotel["reviews"]
            obj = {}
            obj['location'] = loc.replace('"', '').decode('utf-8')
            obj['hotelid'] = hotelId
            obj['details'] = hotel['details']
            obj['reviewList'] = encodeAll(reviews)
            final.append(obj)
    output = { title: final}
    f = open('job2-out.json', 'w')
    f.write(json.dumps(output))