from mrjob.job import MRJob
from mrjob.protocol import JSONProtocol
import argparse
import urllib2
import sys
import time
import os
import bisect
import logging
import traceback
from pyquery import PyQuery as pq
import pdb
import os
import re
import threading
import httplib2
import json
import errno
from socket import error as socket_error 
    
#Set the global variables
path = ''
delayTime = 1
baseUrl = "http://www.tripadvisor.in/"
max_hotels = 1
max_reviews = 1

    
class MRWordFreqCount(MRJob):
    
    OUTPUT_PROTOCOL = JSONProtocol

    def getTextIfItIsThere(self, item):
        # once you find the text:
        # 1. encode, strip white spaces and ':' which is a special char.
        if item.length > 0:
            return pq(item[0]).text().encode('utf-8').strip().replace(':', '')
        return ''

    def getFileContentFromWeb(self, url):
        time.sleep(delayTime)
        try:
            response = urllib2.urlopen(url)
        except urllib2.HTTPError, e:
            logging.error('HTTPError = ' + str(e.code))
            return None
        except urllib2.URLError, e:
            logging.error('URLError = ' + str(e.reason))
            return None
        except httplib2.HTTPException, e:
            logging.error('HTTPException')
            return None
        except Exception:
            logging.error('generic exception: ' + traceback.format_exc())
            return None
        try:
            responseStream = response.read()
        except socket_error as serr:
            if serr.errno != errno.ECONNRESET:
                raise serr
        return responseStream
    
    def createKey(self, city, state):
        return city.upper() + ":" + state.upper()

    def downloadToFile(self, url, fileName, force = True):
        """Downloads url to file

        Inputs:
        - url : Url to be downloaded
        - fileName : Name of the file to write to
        - force : Optional boolean argument which if true will overwrite the file even it it exists

        Returns:
        - Pair indicating if the file was downloaded and a list of the contents of the file split up line-by-line

        """
        fullFileName = path+'/'+ fileName.replace("/","\\/")
        downloaded = False
        if not force and os.path.exists(fullFileName):
            pass
        else:
            fileContents = self.getFileContentFromWeb(url)
            output = None
            if fileContents == None:
                return ([], False)
            downloaded = True
            return (fileContents.split("\n"), downloaded)
    
    def getCityHotelListPage(self, content):
        pages = []
        try:
            select = pq(content)
        except:
            logging.error('error in pq routine in get city hotel list page' + content)
            return pages
        total = len(select('.quality.wrap'))
        count = 0
        while count < total:
            url = pq(select('.quality.wrap')[count])('a').attr('href')
            pages.append(url[1:])
            count = count + 1
        
        ("Total hotel urls parsed: ", len(pages))
        return pages
    
    def analyzeReviewPageModified(self, hotelid, contents, hName, pagenum, metaJson):
        """Analyzes the review page and and gets details about them which it then writes to the output file

        Inputs:
        - contents : Content string
        - hName : Name of the hotel
        - option : Tripad/Orbitz
        - outF : File to write to

        """
        select = pq(contents)
        totalRatings = len(select(".reviewSelector"))
        # create a new url which has expanded reviews
        base = "http://www.tripadvisor.in/ExpandedUserReviews-"
        targetStr = pq(select(".reviewSelector")[0]).attr('id')
        targetId = int(targetStr[targetStr.find('_')+ 1:])
        setOfReviewIds = []
        count = 0
        while count < totalRatings:
            tStr = pq(select(".reviewSelector")[count]).attr('id')
            tId = int(tStr[tStr.find('_')+ 1:])
            setOfReviewIds.append(tId)
            count = count + 1
        targetUrl = str(hotelid) + "?target=" + str(targetId) + "&context=1&reviews=" + ','.join(str(x) for x in setOfReviewIds) + "&servlet=Hotel_Review&expand=1"
        (fileContents, downloaded) = self.downloadToFile(base + targetUrl, targetUrl + ".html")
        fileContentsStr = '\n'.join(fileContents)
        mincount = 0
        while mincount < 3:
            try:
                select = pq(fileContentsStr)
                break
            except:
                logging.error("Oops wrong file contents while parsing review page.")
                mincount = mincount + 1
        if mincount >= 3:
            logging.error('error while parsing the review page number: ' + str(pagenum))
            return False
        numRatings = 0
        try:
            ratingElements = select("[id^=expanded_review_]")
            numRatings = len(ratingElements)
        except:
            logging.error("Reading expanded reviews raised exception: " + str(totalRatings))
            raise
        print('num ratings: ' + str(numRatings))
        count = 0
        while count < numRatings:
            index = 10*pagenum + count
            jsonObj = {}
            temp = pq(ratingElements[count])
            jsonObj['ReviewerImage'] = pq(temp('img')[0]).attr('src')
            reviewerName = self.getTextIfItIsThere(temp('.username'))
            jsonObj['ReviewerName'] = reviewerName
            placeOfResidence = self.getTextIfItIsThere(temp('.location'))
            jsonObj['Place'] = placeOfResidence
            badgeText = ''.join(temp('.badgeText').text())
            jsonObj['Badges'] = badgeText.encode('utf-8').strip()
            title = self.getTextIfItIsThere(temp('.noQuotes'))
            try:
                jsonObj['title'] = title
            except:
                pass #

            rating = temp(".sprite-rating_s_fill").attr('alt')
            if rating == None:
                rating = 'no-rating'
            jsonObj['rating'] = rating.encode('utf-8').strip()
            dateStr = temp(".ratingDate").attr('title')
            if dateStr == None:
                dateStr = 'no-date'
            jsonObj['Date'] = dateStr.encode('utf-8').strip()
            textStr = temp(".entry").text()
            jsonObj['review'] = textStr.encode('utf-8').strip()
            roomTip = self.getTextIfItIsThere(temp('.reviewItem'))
            jsonObj['room_tip'] = roomTip
            managementResponse = self.getTextIfItIsThere(temp(".mgrRspnInline"))
            jsonObj['management_response'] = managementResponse

            traveledAs = self.getTextIfItIsThere(temp('.recommend-titleInline'))
            jsonObj['traveled_as'] = traveledAs
            totalRatings = len(temp('.recommend-answer'))
            indRatings = 0
            print('total individual ratings: ' + str(totalRatings))
            while (indRatings < totalRatings):
                ratingName = pq(temp('.recommend-answer')[indRatings]).text()
                ratingVal = pq(pq(temp('.recommend-answer')[indRatings])('img')[0]).attr('alt')
                indRatings += 1
                jsonObj[ratingName] = ratingVal
            metaJson.append(jsonObj)
            count = count + 1

        return True
    
    def getTAReviewsForHotel(self, revUrl, city):
        """Function to get all reviews for a particular hotel from tripadvisor"""
        revStr = "-Reviews-"
        global baseUrl
        (fileContent, dwnld) = self.downloadToFile(baseUrl+revUrl, revUrl)
        fileContentStr = '\n'.join(fileContent)
        jsonObj2 = {}
        mincount = 0
        while mincount < 3:
            try:
                select = pq(fileContentStr)
                break
            except:
                logging.error("Oops wrong file contents while parsing hotel url")
                mincount = mincount + 1
        if mincount >= 3:
            logging.error('ERROR: while parsing hotel details for revUrl: ' + baseUrl+revUrl)
            return {}

        hotelidStr = "Hotel_Review-"
        hotelidStartIndex = revUrl.find(hotelidStr) + len(hotelidStr)
        hotelidEndIndex = revUrl.find(revStr)
        hotelid = revUrl[hotelidStartIndex:hotelidEndIndex]
        title = select("h1").text()
        jsonObj2['title'] = title
        jsonObj2['address'] = select(".street-address").text()
        jsonObj2['locality'] = select(".locality").text()

        overallrating = select(".sprite-rating_cl_gry_fill").attr('alt')
        amenArr = []
        amenitiesArr =  select('.tab_amenity_text')
        for amen in amenitiesArr:
            amenArr.append(pq(amen).text())
        jsonObj2['amenties'] = amenArr

        length = len(select(".pgLinks")("a"))
        if length == 0:
            return {hotelid: {"details": jsonObj2, "reviews": {}}}
        try:
            totalpg = int(select(".pgLinks")("a")[1].text)
        except:
            totalpg = 1
            logging.error("ERROR: Not able to parse number of pages: ", select(".pgLinks")("a"))

        hotelsub = hotelid[hotelid.index('-') + 2:]
        imageUrl = 'LocationPhotoAlbum?detail=' + hotelsub+ '&filter=1&albumViewMode=images'
        (fileContent, dwnld) = self.downloadToFile(baseUrl + imageUrl, imageUrl)
        if dwnld:
            imageArr = []
            fileContent = '\n'.join(fileContent)
            b = pq(fileContent)('img')
            for img in b:
                url = pq(img).attr('src')
                imageArr.append(url)
            jsonObj2['images'] = imageArr

        count = 0
        reviewJson = [] # format is reviewid -> review
        while count < totalpg:
            # create a url
            substr = "or" + str(count * 10) + "-"
            centerpoint = revUrl.find(revStr) + len(revStr)
            secondpoint = revUrl.find(title.split(' ')[0])
            newrevUrl = revUrl[:centerpoint] + substr + revUrl[secondpoint:]
            (fileContent, dwnld) = self.downloadToFile(baseUrl+newrevUrl, newrevUrl)
            if dwnld:
                if not self.analyzeReviewPageModified(hotelid, fileContentStr,  title, count, reviewJson):
                    logging.error("ERROR: Not able to parse number of pages: 1")
            print('review count: ' + str(count))
            count = count + 1
            global max_reviews
            if count == max_reviews:
                break
        return {hotelid: {"details": jsonObj2, "reviews": reviewJson}}
    
    def searchACity(self, city):
        items = city.split(',')
        state = items[1].strip()
        city = items[0].strip()
        print ("1) Searching for the hotels page for ", city," in state ", state)
        urlCity = city.replace(' ','+')
        urlState = state.replace(' ', '+')
        global baseUrl
        citySearchUrl = baseUrl+"Search?q="+urlCity+"%2C+"+urlState+"&sub-search=SEARCH&geo=&returnTo=__2F__"
        fileName = "citySearch_city-"+urlCity+"_state-"+state+".html"

        print("city search url: ", citySearchUrl)
        (searchContents, dwnld) = self.downloadToFile(citySearchUrl,fileName)

        searchContents = '\n'.join(searchContents)
        hotelUrls = []
        if dwnld:
            a = pq(searchContents)
            hotelPageListUrl = pq(a('.srGeoLinks')[0])('a').attr('href')[1:]
            newurl = baseUrl + hotelPageListUrl
            (nextsearchcontents,dwld) = self.downloadToFile(newurl, hotelPageListUrl)
            nextsearchcontents = '\n'.join(nextsearchcontents)
            a = pq(nextsearchcontents)
            numPages = int(pq(pq(a('#pager_bottom')[0])('a')[1]).text())
            count = 0
            while count < numPages:
                # construct url
                # 1. find the pattern.
                substr = re.search(r'g\d+', newurl).group()
                index = newurl.find(substr) + len(substr)
                if count == 0:
                    strToBeAdded = "-"
                else:
                    strToBeAdded = "-oa" + str(count * 30)

                newnewurl = newurl[:index] + strToBeAdded + newurl[index:]
                (searchContents, dwnld) = self.downloadToFile(newnewurl, newnewurl[newnewurl.find(baseUrl) + len(baseUrl):])
                searchContents = '\n'.join(searchContents)
                hotelUrls = hotelUrls + self.getCityHotelListPage(searchContents)
                count = count + 1

        #Step 2: Get the list of hotels page for each
        #hotelPages = getCityHotelListPage(searchContents)
        hotelcount = 0
        resultJson = {}
        while hotelcount < len(hotelUrls):
            #Step 4: Get the page for each hotel
            hUrl = hotelUrls[hotelcount]
            print("checking for url: ", hotelcount, " : ", hUrl)
    #        if not checkIfExists(hUrl):
    #            print("5) Getting reviews for hotel ", hUrl)
    #            if getTAReviewsForHotel(hUrl,city, key):
    #                successfulResults += 1
            print("5) Getting reviews for hotel ", hUrl)
            hotelJson = self.getTAReviewsForHotel(hUrl,city)
            if hotelJson != {}:
                for key in hotelJson:
                    resultJson[key] = hotelJson[key]
            print('hotel count: ' + str(hotelcount))
            hotelcount = hotelcount + 1
            global max_hotels
            if hotelcount == max_hotels:
                break
        locationKey = self.createKey(urlCity, urlState)
        return locationKey, resultJson
    
    def mapper(self, _, line):
        key, value = self.searchACity(line)
        yield (key, value)

if __name__ == '__main__':
    MRWordFreqCount.run()