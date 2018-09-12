import re
import time
import json
import requests
from urllib import parse
import threading
from queue import Queue
import pymysql
from fake_useragent import UserAgent

class Producer(threading.Thread):
    def __init__(self, page_queue, data_queue, *args, **kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.data_queue = data_queue
        self.page_queue = page_queue
        self.headers = {
                'referer': 'https://www.agoda.com/zh-cn/city/guangzhou-cn.html?cid=10112',
                'user-agent': UserAgent().random
            }
        self.PROXY_POOL_URL = 'http://localhost:5555/random'

    def run(self):
        while True:
            if self.page_queue.empty():
                break
            self.request_url(self.page_queue.get())

    def get_ID(self):
        url = "https://www.agoda.com/zh-cn/pages/agoda/default/DestinationSearchResult.aspx?city=10112"
        proxy = {
            "http": self.get_proxy()
        }
        response = requests.get(url, headers=self.headers, proxies=proxy)
        try:
            if response.status_code == 200:
                SearchMessageID = re.findall('.*"SearchCriteria":{"SearchMessageID":"(.*?)".*', response.text, re.S)
                SearchID = re.findall('.*"CurrentDate".*,"SearchID":(.*?),.*', response.text, re.S)
                return SearchMessageID[0], int(SearchID[0])

        except Exception as e:
            print(e)

    def get_proxy(self):
        try:
            response = requests.get(self.PROXY_POOL_URL)
            if response.status_code == 200:
                proxy = response.text
                print("使用代理：%s" % proxy)
                return proxy
        except ConnectionError:
            return None

    def request_url(self, page):
        print("开始爬取page：%s" % page)
        url = 'https://www.agoda.com/api/zh-cn/Main/GetSearchResultList'
        SearchMessageID, SearchID = self.get_ID()
        formdata = {
            "SearchMessageID": SearchMessageID,
            "SearchType": 1,
            "ObjectID": 0,
            "TotalHotels": 0,
            "SearchID": SearchID,
            "CityId": 10112,
            "PageNumber": page,
            "PageSize": 45,
            "CountryName": "China",
            "CountryId": 191,
            "TotalHotelsFormatted": "0",
            "Cid": -1,
            "LengthOfStay": 1,
        }
        proxy = {
            "http": self.get_proxy()
        }
        response = requests.post(url, data=formdata, headers=self.headers, proxies=proxy)
        try:
            if response.status_code == 200:
                text_json = json.loads(response.text)  # 字典str -> json
                self.parse_json(text_json, url)
                print("已爬取page：%s" % page)
        except ConnectionError:
            return None

    def parse_json(self, response, url):
        for item in response.get('ResultList'):
            HotelID = item.get('HotelID')
            SupplierId = item.get('SupplierId')
            EnglishHotelName = item.get('EnglishHotelName')
            TranslatedHotelName = item.get('TranslatedHotelName')
            urgencyMessages = ' '.join([i.get('text') for i in item.get('urgencyMessages')]) \
                if item.get('urgencyMessages') else None
            AreaName = item.get('AreaName')
            CityName = item.get('CityName')
            LocationHighlight = item.get('LocationHighlight')
            AreaId = item.get('AreaId')
            CityId = item.get('CityId')
            CountryName = item.get('CountryName')
            CountryId = item.get('CountryId')
            AwardYear = item.get('AwardYear')
            GcaTooltipText = item.get('GcaTooltipText')
            StarRating = item.get('StarRating')
            StarRatingColor = item.get('StarRatingColor')
            ReviewScore = item.get('ReviewScore')
            NumberOfReview = item.get('NumberOfReview')
            FreeWifi = item.get('FreeWifi')
            DisplayPrice = item.get('DisplayPrice')
            CrossOutPrice = item.get('CrossOutPrice')
            DisplayCurrency = item.get('DisplayCurrency')
            ReviewText = item.get('ReviewText')
            HotelUrl = parse.urljoin(url, item.get('HotelUrl'))
            Latitude = item.get('Latitude')
            Longitude = item.get('Longitude')
            DistanceWithBracket = item.get('DistanceWithBracket')
            guestRecommended = item.get('guestRecommended').get('text') if item.get('guestRecommended') else None
            self.data_queue.put((HotelID, SupplierId, HotelUrl, EnglishHotelName, TranslatedHotelName,
                                 DisplayPrice, CrossOutPrice, DisplayCurrency, DistanceWithBracket,
                                 urgencyMessages, AreaName, CityName, LocationHighlight, AreaId,
                                 CityId, CountryName, CountryId, AwardYear, GcaTooltipText, StarRating,
                                 StarRatingColor, ReviewScore, NumberOfReview, FreeWifi, ReviewText,
                                 Latitude, Longitude, guestRecommended))

class Consumer(threading.Thread):
    def __init__(self, page_queue, data_queue, *args, **kwargs):
        super(Consumer, self).__init__(*args, **kwargs)
        self.data_queue = data_queue
        self.page_queue = page_queue

    def run(self):
        while True:
            if self.page_queue.empty() and self.data_queue.empty():
                break
            self.save2mysql()

    def save2mysql(self):
        print("进入写入模式")
        conn = pymysql.connect(
            host="127.0.0.1",
            user="root",
            password="104758993",
            database="spider",
            port=3306,
        )

        try:
            print('正在写入MySQL')
            insert_sql = """
                    insert into Agoda_Guangzhou(HotelID, SupplierId, HotelUrl, EnglishHotelName, TranslatedHotelName,
                                                DisplayPrice, CrossOutPrice, DisplayCurrency, DistanceWithBracket, 
                                                urgencyMessages, AreaName, CityName, LocationHighlight, AreaId,
                                                CityId, CountryName, CountryId, AwardYear, GcaTooltipText, StarRating,
                                                StarRatingColor, ReviewScore, NumberOfReview, FreeWifi, ReviewText,
                                                Latitude, Longitude, guestRecommended)
                                values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                              """
            params = self.data_queue.get()

            cursor = conn.cursor()
            cursor.execute(insert_sql, params)
            conn.commit()
            print('写入MySQL完成')
            conn.close()

        except Exception as e:
            print(e)
            conn.rollback()

# def get_page():
#     response = requests.get()

def main():
    data_queue = Queue()
    page_queue = Queue()
    # Thread_list = []  # 保存线程

    for i in range(1, 35):
        page_queue.put(i)

    for i in range(5):
        t = Producer(page_queue, data_queue)
        t.start()
        # Thread_list.append(t)

    for i in range(5):
        t = Consumer(page_queue, data_queue)
        t.start()
        # Thread_list.append(t)

    # for t in Thread_list:
    #     t.join()        # 让主线程等待子线程执行完成

if __name__ == '__main__':
    main()


