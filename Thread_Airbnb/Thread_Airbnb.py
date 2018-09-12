# 多线程爬取Airbnb成都地区的旅馆，并存入MongoDB

import re
import time
import random
import requests
import urllib3
import threading
import pymysql
from queue import Queue
from scrapy.selector import Selector
from Selenium_Airbnb import get_listing_id
import pymongo
conn = pymysql.connect(host="127.0.0.1", user='root', password='104758993',
                       database='spider', port=3306, charset="utf8")
cursor = conn.cursor()

class Producer(threading.Thread):
    def __init__(self, proxy, id_queue, url_queue, data_queue, headers, comment_url, *args, **kwargs):
        super(Producer, self).__init__(*args, **kwargs)
        self.proxy = proxy
        self.id_queue = id_queue
        self.url_queue = url_queue
        self.data_queue = data_queue
        self.headers = headers
        self.comment_url = comment_url
        self.month_fee_url = "https://www.airbnb.cn/api/v2/calendar_months?_format=" \
                             "with_conditions&count=4&listing_id={0}&month=7&year=2018" \
                             "&key=d306zoyjsyarp7ifhu67rjxn52tv0t20&currency=CNY&locale=zh"

    def run(self):
        while True:
            if self.url_queue.empty():
                break
            self.parse_detail(self.url_queue.get())

    def parse_detail(self, url):
        urllib3.disable_warnings()
        proxy = {
            'http': ''
        }
        proxy = proxy.update({'http': self.proxy[random.randint(1, 9)]})
        response = requests.get(url, headers=self.headers, proxies=proxy, verify=False)
        Hotel_tags = '-'.join(Selector(text=response.text).xpath('//span[@class="_ju40xgb"]/span/text()').extract())
        Hotel_title = Selector(text=response.text).xpath('//div[@class="_18gim6s4"]/h1/text()').extract()
        if Hotel_title:
            Hotel_title = Hotel_title[0]
        else:
            Hotel_title = ''
        Hotel_facilities_traffic = Selector(text=response.text).xpath('//div[@class="_1thk0tsb"]/span/text()').extract()
        Hotel_facilities = '-'.join([item for item in Hotel_facilities_traffic[:4]])
        Hotel_traffic = '-'.join([item for item in Hotel_facilities_traffic[-3:]])

        Hotel_location = Selector(text=response.text).xpath('//div[@class="_190019zr"]/text()').extract()
        if Hotel_location:
            Hotel_location = Hotel_location[0].strip().replace('、', '-')
        else:
            Hotel_location = ''
        Hotel_addr = Selector(text=response.text).xpath('//div[@id="location"]/div/div/div/section/div[3]/div/div/div/div/p/span/span/text()')
        if Hotel_addr:
            Hotel_addr = Hotel_addr.extract()[0].strip()
        else:
            Hotel_addr = ''
        Hotel_travel_information = Selector(text=response.text).xpath(
             '//div[@class="_11oyobo"]/div/div/div/p/span/span/text()')
        if Hotel_travel_information:
            Hotel_travel_information = ''.join([item.strip() for item in Hotel_travel_information.extract()])
        else:
            Hotel_travel_information = ''

        Owner_name = Selector(text=response.text).xpath('//div[@class="_1iti0ju"]/span/text()').extract()
        if Owner_name:
            Owner_name = Owner_name[0]
        else:
            Owner_name = ''
        Owner_sign = '-'.join([item.strip() for item in Selector(text=response.text).xpath('//span[@class="_1vzhbuir"]/text()').extract()])
        Owner_info = Owner_name + Owner_sign

        Hotel_desc = ','.join([item.strip() for item in Selector(text=response.text).xpath
                     ('//div[@class="_11abfxr"]/div/p/span/span/text()').extract()])
        Hotel_check_in_time = [item for item in Selector(text=response.text).xpath
                    ('//div[@class="_q401y8m"]/div/span[2]/text()').extract() if item.strip() != '']
        Hotel_check_in_type = ["住房时间", "退房时间"]
        Hotel_check_in_time = list(zip(Hotel_check_in_type, Hotel_check_in_time))

        Hotel_comment_star = Selector(text=response.text).xpath('//div[@itemprop="ratingValue"]/span/@aria-label').extract()
        if Hotel_comment_star:
            Hotel_comment_star = Hotel_comment_star[0].strip()
        else:
            Hotel_comment_star = ''
        Hotel_comment_num = Selector(text=response.text).xpath('//div[@class="_i6dgfcq"]/div/div/span/span/text()').extract()
        if Hotel_comment_num:
            Hotel_comment_num = Hotel_comment_num[0]
        else:
            Hotel_comment_num = ''
        Hotel_score_type = Selector(text=response.text).xpath('//div[@class="_iq8x9is"]/span/text()').extract()
        Hotel_score_type_star = Selector(text=response.text).xpath('//div[@class="_1iu38l3"]/span/@aria-label').extract()
        Hotel_score = list(zip(Hotel_score_type, Hotel_score_type_star))

        # 获取过去月份到当月的价格
        pattern = re.compile('.*?rooms/(.*?)\?', re.S)
        listing_id = re.search(pattern, url).group(1)
        month_fee_url = self.month_fee_url.format(listing_id)
        urllib3.disable_warnings()
        month_fee_response = requests.get(month_fee_url, headers=self.headers, proxies=proxy, verify=False)

        Date_fee = []
        for items in month_fee_response.json().get('calendar_months'):
            for item in items.get('days'):
                date = item.get('price').get('date')
                fee = item.get('price').get('local_price_formatted')
                Date_fee.append((date, fee))

        """
        # 获取评论内容
        def get_reviews_count():
            origin_url = "https://www.airbnb.cn/api/v2/reviews?key=d306zoyjsyarp7ifhu67rjxn52tv0t20" \
                         "&currency=CNY&locale=zh&listing_id={0}&role=guest&_format=for_p3" \
                         "&_limit=7&_offset=0&_order=language_country".format(listing_id)
            urllib3.disable_warnings()
            proxy = {
                'http': ''
            }
            proxy = proxy.update({'http': self.proxy[random.randint(1, 9)]})
            response = requests.get(origin_url, headers=self.headers, proxies=proxy, verify=False)
            reviews_count = response.json().get('metadata').get('reviews_count')
            
            if isinstance(reviews_count / 7, int):
                off_set = (int(reviews_count / 7) - 1) * 7

            else:
                off_set = int(reviews_count / 7) * 7

            return off_set
        def get_comment():
            for x in range(get_reviews_count()+1):
                off_set_list = []
                if isinstance(x/7, int):
                    off_set_list.append(x)
                    for i in off_set_list:
                        comment_url = self.comment_url.format(listing_id, i)
                        print('*'*100)
                        print("我是url：", comment_url)
                        print('*' * 100)
                        urllib3.disable_warnings()
                        comment_response = requests.get(comment_url, headers=self.headers, proxies=proxy, verify=False)
                        print(comment_response.json())
        """
        data = {
            "Owner_info": Owner_info,
            "Hotel_tags": Hotel_tags,
            "Hotel_name": Hotel_title,
            "Hotel_facilities": Hotel_facilities,
            "Hotel_date_fee": Date_fee,
            "Hotel_check_in_time": Hotel_check_in_time,
            "Hotel_desc": Hotel_desc,
            "Hotel_addr": Hotel_addr,
            "Hotel_location": Hotel_location,
            "Hotel_traffic": Hotel_traffic,
            "Hotel_travel_information": Hotel_travel_information,
            "Hotel_score": Hotel_score,
            "Hotel_comment_num": Hotel_comment_num,
            "Hotel_comment_star": Hotel_comment_star,
        }
        print(data)
        self.data_queue.put(data)

# 写入MongoDB
class Consumer(threading.Thread):
    def __init__(self, url_queue, data_queue, *args, **kwargs):
        super(Consumer, self).__init__(*args, **kwargs)
        self.url_queue = url_queue
        self.data_queue = data_queue

    def run(self):
        while True:
            if self.url_queue.empty():
                break
            self.save_into_mongodb(self.data_queue.get())

    def save_into_mongodb(self, data):
        print("--- 正在写入数据 ---")
        client = pymongo.MongoClient(host='127.0.0.1', port=27017)
        Hotel_info = client['Hotel_info']  # 给数据库命名
        Chengdu_collection = Hotel_info['Chengdu_collection']  # 创建数据表
        Chengdu_collection.insert_one(data)
        print("--- 写入完成 ---")

def main():
    cookie, x_csrf_token, id_queue, url_queue = get_listing_id()
    data_queue = Queue()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/68.0.3440.106 Safari/537.36",
        "Host": "www.airbnb.cn",
        "origin": "https://www.airbnb.cn",
        "cookie": cookie,
        "x-csrf-token": x_csrf_token,
    }
    comment_url = "https://www.airbnb.cn/api/v2/reviews?key=d306zoyjsyarp7ifhu67rjxn52tv0t20" \
                  "&currency=CNY&locale=zh&listing_id={0}&role=guest&_format=for_p3&_limit=7" \
                  "&_offset={1}&_order=language_country"

    proxy = []
    def get_proxy():
        select_sql = "select ip,port from proxy_ip order by rand() limit 1"
        cursor.execute(select_sql)

        for ip_info in cursor.fetchall():
            ip = ip_info[0]
            port = ip_info[1]
            judge_result = judge_proxy(ip, port)
            if judge_result:
                return "{0}:{1}".format(ip, port)

    def judge_proxy(ip, port):
        proxy_ip = "http://{0}:{1}".format(ip, port)
        http_url = "https://www.baidu.com"

        proxy = {
            "http": proxy_ip,
        }
        response = requests.get(url=http_url, proxies=proxy)
        if response.status_code >= 200 and response.status_code < 300:
            print("effective ip")
            return True
        else:
            print("invalid ip and port")
            return get_proxy()

    for i in range(10):
        proxy.append(get_proxy())

    for x in range(5):
            t = Producer(proxy, id_queue, url_queue, data_queue, headers, comment_url)
            t.start()

    for x in range(5):
            t = Consumer(url_queue, data_queue)
            t.start()

if __name__ == '__main__':
    main()































