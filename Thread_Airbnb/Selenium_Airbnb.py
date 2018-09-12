# selenium获取Airbnb成都地区的listing_id

import re
import time
from queue import Queue
from selenium import webdriver
from scrapy.selector import Selector
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def get_listing_id():
    id_queue = Queue()
    url_queue = Queue()
    url = "https://www.airbnb.cn/s/Chengdu-Shi/homes?refinement_paths%5B%5D=%2Fhomes&" \
                       "allow_override%5B%5D=&s_tag=wSycEjLT"

    # 设置Chromedriver不加载图片
    chrome_opt = webdriver.ChromeOptions()
    prefs = {
        "profile.managed_default_content_settings.images": 2  # 2为不加载图片
    }
    chrome_opt.add_experimental_option("prefs", prefs)
    browser = webdriver.Chrome(executable_path="A:/PythonVirtualenv/spider-env/chromedriver.exe",
                               chrome_options=chrome_opt)
    browser.get(url)
    cookie = ';'.join(['{0}={1}'.format(item.get('name'), item.get('value')) for item in browser.get_cookies()])
    x_csrf_token = cookie[6]

    while True:
        # 下拉动作
        for i in range(2):
            WebDriverWait(driver=browser, timeout=10).until(
                EC.presence_of_element_located((By.XPATH, '//div[@class="_190019zr"]')))
            browser.execute_script("window.scrollTo(0, document.body.scrollHeight); "
                                    "var lenOfPage=document.body.scrollHeight; "
                                    "return lenOfPage")

        selector = Selector(text=browser.page_source)
        next_btn = browser.find_element_by_xpath('//li[@class="_b8vexar"]/a/div')
        if '_1yofwd5' in next_btn.get_attribute("class"):
            hotel_urls = selector.xpath('//div[@itemprop="itemListElement"]/meta[3]/@content').extract()
            for hotel_url in hotel_urls:
                hotel_url = 'https://' + hotel_url
                pattern = re.compile(".*?rooms/(.*?)\?")
                listing_id = re.search(pattern, hotel_url).group(1)
                id_queue.put(listing_id)
                url_queue.put(hotel_url)
            time.sleep(2)
            browser.execute_script("arguments[0].scrollIntoView(true);", next_btn)
            next_btn.click()
        else:
            break

    print("完成任务时队列是否为空:", id_queue.empty())
    return cookie, x_csrf_token, id_queue, url_queue


if __name__ == '__main__':
    get_listing_id()













































