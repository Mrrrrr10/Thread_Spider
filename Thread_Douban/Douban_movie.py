from threading import Thread
from queue import Queue
import time
from lxml import etree
import requests


class DouBanSpider(Thread):
    def __init__(self, url, q):
        # 重写写父类的__init__方法
        super(DouBanSpider, self).__init__()
        self.url = url
        self.q = q
        self.headers = {
            'Cookie': 'll="118282"; bid=ctyiEarSLfw; ps=y; __yadk_uid=0Sr85yZ9d4bEeLKhv4w3695OFOPoedzC; dbcl2="155150959:OEu4dds1G1o"; as="https://sec.douban.com/b?r=https%3A%2F%2Fbook.douban.com%2F"; ck=fTrQ; _pk_id.100001.4cf6=c86baf05e448fb8d.1506160776.3.1507290432.1507283501.; _pk_ses.100001.4cf6=*; __utma=30149280.1633528206.1506160772.1507283346.1507290433.3; __utmb=30149280.0.10.1507290433; __utmc=30149280; __utmz=30149280.1506160772.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); __utma=223695111.1475767059.1506160772.1507283346.1507290433.3; __utmb=223695111.0.10.1507290433; __utmc=223695111; __utmz=223695111.1506160772.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); push_noty_num=0; push_doumail_num=0',
            'Host': 'movie.douban.com',
            'Referer': 'https://movie.douban.com/top250?start=225&filter=',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.104 Safari/537.36',
        }

    def run(self):
        self.parse_page()

    def send_request(self, url):
        '''
        用来发送请求的方法
        :return: 返回网页源码
        '''
        # 请求出错时，重复请求３次,
        i = 0
        while i <= 3:
            try:
                print(u"[INFO]请求url:" + url)
                html = requests.get(url=url, headers=self.headers).content
            except Exception as e:
                print(u'[INFO] %s%s' % (e, url))
                i += 1
            else:
                return html

    def parse_page(self):
        '''
        解析网站源码，并采用ｘｐａｔｈ提取　电影名称和平分放到队列中
        :return:
        '''
        response = self.send_request(self.url)
        html = etree.HTML(response)
        # 　获取到一页的电影数据
        node_list = html.xpath("//div[@class='info']")
        for move in node_list:
            # 电影名称
            title = move.xpath('.//a/span/text()')[0]
            # 评分
            score = move.xpath('.//div[@class="bd"]//span[@class="rating_num"]/text()')[0]

            # 将每一部电影的名称跟评分加入到队列
            self.q.put(score + "\t" + title)


def main():
    # 创建一个队列用来保存进程获取到的数据
    q = Queue()
    base_url = 'https://movie.douban.com/top250?start='
    # 构造所有ｕｒｌ
    url_list = [base_url + str(num) for num in range(0, 225 + 1, 25)]

    # 保存线程
    Thread_list = []

    # 创建并启动线程
    for url in url_list:
        p = DouBanSpider(url, q)
        p.start()
        Thread_list.append(p)

    # 让主线程等待子线程执行完成
    for i in Thread_list:
        i.join()

    while not q.empty():
        print(q.get())


if __name__ == "__main__":
    start = time.time()
    main()
    print('[info]耗时：%s' % (time.time() - start))