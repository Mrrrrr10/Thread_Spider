from pyecharts import Map
import pymysql

def area_count_analysis():
    count = []
    count_haizhu = []
    count_liwan = []
    count_baiyun = []
    count_yuexiu = []
    count_tianhe = []
    areananme = ["南沙区", "增城区",
                    "从化区", "番禺区", "花都区", "黄埔区", "海珠区", "荔湾区", "白云区", "越秀区", "天河区"]
    cursor.execute("SELECT AreaName, count(AreaName) FROM `agoda_guangzhou` where AreaName is not NULL GROUP BY AreaName")
    data = cursor.fetchall()  # 返回值是多个元组
    for t in data:
        if "白云" in t[0]:
            count_baiyun.append(t[1])
        elif "番禺" in t[0]:
            count.append(t[1])
        elif "南沙" in t[0]:
            count.append(t[1])
        elif "花都" in t[0]:
            count.append(t[1])
        elif "从化" in t[0]:
            count.append(t[1])
        elif "增城" in t[0]:
            count.append(t[1])
        elif "越秀" in t[0]:
            count_yuexiu.append(t[1])
        elif "天河" in t[0]:
            count_tianhe.append(t[1])
        elif "黄埔" in t[0]:
            count.append(t[1])
        # elif "南海" in t[0]:
        #     count.append(t[1])
        elif "海珠" in t[0]:
            count_haizhu.append(t[1])
        elif "琶洲" in t[0]:
            count_haizhu.append(t[1])
        elif "荔湾" in t[0]:
            count_liwan.append(t[1])
        elif "芳村" in t[0]:
            count_liwan.append(t[1])
    conn.close()

    def sum_elems(l):
        sum = 0
        for i in l:
            sum += i
        return sum

    count.append(sum_elems(count_haizhu))
    count.append(sum_elems(count_liwan))
    count.append(sum_elems(count_baiyun))
    count.append(sum_elems(count_yuexiu))
    count.append(sum_elems(count_tianhe))

    ma = Map("广州酒店数量分布地图", width=1200, height=600)
    ma.add('', areananme, count, maptype="广州", is_visualmap=True, is_label_show=True, visual_text_color='#000')
    ma.render(path='C:\\Users\\10475\Desktop\Spider_Project\Scrapy_Project\\agoda_area.html')

if __name__ == '__main__':
    conn = pymysql.connect(
        host="127.0.0.1",
        user='root',
        password='104758993',
        database='spider',
        port=3306
    )
    cursor = conn.cursor()
    area_count_analysis()