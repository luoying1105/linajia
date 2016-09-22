from bs4 import BeautifulSoup
import requests
from time import sleep
from random import randint
import pymongo
from multiprocessing import Pool

client = pymongo.MongoClient('localhost', 27017, connect=False)
cheshi = client['lianjia_project']
url_list = cheshi['url_list']
"""
商品详情分两个数据库
第一个数据库只用于存放抓取的url 第二个存放商品详细信息
写入第二个数据库同时 增加index url 存放第一个数据库详情对应链接
"""
info_url = cheshi['info_url']
city_info = cheshi['city_info']


def get_xinfang_url():
    url_host = set()
    url_host = map(lambda x: x, url_list.find())  # 读有多少个城市
    xinfang_url = set()
    for i in url_host:
        lists = i['city_extract_url']
        city_before_url = i['city_all_url']
        city_name = i['city_name']
        xinfang = lists['新房']
        print(city_name, xinfang)
        xinfang_url.add(
            {'新房': xinfang + 'loupan',
             'before_url': '',
             'city_name': city_name,
             'class': '新房',

             }
        )

    return xinfang_url


def get_city_url():
    # 获取数据库中获取城市链接
    url_host = set()
    url_host = map(lambda x: x, url_list.find())  # 读有多少个城市
    for i in url_host:
        lists = i['city_extract_url']
        city_before_url = i['city_all_url']
        city_name = i['city_name']
        zufang = lists['租房']
        ershoufang = lists['二手房']
        xuequfang = lists['学区房']
        xinfang = lists['新房']
        # for url in xinfang:
        #     for i in range(1,100):
        #         get_city_are_url(url,city_name,city_before_url,'新房',i)
        print(city_name, xinfang)

        for i in range(1, 100):
            get_city_are_url(xinfang, '', city_name, '新房', i)


def get_city_are_url(url, before_url, city_name, chennel, pages):
    sleep(randint(2, 4))
    web_data = requests.get(url + 'loupan/')
    web_data.encoding = web_data.apparent_encoding  # 解决中文乱码问题
    soup = BeautifulSoup(web_data.text, 'lxml')
    no_longer = soup.select('.list-no-data > p')
    nolonger_exist = '没有找到相关内容，请您换个条件试试吧~' in str(no_longer).split()
    if nolonger_exist:
        print('没有找到相关内容，请您换个条件试试吧~')
        pass
    else:
        links = soup.select('dl.dl-lst > dd > div > a')
        for link in links:
            city_location = link.get_text()
            url_1 = url + str(link.get('href')) + '/pg{}'.format(str(pages))
            print('url' + url_1)
            chnneal_data = get_channel_urls(url_1, city_name, city_location)

            print(city_name, city_location, chennel, url_1, chnneal_data)


def get_channel_urls(url, city_name, city_location):
    sleep(randint(2, 5))
    web_data = requests.get(url)
    web_data.encoding = web_data.apparent_encoding  # 解决中文乱码问题
    soup = BeautifulSoup(web_data.text, 'lxml')
    titles = soup.select('#house-lst > li > div > div > h2 > a')
    links = soup.select('#house-lst > li > div > div > h2 > a')
    prices = soup.select('#house-lst > li > div > div > div > div > span')
    areas = soup.select('#house-lst > li > div > div > div')
    locations = soup.select('#house-lst > li > div > div > div > span')

    for title, link, price, location, area in zip(titles, links, prices, locations, areas):
        info_url.insert_one({'url': str(link.get('href'))})
        city_info.insert_one(
            {

                'url': str(link.get('href')),
                'city': city_name,
                'location': city_location,
                'title': str(title.get_text()),
                'prices': str(price.get_text()),
                'loc_areas': str(location.get_text()).split(),
                'special': str(area.get_text()).split()
            }
        )

        print(str(title.get_text()), str(link.get('href')))



# get_city_url()
# get_channel_urls('http://bj.fang.lianjia.com//pg1 ')
def get_info():
    for i in range(1, 100):
        get_city_are_url('http://bj.fang.lianjia.com/', '', '北京', '新房', i)
    db_url = [item['url'] for item in info_url.find()]
    indexs_url = [item['url'] for item in city_info.find()]
    rest_url = set(db_url) - set(indexs_url)
#数据去重
def data_url_quchong():
    db_url = [item['url'] for item in info_url.find()]
    indexs_url = [item['url'] for item in city_info.find()]
    rest_url = set(db_url) - set(indexs_url)
    url_host = map(lambda x: x, city_info.find())
    data_remove = set()
    for i in url_host:
        for url in indexs_url:
           if url['url'] in i['url']:
               data_remove.add(i)


if __name__ == '__main__':
    pool = Pool()
    pool.map(get_info())
