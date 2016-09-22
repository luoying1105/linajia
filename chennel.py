from bs4 import BeautifulSoup
import requests
from time import sleep
from random import randint
import pymongo

client = pymongo.MongoClient('localhost', 27017, connect=False)
cheshi = client['lianjia_project']
url_list = cheshi['url_list']

start_url = 'http://bj.lianjia.com/'

"""
获取所有城市链接
录入mongodb
"""


def get_channel_urls(url):
    web_data = requests.get(start_url)
    web_data.encoding = web_data.apparent_encoding  # 解决中文乱码问题
    soup = BeautifulSoup(web_data.text, "lxml")
    links = soup.select('.citys-l > ul > li > div > a')
    for link in links:
        page_url = str(link.get('href'))
        page_name = str(link.get_text())
        sleep(randint(3, 8))
        city_class_url = get_city_urlls(page_url)
        url_list.insert_one({'city_name': page_name, 'city_all_url': page_url, 'city_extract_url': city_class_url})
        print(page_name, page_url, city_class_url)


# categorys-2014 > div:nth-child(2) > div:nth-child(2)
# categorys-2014 > div:nth-child(2) > div:nth-child(2)
def get_city_urlls(url):
    web_data = requests.get(start_url)
    web_data.encoding = web_data.apparent_encoding  # 解决中文乱码问题
    soup = BeautifulSoup(web_data.text, "lxml")
    links = soup.select('.nav > ul > li > a')
    city_class_url =set()
    ershoufang_url = str(soup.select('.nav > ul > li > a'))
    xinfang_url = str()
    xuequfang_url = str()
    zhufang_url = set()
    for link in links:
        page_url = str(link.get('href'))
        page_name = str(link.get_text())
        if page_name == '二手房':
            ershoufang_url = page_url
        if '学区' in page_name:
            if 'home/' in page_name:#方便查询删除主页面
                print('有主页',page_name,page_url)
                xuequfang_url =page_url.replace('home/','')
            xuequfang_url = page_url.replace('home/', '')
        if '新' in page_name:
            xinfang_url = page_url
        if page_name == '租房':
            zhufang_url = page_url

    city_class_url=({'二手房':ershoufang_url,'租房':zhufang_url,'学区房':xuequfang_url,'新房':xinfang_url})
    return  city_class_url


get_channel_urls(start_url)

