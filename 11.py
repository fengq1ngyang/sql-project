import requests
from bs4 import BeautifulSoup
import asyncio
import time
import aiohttp

async def main():
  cookies = {
    '__utmc': '223695111',
    '__utmz': '223695111.1681120605.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
    '__yadk_uid': 'MaUoRDnhGIBY6a7hI48TMvg12Y9qIWU4',
    '_vwo_uuid_v2': 'D876CF8FD312919DDE6805B888952462E|abd7a2ce8f4178a6deab1d571e82a520',
    'bid': 'XvbP236PHdY',
    'll': '118286',
    '_pk_id.100001.4cf6': '6f0832ab96e194dc.1681120604.2.1681173165.1681120841.',
    '_pk_ses.100001.4cf6': '*',
    'ap_v': '0,6.0',
    '__utma': '30149280.2106855020.1681173165.1681173165.1681173165.1',
    '__utmb': '30149280.0.10.1681173165',
    '__gads': 'ID=35583760ae468f52-2254e4f02add0089:T=1681173165:RT=1681173165:S=ALNI_MbTrQ_4zT_qUef2ogWrrgFoWhcFPQ',
    '__gpi': 'UID=00000bf263b4d497:T=1681173165:RT=1681173165:S=ALNI_MZH9MfELwVauVnqbO5UkoLH7pw_Bg',
  }

  headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.62',
    'sec-ch-ua': '"Microsoft Edge";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
  }

  session = aiohttp.ClientSession()

  # 豆瓣电影详情页面的url
  url = 'https://movie.douban.com/subject/1292052/'
  async with session.get(url, headers=headers, cookies=cookies) as resp:
    print(await resp.text())


tasks = [asyncio.ensure_future(main()) for _ in range(1)]
loop = asyncio.get_event_loop()
loop.run_until_complete(asyncio.wait(tasks))
print('hello world')