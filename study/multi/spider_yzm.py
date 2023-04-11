import requests
import threading
import time
from tqdm import tqdm
import json
"""

因为在关于变量的操作时，Python会在变量上加GIL锁，所以只会有一个线程能够访问到所有变量

GIL锁会限制线程对于共享变量（全局变量）的读取，因为每一个线程都需要获得GIL才能执行。但是，并不会限制线程对于本地变量的读取。


所以多线程不适用于关于程序中变量的修改操作，或者计算操作

多线程只适用于 读取文件，发送http请求，写入文件等
不涉及计算的任务

"""


def download_yzm(url):
    response = requests.get(url)
    with open('./img1/'+str(time.time()).split(".")[0]+'.jfif','wb') as file:
        file.write(json.dumps(response.headers))

if __name__ == '__main__':
    start = time.time()
    url = 'https://gd.tzxm.gov.cn/tzxmspweb/captcha?type=hutool&timer1679476300589'
    threads = []
    for index in  (range(500)):
        thread = threads.append(
            threading.Thread(target=download_yzm,args=(url,))
        )

    for thread in tqdm(threads):
        thread.start()
    
    for thread in tqdm(threads):
        thread.join()
    end = time.time()

    print("执行完成:::::::::::::: {}".format(end-start))