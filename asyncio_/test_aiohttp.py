import asyncio
import time

import aiohttp


def task(number):
    start = time.time()

    async def get():
        url = 'https://httpbin.org/delay/5'
        session = aiohttp.ClientSession()
        await session.get(url)

    # 将async coroutine 协程对象包装城task对象，task对象就是任务对象，包含运行状态，
    tasks = [asyncio.ensure_future(get()) for _ in range(number)]
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.wait(tasks))
    end = time.time()

    print(end - start )

if __name__ == '__main__':
    task(10)