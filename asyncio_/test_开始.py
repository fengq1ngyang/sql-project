import asyncio

async def execute(x):
    print('Number:', x)

# 返回了一个 coroutine 协程对象
coroutine = execute(1)
print('Coroutine:', coroutine)
print('After calling execute')

# 创建了一个事件循环 loop
loop = asyncio.get_event_loop()
# 将协程注册到事件循环 loop 中，然后启动。
#  run_until_complete 方法将 coroutine 隐式封装成了 task（任务） 对象
loop.run_until_complete(coroutine)
print('After calling loop')