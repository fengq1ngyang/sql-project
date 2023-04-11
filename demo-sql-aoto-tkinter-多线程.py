# -*- coding:utf-8 -*-
import tkinter
from tkinter import *
from tkinter import ttk
import psycopg2
import asyncio,time,threading
from tkinter import messagebox
from tkinter.ttk import Treeview,Style

# sql-
sql = """
with temp1 as (
SELECT
    *,
    dense_rank() over(partition by "NAME" order by "ROWCOUNT" desc ) time_rank
FROM
    dw.table_rows_check
WHERE
    case "NAME"
    when 'spgl_ccp_proj_construction_permit' then 1=1
    when 'spgl_ccp_proj_info' then 1=1
    when 'spgl_ccp_proj_unit_info' then 1=1
    when 'spgl_ccp_proj_unit_linkman_info' then 1=1
    when 'spgl_地方项目审批流程阶段事项信息表' then 1=1
    when 'spgl_项目其他附件信息表' then 1=1
    when 'qt_市场主体基本信息' then 1=1
    when 'sheng_建筑工程施工许可证' then 1=1
    end
    and DATE_PART('day', now() - table_rows_check.create_time) <=7
)
SELECT id,"ROWCOUNT","NAME" ,create_time,table_id,table_comments,time_rank from temp1 ;
"""


class Window:
    def __init__(self):
        self.win = tkinter.Tk()  # 窗口
        self.win.title('SQL查询')  # 标题
        self.screenwidth = self.win.winfo_screenwidth()  # 屏幕宽度
        self.screenheight = self.win.winfo_screenheight()  # 屏幕高度
        self.width = 1000
        self.height = 500
        self.x = int((self.screenwidth - self.width) / 2)
        self.y = int((self.screenheight - self.height) / 2)
        self.win.geometry('{}x{}+{}+{}'.format(self.width, self.height, self.x, self.y))  # 大小以及位置

        tabel_frame = tkinter.Frame(self.win)
        tabel_frame.pack()
        xscroll = Scrollbar(tabel_frame, orient=HORIZONTAL)
        yscroll = Scrollbar(tabel_frame, orient=VERTICAL)

        self.columns = ['id', '"NAME"', 'ROWCOUNT', 'create_time', 'table_id', 'table_comments','time_rank']
        self.table = ttk.Treeview(
            master=tabel_frame,  # 父容器
            height=20,  # 表格显示的行数,height行
            columns=self.columns,  # 显示的列
            show='headings',  # 隐藏首列
            xscrollcommand=xscroll.set,  # x轴滚动条
            yscrollcommand=yscroll.set,  # y轴滚动条

        )

        style = Style()
        # 帮助tag_configure渲染表格，不然tag_configure不生效
        def fixed_map(option):
            return [elm for elm in style.map("Treeview", query_opt=option)
                    if elm[:2] != ("!disabled", "!selected")]
        style.map(
            'Treeview',
            foreground=fixed_map('foreground'),
            background=fixed_map('background')
        )
        # 
        self.table.tag_configure('tag_red', background='red', foreground='white')
        self.table.tag_configure('tag_normal', background='white', foreground='black')

        for column in self.columns:
            self.table.heading(column=column, text=column, anchor=CENTER,
                          command=lambda name=column:
                          messagebox.showinfo('', '{}'.format(name)))  # 定义表头
            self.table.column(column=column, width=100, minwidth=100, anchor=CENTER)  # 定义列

        xscroll.config(command=self.table.xview)
        xscroll.pack(side=BOTTOM, fill=X)
        yscroll.config(command=self.table.yview)
        yscroll.pack(side=RIGHT, fill=Y)
        self.table.pack(fill=BOTH, expand=True)

        # 调用异步方法
        self.change_form_state()

        f = Frame()
        f.pack()
        Button(f, text='更新', bg='yellow', width=20, command=self.conn_executeSQL_insertList).pack(side=LEFT)
        Button(f, text='清空', bg='pink', width=20, command=self.delete).pack(side=LEFT)
        self.win.mainloop()

    """
        删除列表数据
    """
    def delete(self):
        obj = self.table.get_children()  # 获取所有对象
        for o in obj:
            self.table.delete(o)  # 删除对象
    """
        执行SQL语句，插入到列表
    """
    def conn_executeSQL_insertList(self):

        # 创建连接对象
        conn = psycopg2.connect(database="fsdrm", user="hehongquan", password="yhlz@123", host="172.16.0.127",port="5432")
        cur = conn.cursor()  # 创建指针对象

        # 获取结果
        cur.execute(sql)
        results = cur.fetchall()
        tag_flag = []
        num = 0
        for index, data in enumerate(results):
            if data[6] == 1:
                num +=1
            if index%8==7:
                tag_flag.append(num)
                num = 0
        print(tag_flag)
        for index, data in enumerate(results):
            # 每八条如果1的数量超过3,1显示为红色tag_red，否则tag_normal
            tag = 'tag_red' if tag_flag[int(index/8)] > 3 else 'tag_normal'
            self.table.insert('', END, values=data,tags=tag)  # 添加数据到末尾

        # 关闭连接
        conn.commit()
        cur.close()
        conn.close()

    """
        后台任务进程
    """
    async def action_SQL(self):
        while True:
            time_now = time.strftime("%H:%M:%S", time.localtime())  # 刷新
            if time_now == "14:12:20":  # 此处设置每天定时的时间
                # 此处为需要执行的动作
                # 连接数据库，执行SQL查询
                print(3)
                self.conn_executeSQL_insertList()
                time.sleep(2)  # 因为以秒定时，所以暂停2秒，使之不会在1秒内执行多次

    def get_loop(self, loop):
        self.loop = loop
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def change_form_state(self):
        coroutine1 = self.action_SQL()
        new_loop = asyncio.new_event_loop()  # 在当前线程下创建时间循环，（未启用），在start_loop里面启动它
        t = threading.Thread(target=self.get_loop, args=(new_loop,))  # 通过当前线程开启新的线程去启动事件循环
        t.start()

        asyncio.run_coroutine_threadsafe(coroutine1, new_loop)  # 这几个是关键，代表在新线程中事件循环不断“游走”执行


if __name__ == '__main__':

    window = Window()




