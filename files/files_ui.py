# !/usr/bin/python 3.6.5
# -*- coding:utf-8 -*-


__author__ = {
    "name": "ZhuHaifang",
    "email": "1159878350@qq.com",
    "data": "2018-12-11 23:03:30",
    "project": "Python 串口工具",
    "version": " V1.0"
}


from tkinter import *
from tkinter import ttk
# import SerialPort

# 下面三个库为个人私有库，可自己定义替换对相关内容进行修改
# from PySerialPortDataBase import PYSP_DB
# from PySerialPortDataBase import PYSP_WIN
# from PySerialPort import PYSP_LG


class Windows:
    def __init__(self, master, width=690, height=490):
        master.title(__author__["project"] + __author__["version"])
        master.geometry(str(width) + "x" + str(height))
        master.resizable(width=False, height=False)   # 窗口大小是否可变

        self.Menu_Init(master)
        self.Tool_Boxs(master, width)
        self.SerialSet_Init(master)
        self.ReceiveSet_Init(master)
        self.SendSet_Init(master)
        self.DataWin_Init(master)
        self.BottomWin_Init(master)
        self.Data_Init()


    def Menu_Init(self, windows):   # 菜单栏初始化

        menu = Menu(windows)

        file_menu = Menu(menu, tearoff=False)
        file_menu_list = ("新建...",
                          "打开配置...",
                          "保存配置...      Ctrl+S",
                          "配置另存为...",
                          "保存消息...",
                          "查看当前消息日志",
                          "打开消息日志文件夹",
                          "退出(X)")
        for i in range(0, len(file_menu_list)):
            if i == 4 or i == 7:
                file_menu.add_separator()
            file_menu.add_command(label=file_menu_list[i])
        menu.add_cascade(label="文件(F)", menu=file_menu)

        edit_menu = Menu(menu, tearoff=False)
        edit_menu_list = ("开始",
                          "暂停",
                          "停止",
                          "Cloud Sync",
                          "增加端口",
                          "删除端口",
                          "清除",
                          "清除发送历史",
                          "Line Mode")
        for i in range(0, len(edit_menu_list)):
            if i == 3 or i == 4 or i == 6 or i == 8:
                edit_menu.add_separator()
            edit_menu.add_command(label=edit_menu_list[i])
        menu.add_cascade(label="编辑(E)", menu=edit_menu)

        view_menu = Menu(menu, tearoff=False)
        view_menu_list = ("水平",
                          "垂直",
                          "网格",
                          "自定义",
                          "快速设置",
                          "窗口设置",
                          "语言")
        for i in range(0, len(view_menu_list)):
            if i == 4 or i == 5 or i == 6:
                view_menu.add_separator()
            view_menu.add_command(label=view_menu_list[i])
        menu.add_cascade(label="视图(V)", menu=view_menu)

        tool_menu = Menu(menu, tearoff=False)
        tool_menu_list = ("ToolBox...",
                          "ASCII Code...",
                          "选项")
        for i in range(0, len(tool_menu_list)):
            if i == 2:
                tool_menu.add_separator()
            tool_menu.add_command(label=tool_menu_list[i])
        menu.add_cascade(label="工具(T)", menu=tool_menu)

        help_menu = Menu(menu, tearoff=False)
        help_menu.add_command(label="帮助文档")
        help_menu.add_command(label="联系作者")
        menu.add_cascade(label="帮助(H)", menu=help_menu)

        windows.config(menu=menu)

    def Tool_Boxs(self, windows, width):    # 工具栏初始化
        # PYSP_WIN.Tool_box = Frame(windows, bg="#DEDEDE", width=width, heigh=30)
        # PYSP_WIN.Tool_box.place(x=0, y=0)
        # PYSP_WIN.switch_button = Button(PYSP_WIN.Tool_box, text=" 打  开 ", bg="green")
        # PYSP_WIN.switch_button.place(x=10, y=0)
        Tool_box = Frame(windows, bg="#DEDEDE", width=width, heigh=30)
        Tool_box.place(x=0, y=0)
        switch_button = Button(Tool_box, text=" 打  开 ", bg="green")
        switch_button.place(x=10, y=0)

    def SerialSet_Init(self, windows):      # 串口设置模块
        SerialSetWin = LabelFrame(windows, width=200, height=210, text="串口设置")
        SerialSetWin.place(x=10, y=35)
        Label(SerialSetWin, text="端  口").place(x=5, y=10)
        Label(SerialSetWin, text="波特率").place(x=5, y=40)
        Label(SerialSetWin, text="数据位").place(x=5, y=70)
        Label(SerialSetWin, text="校验位").place(x=5, y=100)
        Label(SerialSetWin, text="停止位").place(x=5, y=130)
        Label(SerialSetWin, text="流  控").place(x=5, y=160)
        # PortComsList = PYSP_DB.com_port_list
        PortComsList = ['com1', 'com2', 'com3']
        PortComs = ttk.Combobox(SerialSetWin, width=15, values=PortComsList)
        PortComs.place(x=60, y=10)
        # PortComs.current(0)
        # BaudRateList = PYSP_DB.baud_rate_list
        BaudRateList =[9600, 12800]
        BaudRate = ttk.Combobox(SerialSetWin, width=15, values=BaudRateList)
        BaudRate.place(x=60, y=40)
        # DataBitsList = PYSP_DB.data_bits_list
        DataBitsList = [3200]
        DataBits = ttk.Combobox(SerialSetWin, width=15, values=DataBitsList)
        DataBits.place(x=60, y=70)
        # PariBitsList = PYSP_DB.parity_bit_list
        PariBitsList = [1]
        PariBits = ttk.Combobox(SerialSetWin, width=15, values=PariBitsList)
        PariBits.place(x=60, y=100)
        # StopBitsList = PYSP_DB.stop_bit_list
        StopBitsList = [3]
        StopBits = ttk.Combobox(SerialSetWin, width=15, values=StopBitsList)
        StopBits.place(x=60, y=130)
        # FlowCtrlList = PYSP_DB.flow_control_list
        FlowCtrlList = ['ctrl']
        FlowCtrl = ttk.Combobox(SerialSetWin, width=15, values=FlowCtrlList)
        FlowCtrl.place(x=60, y=160)


    def ReceiveSet_Init(self, windows):     # 接收设置模块
        ReceiveSetWin = LabelFrame(windows, width=200, height=120, text="接收设置")
        ReceiveSetWin.place(x=10, y=250)
        Radiobutton(ReceiveSetWin, text="ASCII").place(x=5, y=5)
        Radiobutton(ReceiveSetWin, text="Hex").place(x=100, y=5)
        Checkbutton(ReceiveSetWin, text="自动换行").place(x=5, y=30)
        Checkbutton(ReceiveSetWin, text="显示发送").place(x=5, y=50)
        Checkbutton(ReceiveSetWin, text="显示时间").place(x=5, y=70)

    def SendSet_Init(self, windows):        # 发送设置模块
        SendSetWin = LabelFrame(windows, width=200, height=80, text="发送设置")
        SendSetWin.place(x=10, y=375)
        Radiobutton(SendSetWin, text="ASCII").place(x=5, y=5)
        Radiobutton(SendSetWin, text="Hex").place(x=100, y=5)
        Checkbutton(SendSetWin, text="自动重发").place(x=5, y=30)
        Spinbox(SendSetWin, width=10).place(x=85, y=33)
        Label(SendSetWin, text="ms").place(x=170, y=30)

    def DataWin_Init(self, windows):    # 收发窗模块
        ReceiveWin = Text(windows, width=65, height=23)
        ReceiveWin.place(x=220, y=40)
        SendWin = Text(windows, width=55, height=5)
        SendWin.place(x=220, y=352)
        # Button(windows, text=" 发   送 ").place(x=620, y=370)
        Button(windows, text=" 清   空 ").place(x=620, y=351)
        Button(windows, text=" 发   送 ").place(x=620, y=390)
        ttk.Combobox(windows, width=62).place(x=220, y=430)

    def BottomWin_Init(self, windows):      # 底部信息栏
        button_fram = Frame(windows, bg="#DCDCDC", width=700, height=30)
        button_fram.place(x=0, y=460)

        com_state = Text(button_fram, width=40, height=1)
        # com_state.insert(END, PYSP_DB.get_com_state(PYSP_DB)[1])
        com_state.config(state=DISABLED)
        com_state.place(x=5, y=5)

        Rx_data_bits = Text(button_fram, width=20, height=1)
        # Rx_data_bits.insert(END, PYSP_DB.get_Rx_Bytes(PYSP_DB)[1])
        Rx_data_bits.config(state=DISABLED)
        Rx_data_bits.place(x=390, y=5)

        Tx_data_bits = Text(button_fram, width=20, height=1)
        # Tx_data_bits.insert(END, PYSP_DB.get_Tx_Bytes(PYSP_DB)[1])
        Tx_data_bits.config(state=DISABLED)
        Tx_data_bits.place(x=540, y=5)


    def Data_Init(self):        # 界面设置参数初始化
        print("Data Init")

root = Tk()

app = Windows(root)

root.mainloop()
