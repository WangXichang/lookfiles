# coding: utf-8


import time
import threading
import hashlib
import os
from concurrent import futures
from tkinter import Canvas, Tk, StringVar, Label, Button, CENTER, NW, GROOVE
import tkinter as tk


class main:
    def __init__(self):
        self.mywin = Tk()
        self.mywin.title('Check Same Files')
        self.mywin.geometry('800x500+290+100')
        self.mywin.resizable(False, False)
        self.mywin.config(bg='#535363')

        # 进度条
        self.bar_length = 630
        self.canvas_progress_bar = Canvas(self.mywin, width=self.bar_length, height=20)
        self.canvas_shape = self.canvas_progress_bar.create_rectangle(0, 0, 0, 25, fill='green')
        self.canvas_text = self.canvas_progress_bar.create_text(292, 4, anchor=NW)
        self.canvas_progress_bar.itemconfig(self.canvas_text, text='00:00:00')
        self.canvas_progress_bar.place(relx=0.45, rely=0.4, anchor=CENTER)

        self.var_progress_bar_percent = StringVar()
        self.var_progress_bar_percent.set('00.00 %')
        self.label_progress_bar_percent = Label(self.mywin,
                                                textvariable=self.var_progress_bar_percent,
                                                fg='#F5F5F5', bg='#535353')
        self.label_progress_bar_percent.place(relx=0.89, rely=0.4, anchor=CENTER)

        # 按钮-1
        self.button1_start = Button(self.mywin, text='step-1', fg='#F5F5F5', bg='#7A7A7A',
                                    command=self.run_button1,
                                    height=1, width=15, relief=GROOVE, bd=2,
                                    activebackground='#F5F5F5', activeforeground='#535353')
        self.button1_start.place(relx=0.45, rely=0.5, anchor=tk.SE)

        # 按钮-2
        self.button2_start = Button(self.mywin, text='step-2', fg='#F5F5F5', bg='#7A7A7A',
                                    command=self.run_button2,
                                    height=1, width=15, relief=GROOVE, bd=2,
                                    activebackground='#F5F5F5', activeforeground='#535353')
        self.button2_start.place(relx=0.45, rely=0.5, anchor=tk.SW)

        # 按钮-3
        self.button3_start = Button(self.mywin, text='step-3', fg='#F5F5F5', bg='#7A7A7A',
                                    command=self.run_button3,
                                    height=1, width=15, relief=GROOVE, bd=2,
                                    activebackground='#F5F5F5', activeforeground='#535353')
        self.button3_start.place(relx=0.45, rely=0.5, anchor=tk.NE)

        # 按钮-4
        self.button4_start = Button(self.mywin, text='step-4', fg='#F5F5F5', bg='#7A7A7A',
                                    command=self.run_button4,
                                    height=1, width=15, relief=GROOVE, bd=2,
                                    activebackground='#F5F5F5', activeforeground='#535353')
        self.button4_start.place(relx=0.45, rely=0.5, anchor=tk.NW)

        self.button1_start.grid(row=3, column=1, padx=10, pady=10)
        self.button2_start.grid(row=3, column=2, padx=10, pady=10)
        self.button3_start.grid(row=3, column=3, padx=10, pady=10)
        self.button4_start.grid(row=3, column=4, padx=10, pady=10)

        self.signal = False
        self.time = time.time()
        self.test_path = 'd:\\anaconda3' if os.path.isdir('d:/temp') else 'e:/test'
        self.finder = FileFinder(self.test_path)
        self.time_step = 0.5    # for progress bar

    def run(self):
        self.mywin.mainloop()

    def run_button1(self):
        th = threading.Thread(target=self.run_prog1)
        th.setDaemon(True)
        th.start()

    def run_button2(self):
        th = threading.Thread(target=self.run_prog2)
        th.setDaemon(True)
        th.start()

    def run_button3(self):
        th = threading.Thread(target=self.run_prog3)
        th.setDaemon(True)
        th.start()

    def run_button4(self):
        th = threading.Thread(target=self.run_prog4)
        th.setDaemon(True)
        th.start()

    # find subpath and files
    def run_prog1(self):
        # t = time.time()
        # for percent in range(1, 101):
        #     used_time = int(time.time() - t)
        #     self.update_progress_bar(percent, used_time, name='step-1')
        #     time.sleep(0.1)

        # finder = FileFinder(self.test_path)
        finder = self.finder
        self.time = time.time()
        with futures.ThreadPoolExecutor(max_workers=2) as executor:
            to_do = []
            future_dict = dict()
            future1 = executor.submit(finder.run_subdir_files)
            to_do.append(future1)
            future_dict.update({future1: 1})
            print('process-{} start ... '.format(1))

            self.signal = False
            future2 = executor.submit(self.run_prog1_update_bar, 'step-1')
            to_do.append(future2)
            future_dict.update({future2: 2})
            print('process-{} start ... '.format(2))

            for future in futures.as_completed(to_do):
                if future_dict[future] == 1:
                    result = future.result()
                    # print(result)
                    self.finder.find_file_list = result
                    self.signal = True
                    print('find files process-{} end'.format(future_dict[future]))

    def run_prog1_update_bar(self, name):
        print(name)
        percent = 0
        while True:
            time.sleep(0.1)
            used_time = int(time.time() - self.time)
            percent += 1
            self.update_progress_bar(percent, used_time, name=name)
            if self.signal:
                percent = 0
                self.update_progress_bar(percent, used_time, name='finished, elapsed=')
                print('progress bar process end')
                break

    def run_prog2(self):
        t = time.time()
        for percent in range(1, 101):
            used_time = int(time.time() - t)
            self.update_progress_bar(percent, used_time, name='step-2')
            time.sleep(0.1)

    def run_prog3(self):
        t = time.time()
        for percent in range(1, 101):
            used_time = int(time.time() - t)
            self.update_progress_bar(percent, used_time, name='step-3')
            time.sleep(0.1)

    def run_prog4(self):
        t = time.time()
        for percent in range(1, 101):
            used_time = int(time.time() - t)
            self.update_progress_bar(percent, used_time, name='step-4')
            time.sleep(0.1)

    def update_progress_bar(self, percent=1, used_time=0, name='test'):
        hour = int(used_time / 3600)
        minute = int(used_time / 60) % 60
        second = used_time % 60
        self.canvas_progress_bar.itemconfig(self.canvas_text, text=name+' %02d:%02d:%02d' % (hour, minute, second))
        self.var_progress_bar_percent.set('%0.2f %%' % percent)
        green_length = int(self.bar_length * percent / 100)
        self.canvas_progress_bar.coords(self.canvas_shape, (0, 0, green_length, 25))


class FileFinder:
    def __init__(self, path='e:/test'):
        self.root_dir = path

        # result data
        self.find_dir_list = []
        self.find_file_list = []
        self.find_file_md5_list = []
        self.find_fail = []

        # result number
        self.total_size = 0
        self.total_file_num = 0
        self.find_dir_num = 0

    def run(self):
        self.run_subdir_files()
        # self.find_file_list = self.run_md5(self.find_file_list)
        self.find_file_md5_list = self.run_md5_process(self.find_file_list)
        self.find_file_md5_list = sorted(self.find_file_md5_list, key=lambda x: x[2])

    def run_subdir_files(self):
        self.total_size = 0
        self.total_file_num = 0
        self.find_dir_list = []
        self.find_file_list = []
        self.find_dir_num = 0
        print('get subdir files ...')
        # pbar = ProgressBar(total=10**6)
        t = time.time()
        self.get_subdir_files(self.root_dir)
        print('elapsed: {:3f}'.format(time.time()-t))
        return self.find_file_list

    def get_subdir_files(self, check_dir):
        self.find_dir_num +=1
        # pbar.log()
        this_size = 0
        try:
            this_check = os.listdir(check_dir)
        except IOError:
            self.find_fail.append('dir: '+check_dir)
            this_check = []

        for d in this_check:
            full_name = check_dir + '/' + d
            if os.path.isdir(full_name):
                self.get_subdir_files(full_name)
            else:
                this_size += os.path.getsize(full_name)
                self.find_file_list.append(full_name)
                self.total_file_num += 1
        self.find_dir_list.append([check_dir, this_size])
        self.total_size += this_size
        return

    def run_md5_process(self, find_file_list):
        t = time.time()
        seg_num = os.cpu_count()
        tlen = len(find_file_list)
        seg_len = int(tlen/seg_num)
        file_seg_list = [find_file_list[i*seg_len:(i+1)*seg_len] if i<seg_num-1 else
                         find_file_list[i*seg_len:]
                         for i in range(seg_num)]
        with futures.ProcessPoolExecutor() as executor:
            to_do = []
            future_dict = dict()
            for fi, flist in enumerate(file_seg_list):
                future = executor.submit(self.run_md5, flist)
                to_do.append(future)
                future_dict.update({future: fi})
                print('process-{} start ... '.format(fi))
            result = []
            for future in futures.as_completed(to_do):
                res = future.result()
                result.extend(res)
                print('process-{} end'.format(future_dict[future]))
        print('md5 process elapsed: {:.2f}'.format(time.time()-t))
        return result

    def run_md5_thread(self, find_file_list):
        t = time.time()
        workers = 30
        seg_len = int(len(find_file_list)/workers)
        file_seg_list = [find_file_list[i*seg_len:(i+1)*seg_len]
                         if i < workers-1 else
                         find_file_list[i*seg_len:]
                         for i in range(workers)]
        with futures.ThreadPoolExecutor(max_workers=workers) as executor:
            to_do = []
            future_dict = dict()
            for fi, flist in enumerate(file_seg_list):
                future = executor.submit(self.run_md5, flist)
                to_do.append(future)
                future_dict.update({future: fi})
                print('thread-{} start len={} ... '.format(fi, len(flist)))
            result = []
            for future in futures.as_completed(to_do):
                res = future.result()
                result.extend(res)
                print('thread-{} end'.format(future_dict[future]))
        print('md5 thread elapsed: {:.2f}'.format(time.time()-t))
        return result

    def run_md5_pandas(self, find_file_list):
        import pandas as pd
        t = time.time()
        df = pd.DataFrame({'file': find_file_list, 'md5': list(range(len(find_file_list)))})
        df['md5'] = df['file'].apply(self.get_file_md5)
        print('elapsed {:.2f}'.format(time.time()-t))
        return df

    def run_md5_map(self, find_file_list):
        from multiprocessing import Pool
        import copy
        file_copy = copy.deepcopy(find_file_list)
        pool = Pool()
        pool.map(self.get_file_md5, find_file_list)
        pool.close()
        pool.join()
        return zip(file_copy, find_file_list)

    def get_file_md5(self, filename):
        fp = open(filename, 'rb')
        m5 = hashlib.md5()
        m5.update(fp.read())
        return m5.digest()

    def run_md5(self, file_list):
        t = time.time()
        print('get md5 ...')
        if len(file_list) > 0:
            # pbar = ProgressBar(total=len(file_list))
            pass
        else:
            print('no files found!')
            return

        find_file_md5_list = []
        for f in file_list:
            # pbar.log()
            try:
                f_m5 = self.make_md5(f)
            except IOError:
                print('error read file: {}'.format(f))
                self.find_fail.append('file: ' + f)
                continue
            find_file_md5_list.append([f, os.path.getsize(f), f_m5])

        # result_file_list = sorted(find_file_md5_list, key=lambda x: x[2])
        print('elapsed: {:3f}'.format(time.time()-t))
        return find_file_md5_list

    @staticmethod
    def make_md5(filename):
        # block_size = 10*1024 * 1024
        m5 = hashlib.md5()
        with open(filename, 'rb') as fp:
            m5.update(fp.read())
            # while True:
            #     data = fp.read(block_size)
            #     if not data:
            #         break
            #     m5.update(data)
        return m5.digest()

# freeze.support()
w = main()
w.run()
print('exit prog')
print('check files num={}'.format(len(w.finder.find_file_list)))
