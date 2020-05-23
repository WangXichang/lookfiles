# coding: utf-8


import time
import threading
import hashlib
import os
from concurrent import futures
from tkinter import Canvas, Tk, StringVar, Label, Button, CENTER, NW, GROOVE, scrolledtext
import tkinter as tk
# import tkinter.messagebox as msgbox


class main:
    def __init__(self):
        self.mywin = Tk()
        self.mywin.title('Check Same Files')
        self.mywin.geometry('800x500+290+100')
        # self.mywin.resizable(False, False)
        self.mywin.config(bg='#535363')
        self.mywin.resizable = (True, True)

        # 进度条
        self.bar_length = 630
        self.canvas_progress_bar = Canvas(self.mywin, width=self.bar_length, height=20)
        self.canvas_shape = self.canvas_progress_bar.create_rectangle(0, 0, 0, 25, fill='green')
        self.canvas_text = self.canvas_progress_bar.create_text(292, 4, anchor=NW)
        self.canvas_progress_bar.itemconfig(self.canvas_text, text='00:00:00')
        # self.canvas_progress_bar.place(relx=0.45, rely=0.4, anchor=CENTER)

        self.var_progress_bar_percent = StringVar()
        self.var_progress_bar_percent.set('00.00 %')
        self.label_progress_bar_percent = Label(self.mywin,
                                                textvariable=self.var_progress_bar_percent,
                                                fg='#F5F5F5', bg='#535353')
        # self.label_progress_bar_percent.place(relx=0.89, rely=0.4, anchor=CENTER)

        # 按钮-1
        self.button1_start = Button(self.mywin, text='1: find', fg='#F5F5F5', bg='#7A7A7A',
                                    command=lambda: self.run_button(step=1),
                                    height=1, width=15, relief=GROOVE, bd=2,
                                    activebackground='#F5F5F5', activeforeground='#535353')
        self.button1_start.place(relx=0.45, rely=0.5, anchor=tk.SE)

        # 按钮-2
        self.button2_start = Button(self.mywin, text='2: md5', fg='#F5F5F5', bg='#7A7A7A',
                                    command=lambda: self.run_button(step=2),
                                    height=1, width=15, relief=GROOVE, bd=2,
                                    activebackground='#F5F5F5', activeforeground='#535353')
        self.button2_start.place(relx=0.45, rely=0.5, anchor=tk.SW)

        # 按钮-3
        self.button3_start = Button(self.mywin, text='3: group', fg='#F5F5F5', bg='#7A7A7A',
                                    command=lambda: self.run_button(step=3),
                                    height=1, width=15, relief=GROOVE, bd=2,
                                    activebackground='#F5F5F5', activeforeground='#535353')
        self.button3_start.place(relx=0.45, rely=0.5, anchor=tk.NE)

        # 按钮-4
        self.button4_start = Button(self.mywin, text='4: result', fg='#F5F5F5', bg='#7A7A7A',
                                    command=lambda: self.run_button(step=4),
                                    height=1, width=15, relief=GROOVE, bd=2,
                                    activebackground='#F5F5F5', activeforeground='#535353')
        self.button4_start.place(relx=0.45, rely=0.5, anchor=tk.NW)

        # 按钮-5
        self.button5_start = Button(self.mywin, text='5: delete', fg='#F5F5F5', bg='#7A7A7A',
                                    command=lambda: self.run_button(step=5),
                                    height=1, width=15, relief=GROOVE, bd=2,
                                    activebackground='#F5F5F5', activeforeground='#535353')
        self.button5_start.place(relx=0.45, rely=0.5, anchor=tk.NW)

        # path label-entry
        self.label_path = tk.Label(self.mywin,
                                   text='path',
                                   bg='#535363',
                                   fg='#F5F5F5')
        self.entry_path = tk.Entry(self.mywin, bd=5)

        # task label-entry
        self.label_task = tk.Label(self.mywin,
                                   text='task',
                                   bg='#535363',
                                   fg='#F5F5F5')
        self.entry_task = tk.Entry(self.mywin, bd=5)

        # result text
        self.text_result = tk.scrolledtext.ScrolledText(self.mywin, width=110, height=25)

        self.button1_start.grid(row=2, column=1, padx=10, pady=10)
        self.button2_start.grid(row=2, column=2, padx=10, pady=10)
        self.button3_start.grid(row=2, column=3, padx=10, pady=10)
        self.button4_start.grid(row=2, column=4, padx=10, pady=10)
        self.button5_start.grid(row=2, column=5, padx=10, pady=10)

        self.label_path.place(x=5, y=68)
        self.entry_path.place(x=40, y=65, width=400)

        self.label_task.place(x=450, y=68)
        self.entry_task.place(x=485, y=65, width=300)

        self.text_result.place(x=12, y=105)

        self.canvas_progress_bar.place(x=5, y=470, width=790)
        self.label_progress_bar_percent.place(x=5, y=440)

        self.signal = False
        self.time_step = 0.5    # for progress bar
        self.time = time.time()

        self.entry_path.insert(tk.INSERT, 'e:/test')
        self.entry_task.insert(tk.INSERT, 'test')
        self.test_path = None
        self.test_task = None
        self.findfiler = None
        self.findmd5 = None
        self.result1_file = None
        self.result2_md5 = None
        self.result3_group = None
        self.result4_same = None

    def run(self):
        self.mywin.mainloop()

    def run_button(self, step=1):
        self.test_path = self.entry_path.get()
        self.test_task = self.entry_task.get()
        th = None
        if step == 1:
            th = threading.Thread(target=self.run_prog1)
        elif step == 2:
            th = threading.Thread(target=self.run_prog2)
        elif step == 3:
            th = threading.Thread(target=self.run_prog3)
        elif step == 4:
            th = threading.Thread(target=self.run_prog4)
        elif step == 5:
            th = threading.Thread(target=self.run_prog5)
        else:
            self.disp_msg('error button!')
        th.setDaemon(True)
        th.start()

    # find subpath and files
    def run_prog1(self):
        self.test_path = self.entry_path.get()
        self.findfiler = FileFinder(self.test_path)
        finder = self.findfiler

        self.time = time.time()
        self.text_result.insert(tk.INSERT, '='*60+'\n')
        with futures.ThreadPoolExecutor() as executor:
            to_do = []
            future_dict = dict()
            future1 = executor.submit(finder.run_subdir_files)
            to_do.append(future1)
            future_dict.update({future1: 1})
            self.text_result.insert(tk.INSERT, 'step-1 process-{} start ... \n'.format(1))

            self.signal = False
            future2 = executor.submit(self.run_prog1_update_bar, 'step-1')
            to_do.append(future2)
            future_dict.update({future2: 2})
            self.text_result.insert(tk.INSERT, 'bar process-{} start ... \n'.format(2))

            for future in futures.as_completed(to_do):
                if future_dict[future] == 1:
                    result = future.result()
                    self.findfiler.find_file_list = result
                    self.signal = True
                    self.text_result.insert(tk.INSERT, 'find files process-{} end\n'.format(future_dict[future]))
                    self.text_result.insert(tk.INSERT, '-'*60+'\n')
                    [self.text_result.insert(tk.INSERT, str(f)+'\n') for i, f in enumerate(result) if i < 5]
                    self.text_result.insert(tk.INSERT, '-'*60+'\n')
                    end_str = 'check files num={}\nelapsed={:.3f}sec\nrunning end\n'.\
                        format(len(self.findfiler.find_file_list), time.time() - self.time)
                    self.text_result.insert(tk.INSERT, end_str+'='*60+'\n')
            self.result1_file = self.findfiler.find_file_list
            self.write_result(task=self.test_task,
                              step=1,
                              result_seq=self.result1_file)

    def run_prog1_update_bar(self, name):
        percent = 0
        while True:
            time.sleep(0.1)
            used_time = int(time.time() - self.time)
            percent += 1
            self.update_progress_bar(percent, used_time, name=name)
            if self.signal:
                percent = 0
                self.update_progress_bar(percent, used_time, name='finished, elapsed=')
                break

    def run_prog2(self):
        self.result1_file = self.read_result(self.test_task, step=1)
        self.text_result.insert(tk.INSERT, '{}\nstep-2 start\n'.format('='*60))
        self.findmd5 = FileMd5(self.result1_file,
                               msg_fun=self.disp_msg,
                               pbar=self.update_progress_bar)
        self.findmd5.run()
        self.result2_md5 = self.findmd5.find_file_md5_list
        self.write_result(task=self.test_task,
                          step=2,
                          result_seq=self.result2_md5)
        self.text_result.insert(tk.INSERT, 'step-2 end\n{}\n'.format('='*60))

    def run_prog3(self):
        self.result2_file = self.read_result(self.test_task, step=2)
        self.text_result.insert(tk.INSERT, '{}\nstep-3 start\n'.format('='*60))
        findgrp = FileGroup(self.result2_file,
                            msg_fun=self.disp_msg,
                            pbar=self.update_progress_bar)
        findgrp.run()
        self.result3_group = findgrp.find_file_md5_list
        self.write_result(task=self.test_task,
                          step=3,
                          result_seq=self.result3_group)
        self.text_result.insert(tk.INSERT, 'step-3 end\n{}\n'.format('='*60))

    def run_prog4(self):
        t = time.time()
        for percent in range(1, 101):
            used_time = int(time.time() - t)
            self.update_progress_bar(percent, used_time, name='step-4')
            time.sleep(0.1)

    def run_prog5(self):
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

    def disp_msg(self, msg=''):
        self.text_result.insert(tk.INSERT, msg)

    def write_result(self, task, step, result_seq):
        filename = 'result_'+task+'_'+str(step)+'.csv'
        fp = open(filename, 'w', encoding='utf-8')
        fp.write(str(result_seq))
        fp.close()

    def write_report(self, task, same_file_group_list: list):
        if len(same_file_group_list) > 0:
            path_pos = same_file_group_list[0][0].find('/') if '/' in same_file_group_list else \
                same_file_group_list[0][0].find('\\')
            check_dir = same_file_group_list[0][0][0:path_pos]
        else:
            check_dir = self.test_path
        save_file = task + '_report.csv'

        # total_file_num = len(self.result2_files_size_md5)
        total_same_file_num = sum([len(g) for g in same_file_group_list])
        t = time.time()
        tl = time.localtime()
        self.disp_msg('write same files...')
        with open(save_file, 'w', encoding='utf-8') as fw:
            fw.write('check same files in {} at {}\n'.
                     format(check_dir, [tl.tm_year, tl.tm_mon, tl.tm_mday, tl.tm_hour, tl.tm_min, tl.tm_sec])
                     )
            fw.write('='*120 + '\n')
            # fw.write('total fiels: {}\n'.format(total_file_num))
            fw.write('same  files: {}\n'.format(total_same_file_num))
            fw.write('same groups: {}\n'.format(len(same_file_group_list)))
            fw.write('-'*120 + '\n')
            for fno, flist in enumerate(same_file_group_list):
                for i, ff in enumerate(flist):
                    if i == 0:
                        fw.write('[{:^5d}]  same number={}  filesize={}\n'.
                                 format(fno, len(flist), ff[1]))
                        fw.write('-' * 50 + '\n')
                    fw.write(' ' * 8 + ff[0] + '\n')
                if fno < len(same_file_group_list) - 1:
                    fw.write('-' * 50 + '\n')
            fw.write('-'*120 + '\n')
        self.disp_msg('write elapsed={:.3f}'.format(time.time()-t))

    def read_result(self, task, step):
        filename = 'result_'+task+'_'+str(step)+'.csv'
        if os.path.isfile(filename):
            fp = open(filename, 'r', encoding='utf-8')
            result = fp.read()
            fp.close()
        else:
            result = ''
            self.disp_msg('no result file: {}\n'.format(filename))
        return result

class FileFinder:
    def __init__(self, path='e:/test'):
        self.root_dir = path

        # result data
        self.find_dir_list = []
        self.find_file_list = []
        self.find_fail = []

        # result number
        self.total_size = 0
        self.total_file_num = 0
        self.find_dir_num = 0

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
        self.find_dir_num += 1
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


class FileMd5:
    def __init__(self, file_list=None, msg_fun=None, pbar=None):
        self.msg_fun = msg_fun
        self.pbar = pbar
        self.find_file_list = file_list

        # result data
        self.find_file_md5_list = []
        self.find_fail = []

        # result number
        self.total_size = 0
        self.total_file_num = 0
        self.total_group_num = 0

    def run(self):
        if len(self.find_file_list) == 0:
            self.msg_fun('no file in file_list!')
            return
        else:
            self.msg_fun('-'*60+'\n')
        self.find_file_md5_list = self.run_md5_thread(self.find_file_list)
        self.find_file_md5_list = sorted(self.find_file_md5_list, key=lambda x: x[2])

    def run_md5_thread(self, find_file_list):
        t = time.time()
        seg_num = os.cpu_count()
        self.total_file_num = len(find_file_list)
        seg_len = int(self.total_file_num/seg_num)
        file_seg_list = [find_file_list[i*seg_len:(i+1)*seg_len] if i<seg_num-1 else
                         find_file_list[i*seg_len:]
                         for i in range(seg_num)]
        with futures.ThreadPoolExecutor(max_workers=seg_num) as executor:
            to_do = []
            future_dict = dict()
            for fi, file_list in enumerate(file_seg_list):
                future = executor.submit(self.run_md5, file_list, 'md5 thread-'+str(fi))
                to_do.append(future)
                future_dict.update({future: fi})
            self.msg_fun('-'*60+'\n')
            result = []
            for future in futures.as_completed(to_do):
                res = future.result()
                result.extend(res)
                # self.msg_fun('md5 thread-{} end ... \n'.format(fi))
        self.msg_fun('{1}\ncalc md5 end  elapsed={0:.2f} \n'.format(time.time()-t, '-'*60))
        return result

    def run_md5(self, file_list=None, task='run md5'):
        t = time.time()
        self.msg_fun('{}\n'.format(task))
        if len(file_list) == 0:
            self.msg_fun('no file found!\n')
            return

        find_file_md5_list = []
        total = len(file_list)
        for i, f in enumerate(file_list):
            percent = int(i/total*100)
            self.pbar(percent, time.time()-t, task)
            # if percent in [30, 50, 80]:
            #     self.msg_fun(task+' percent= {}\n'.format(percent))
            try:
                f_m5 = self.make_md5(f)
            except IOError:
                self.find_fail.append('file: ' + f)
                continue
            find_file_md5_list.append([f, os.path.getsize(f), f_m5])
        self.msg_fun(task+'end  elapsed: {:3f}\n'.format(time.time()-t))
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


class FileGroup:
    def __init__(self, md5_list=None, msg_fun=None, pbar=None):
        self.msg_fun = msg_fun
        self.pbar = pbar
        self.md5_list = md5_list

        # result data
        self.find_group_list = []
        self.find_fail = []

    def run(self):
        tstart = time.time()
        # print('get same md5 group start ...')
        self.msg_fun('get same md5 group start ...')

        # create groups with same files
        file_size_md5_list = self.md5_list
        file_group_same_md5 = []
        current_group = []
        last_md5 = b''
        total_len = len(file_size_md5_list)
        for fi, f_size_md5 in enumerate(file_size_md5_list):
            self.pbar(int(fi/total_len)*100, time.time()-tstart, 'group files by md5 ')
            if f_size_md5[2] != last_md5:
                # add group to file_group if 2 more files in
                if len(current_group) > 1:
                    file_group_same_md5.append(current_group)
                # new group
                current_group = [f_size_md5]  # [ftuple(f_name_md5[0], os.path.getsize(f_name_md5[0]), f_name_md5[2])]
                last_md5 = f_size_md5[2]
            else:
                # current_group.append(ftuple(f_name_md5[0], os.path.getsize(f_name_md5[0]), f_name_md5[2]))
                current_group.append(f_size_md5)
        # group with 2 more files
        if len(current_group) > 1:
            file_group_same_md5.append(current_group)

        self.msg_fun('md5 group end,  elapsed={:.3f}\n'.format(time.time()-tstart))
        self.find_group_list =  file_group_same_md5


if __name__ == '__main__':
    w = main()
    w.run()
