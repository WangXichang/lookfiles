# coding=utf8


import os
import glob
import time
import sys
import filecmp
import hashlib
from collections import namedtuple as ntp
from concurrent import futures
import asyncio


help_doc = \
    """
        调用方式：
        [一次性调用]
        files.same_files(dir, task, step)
            dir是要检查的目录名
            task为任务名, 用于生成分步计算结果： [task]_[step].csv 及 最终报告： [task]_report.csv
            step为运行步骤号，0为全部一次运行， 1-4为单步运行第step步（读取上一步结果）      
        
        运行过程：
        *** start ...
        [step-1: 获取路径文件名]
        [step-2: 计算md5]
        [step-3: 按md5进行分组]
        [step-4: 分组内进行内容比较]                 
        
    """


def same_files(dir: str, task='check_test', step=0):
    fd = FindSameFiles(dir, task)
    if step == 0:
        fd.run()
    elif step in [1, 2, 3, 4]:
        fd.run_step(step=step)
    else:
        print('step is in 0-4!')
    return fd


class FindSameFiles:
    def __init__(self, check_dir=None, task='check'):
        # input para
        self.check_dir = check_dir
        self.task = task

        # result save file name
        self.result1_file_name = task + '_1.csv'
        self.result2_file_name = task + '_2.csv'
        self.result3_file_name = task + '_3.csv'
        self.result4_file_name = task + '_4.csv'

        # result data
        self.result1_files = None
        self.result2_files_size_md5 = None
        self.result3_group_by_md5 = None
        self.result4_group_same_files = None

        self.file_finder = FileFinder(check_dir)

        # result para
        self.total_time = 0

    def run(self):
        # test dir
        if not os.path.isdir(self.check_dir):
            print('Invalid directory: {}'.format(self.check_dir))
            return None

        # running
        print('*** start ...')

        self.total_time = 0
        t = time.time()
        self.run_step(all_step=True)
        self.total_time += time.time()-t

        print('total files: {}, same file: {}'.
              format(len(self.result2_files_size_md5), sum([len(g) for g in self.result4_group_same_files]))
              )
        print('*** end: total elapsed={:.2f}'.format(self.total_time))

    def run_step(self, step=1, load=True, all_step=False):
        if all_step:
            load = False
        if step == 1 or all_step:
            self.run1_get_files()
            self.write_result(step=step)
        if step == 2 or all_step:
            if load:
                self.result1_files = self.load_result(step=1)
            self.run2_get_md5()
            self.write_result(step=2)
        if step == 3 or all_step:
            if load:
                self.result2_files_size_md5 = self.load_result(step=2)
            self.result3_group_by_md5 = self.run3_get_files_group()
            self.write_result(step=3)
        if step == 4 or all_step:
            if load:
                self.result3_group_by_md5 = self.load_result(step=3)
            # self.result4_group_same_files = self.run4_get_groups_same_content(self.result3_group_by_md5)
            # self.result4_group_same_files = self.run4_thread(self.result3_group_by_md5)
            self.result4_group_same_files = self.run4_process(self.result3_group_by_md5)
            self.result4_group_same_files = sorted(self.result4_group_same_files, key=lambda x: x[0][1])
            self.write_result(step=4)
            self.write_report()

    def run1_get_files(self):
        self.file_finder.run_subdir_files()
        self.result1_files = self.file_finder.find_file_list

    def run2_get_md5(self):
        result = self.file_finder.run_md5_process(self.result1_files)
        self.result2_files_size_md5 = sorted(result, key=lambda x: x[2])

    def run3_get_files_group(self):
        ftuple = ntp('Ftuple', ('name', 'size', 'md5'))
        tstart = time.time()
        print('get same md5 group start ...')

        # create groups with same files
        file_size_md5_list = self.result2_files_size_md5
        pbar = ProgressBar(total=len(file_size_md5_list), name='same size check')
        file_group_same_md5 = []
        current_group = []
        last_md5 = b''
        for f_size_md5 in file_size_md5_list:
            pbar.log()
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

        print('elapsed={:.3f}'.format(time.time()-tstart))
        return file_group_same_md5

    def run4_thread(self, file_group_same_md5):
        t = time.time()
        print('get same content start ...')
        workers = 10
        seg_len = int(len(file_group_same_md5)/workers)
        file_seg_list = [file_group_same_md5[i*seg_len:(i+1)*seg_len]
                         if i < workers-1 else
                         file_group_same_md5[i*seg_len:]
                         for i in range(workers)]
        with futures.ThreadPoolExecutor(max_workers=workers) as executor:
            to_do = []
            future_dict = dict()
            for fi, fglist in enumerate(file_seg_list):
                future = executor.submit(self.run4_get_groups_same_content, fglist)
                to_do.append(future)
                future_dict.update({future: fi})
                print('thread-{} start len={} ... '.format(fi, len(fglist)))
            result = []
            for future in futures.as_completed(to_do):
                res = future.result()
                result.extend(res)
                print('thread-{} end'.format(future_dict[future]))
        print('same content total elapsed: {:.2f}'.format(time.time()-t))
        return result

    def run4_process(self, file_group_same_md5):
        t = time.time()
        print('content check processes start ...')
        workers = os.cpu_count()
        seg_len = int(len(file_group_same_md5)/workers)
        file_seg_list = [file_group_same_md5[i*seg_len:(i+1)*seg_len]
                         if i < workers-1 else
                         file_group_same_md5[i*seg_len:]
                         for i in range(workers)]
        with futures.ProcessPoolExecutor() as executor:
            to_do = []
            future_dict = dict()
            for fi, fglist in enumerate(file_seg_list):
                future = executor.submit(self.run4_get_groups_same_content, fglist)
                to_do.append(future)
                future_dict.update({future: fi})
                print('content process-{} start len={} ... '.format(fi, len(fglist)))
            result = []
            for future in futures.as_completed(to_do):
                res = future.result()
                result.extend(res)
                print('content process-{} end'.format(future_dict[future]))
        print('same content total elapsed: {:.2f}'.format(time.time()-t))
        return result

    def run4_get_groups_same_content(self, file_group_same_md5):
        t = time.time()
        print('check same content subgroup start ...')
        file_group_same_content = []
        pbar = ProgressBar(total=len(file_group_same_md5), name='same content check')
        for i, fg in enumerate(file_group_same_md5):
            pbar.log()
            if len(fg) <= 1:
                continue
            elif len(fg) == 2:
                if fg[0][2] == fg[1][2]:
                    # size == 0
                    if fg[0][1] == 0:
                        file_group_same_content.append(fg)
                        continue
                    with open(fg[0][0], 'rb') as f1, open(fg[1][0], 'rb') as f2:
                        f1r = f1.read()
                        f2r = f2.read()
                        if f1r == f2r:
                            file_group_same_content.append(fg)
                continue
            # len(fg) > 2
            else:
                same_group = []
                same_to_last = False
                for fi, f in enumerate(fg[:-1]):
                    # compare by md5
                    if f[2] == fg[fi+1][2]:
                        if same_to_last:
                            same_group.append(f)
                        else:
                            if len(same_group) > 0:
                                file_group_same_content.append(same_group)
                            same_group = [f]
                        same_to_last = True
                        # last item
                        if fi == len(fg) - 2:
                            same_group.append(fg[fi+1])
                            file_group_same_content.append(same_group)
                    else:
                        if same_to_last:
                            same_group.append(f)
                            file_group_same_content.append(same_group)
                            same_group = []
                        else:
                            # single file, different from last and next
                            # save and clear same_group
                            if len(same_group) > 1:
                                file_group_same_content.append(same_group)
                                same_group = []
                        same_to_last = False
        print('elapsed={:.3f}'.format(time.time()-t))
        return file_group_same_content

    def write_result(self, step=0):
        if step in [1, 2, 3, 4]:
            file_name = [self.result1_file_name,
                         self.result2_file_name,
                         self.result3_file_name,
                         self.result4_file_name][step-1]
            result_list = [self.result1_files,
                           self.result2_files_size_md5,
                           self.result3_group_by_md5,
                           self.result4_group_same_files][step-1]
            with open(file_name, 'w', encoding='utf-8') as fp:
                for line in result_list:
                    fp.write(str(line)+'\n')
        else:
            raise ValueError

    def load_result(self, step=0):
        result_list = []
        if step in [1, 2, 3, 4]:
            file_name = [self.result1_file_name,
                         self.result2_file_name,
                         self.result3_file_name,
                         self.result4_file_name][step-1]
            with open(file_name, 'r', encoding='utf-8') as fp:
                while True:
                    rline = fp.readline()
                    if rline:
                        if step == 1:
                            result_list.append(rline.replace('\n', ''))
                        else:
                            result_list.append(eval(rline))
                    else:
                        break
        return result_list

    def write_report(self):
        # set data and check_dir, filename to write
        same_file_group_list = self.result4_group_same_files
        check_dir = self.check_dir
        save_file = self.task + '_report.csv'

        # total_file_num = len(self.result2_files_size_md5)
        total_same_file_num = sum([len(g) for g in same_file_group_list])
        t = time.time()
        tl = time.localtime()
        print('write same files...')
        pbar = ProgressBar(total=len(same_file_group_list), name='write to ' + save_file)
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
                pbar.log()
                for i, ff in enumerate(flist):
                    if i == 0:
                        fw.write('[{:^5d}]  same number={}  filesize={}\n'.
                                 format(fno, len(flist), ff[1]))
                        fw.write('-' * 50 + '\n')
                    fw.write(' ' * 8 + ff[0] + '\n')
                if fno < len(same_file_group_list) - 1:
                    fw.write('-' * 50 + '\n')
            fw.write('-'*120 + '\n')
        print('write elapsed={:.3f}'.format(time.time()-t))


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
        pbar = ProgressBar(total=10**6)
        t = time.time()
        self.get_subdir_files(self.root_dir, pbar)
        print('elapsed: {:3f}'.format(time.time()-t))

    def get_subdir_files(self, check_dir, pbar):
        self.find_dir_num +=1
        pbar.log()
        this_size = 0
        try:
            this_check = os.listdir(check_dir)
        except IOError:
            self.find_fail.append('dir: '+check_dir)
            this_check = []

        for d in this_check:
            full_name = check_dir + '/' + d
            if os.path.isdir(full_name):
                self.get_subdir_files(full_name, pbar)
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
            pbar = ProgressBar(total=len(file_list))
        else:
            print('no files found!')
            return

        find_file_md5_list = []
        for f in file_list:
            pbar.log()
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


class AsyncioRunner:

    def __init__(self, file_list):
        self.file_list = file_list
        self.stop = False

    @asyncio.coroutine
    def low_func(self, file_list):
        pass

    @asyncio.coroutine
    def superproc(self):
        pass

    def run(self):
        pass


def test_find_files(dir: str):
    fd = FileFinder(path=dir)
    fd.run()
    return fd


class ProgressBar:
    def __init__(self, count=0, total=0, width=50, display_gap=1, name='progress'):
        self.name = name + ':'
        self.count = count
        self.total = total
        self.width = width
        self.display_gap = display_gap

    def __del__(self):
        # sys.stdout.write('\n')
        pass

    def __move(self):
        self.count += 1

    def log(self, s=''):
        if (self.count != 1) & (self.count != self.total) & \
                (self.count % self.display_gap > 0):
            return
        sys.stdout.write(' ' * (self.width + 9) + '\r')
        sys.stdout.flush()
        if len(s) > 0:
            print(s)
        progress = int(self.width * self.count / self.total)
        progress = progress if progress < self.width else self.width  # progress + 1
        # black:\u2588
        black_part_str = '\u25A3' * progress
        white_part_str = '\u2610' * (self.width - progress)
        sys.stdout.write('{3}{0:6d}/{1:d} {2:>6}%: '.
                         format(self.count,
                                self.total,
                                str(round(self.count / self.total * 100, 2)),
                                self.name
                                ))
        sys.stdout.write(black_part_str + ('\u27BD' if progress < self.width else '') +
                         white_part_str + '\r')
        # if progress == self.width:
        #     sys.stdout.write('\n')
        sys.stdout.flush()
        self.__move()


def test_read(f1='', f2=''):
    if os.path.isfile(f1) and os.path.isfile(f2):
        with open(f1, 'rb') as fp1, open(f2, 'rb') as fp2:
            print(fp1.read())
            print(fp2.read())
    elif os.path.isfile(f1):
        with open(f1, 'rb') as fp1:
            print('name1 is valid, read=: {}'.format(fp1.read()))
    elif os.path.isfile(f2):
        with open(f2, 'rb') as fp2:
            print('name2 is valid, read=: {}'.format(fp2.read()))
    else:
        print('two invalid file name!')


def get_mem_cpu():
    import psutil
    data = psutil.virtual_memory()
    result_dict = {'total_mem': data.total,
                   'free_mem': data.available,
                   'cpu_num': psutil.cpu_count(),
                   'cpu_used_percent': psutil.cpu_percent(interval=1)}

    # total = data.total  # 总内存,单位为byte
    # print('total', total)
    # free = data.available  # 可用内存
    # print('free', free)
    # memory = "Memory usage:%d" % (int(round(data.percent))) + "%" + " "  # 内存使用情况
    # print('memory', memory)
    # cpu = "CPU:%0.2f" % psutil.cpu_percent(interval=1) + "%"  # CPU占用情况
    # print('cpu', cpu)

    return result_dict


class ProgressBarGui:
    def __init__(self):
        # import threading
        # from tkinter import *
        pass

    def update_progress_bar():
        for percent in range(1, 101):
            hour = int(percent / 3600)
            minute = int(percent / 60) - hour * 60
            second = percent % 60
            green_length = int(sum_length * percent / 100)
            canvas_progress_bar.coords(canvas_shape, (0, 0, green_length, 25))
            canvas_progress_bar.itemconfig(canvas_text, text='%02d:%02d:%02d' % (hour, minute, second))
            var_progress_bar_percent.set('%0.2f %%' % percent)
            time.sleep(1)


    def run(self):
        th = threading.Thread(target=update_progress_bar)
        th.setDaemon(True)
        th.start()

    def main(self):
        top = Tk()
        top.title('Progress Bar')
        top.geometry('800x500+290+100')
        top.resizable(False, False)
        top.config(bg='#535353')

        # 进度条
        sum_length = 630
        canvas_progress_bar = Canvas(top, width=sum_length, height=20)
        canvas_shape = canvas_progress_bar.create_rectangle(0, 0, 0, 25, fill='green')
        canvas_text = canvas_progress_bar.create_text(292, 4, anchor=NW)
        canvas_progress_bar.itemconfig(canvas_text, text='00:00:00')
        var_progress_bar_percent = StringVar()
        var_progress_bar_percent.set('00.00 %')
        label_progress_bar_percent = Label(top, textvariable=var_progress_bar_percent, fg='#F5F5F5', bg='#535353')
        canvas_progress_bar.place(relx=0.45, rely=0.4, anchor=CENTER)
        label_progress_bar_percent.place(relx=0.89, rely=0.4, anchor=CENTER)
        # 按钮
        button_start = Button(top, text='开始', fg='#F5F5F5', bg='#7A7A7A', command=run, height=1, width=15, relief=GROOVE, bd=2,
                              activebackground='#F5F5F5', activeforeground='#535353')
        button_start.place(relx=0.45, rely=0.5, anchor=CENTER)

        top.mainloop()