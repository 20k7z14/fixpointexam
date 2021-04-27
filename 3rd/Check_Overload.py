import csv,glob,sys,queue,json
import datetime as dt
import argparse


def Convert_Roundtime(str_rt:str) -> 'int':
    if str_rt == '-':
        return -1
    else:
        return int(str_rt)

# File input sequence
def Load_Testdata() -> 'list,dict':
    opetime_info = {}
    all_serveraddress = set()
    datetime_list = []
    try:
        #modify to the expression windows's path
        testcase_files = [tmp.replace('\\','/') for tmp in glob.glob(TESTCASE_PATH)]
        fix_candidate = set()

        for test_file in testcase_files:
            with open(test_file,'r') as infile:
                testcase = csv.reader(infile)
                for t in testcase:
                    try:
                        date_info,ip_address,round_time = t

                        date_info = dt.datetime.strptime(date_info,'%Y%m%d%H%M%S')
                        datetime_list.append(date_info)
                        round_time = Convert_Roundtime(round_time)
                        all_serveraddress.add(ip_address)
                        if round_time == -1:
                            fix_candidate.add(ip_address)

                        opetime_info.setdefault(ip_address,[]).append([date_info,round_time])
                    except ValueError:
                        continue

    except FileNotFoundError:
        print('You prepare testcase csv files!')
        sys.exit()

    period = min(datetime_list).strftime('%Y%m%d_%H%M%S')+'-'+max(datetime_list).strftime('%Y%m%d_%H%M%S')

    return list(all_serveraddress),list(fix_candidate),opetime_info,period

# File Output sequence
def Idlinglog_ToCsv(idle_log:dict,period:str,n:int):
    with open(RESULT_DIR + f'/n_{n}_{period}.csv', 'w') as outfile:
        writer = csv.writer(outfile)
        for k,v in idle_log.items():
            writer.writerow([k,v])
    print('output : ' + RESULT_DIR + f'/n_{n}_{period}.csv')

def Overloadlog_ToCsv(overload_log:dict,period:str,m:int,t:int):
        with open(RESULT_DIR + f'/m{m}_t{t}_{period}.csv', 'w') as outfile:
            writer = csv.writer(outfile)
            for k,v in overload_log.items():
                writer.writerow([k,v])
        print('output : ' + RESULT_DIR + f'/m{m}_t{t}_{period}.csv')

# Inner sequence
def Extract_Idlinglog(target_servers:list,opetime_info:dict,max_checktime:int=4) -> 'dict':
    idle_log = {}
    for server in list(set(target_servers)):
        server_info = queue.Queue()
        for info in sorted(opetime_info[server]): # set server's infomation a queue
            server_info.put(info)

        total_idlingtime = 0
        while not server_info.empty():
            bf_time,bf_rdtime = server_info.get()
            if bf_rdtime == -1:
                begin_idlingtime = bf_time
                check_index = 1
                record_flg = False

                while not server_info.empty():
                    af_time,af_rdtime = server_info.get()
                    if max_checktime <= check_index: # when count over, set a record flag
                        record_flg = True
                    else:
                        pass

                    if af_rdtime!= -1 and record_flg: # case: set record flag and responded
                        between = af_time - begin_idlingtime
                        total_idlingtime += int(between.total_seconds()*1000) + af_rdtime
                        idle_log[server] = total_idlingtime
                        record_flg = False

                    if server_info.empty() and record_flg: # case: the log file is cut on the way
                        idle_log[server] = float('inf')

                    check_index += 1
            else:
                pass
    return idle_log


def Calc_Average(info_queue:queue)->'int, int':
    init_length = info_queue.qsize()
    sum = 0
    while not info_queue.empty():
        datetime_info, rd_time = info_queue.get()
        if rd_time == -1:
            rd_time = DefaultRoundTime
        sum += rd_time

    return sum//init_length, init_length


def Extract_Overloadlog(all_servers:list,opetime_info:dict,m:int,t:int)-> 'dict':
    overload_log = {}
    for server in all_servers:
        server_info = queue.Queue()
        average_overload = 0
        for a_row_log in sorted(opetime_info[server]):
            server_info.put(a_row_log)

        if server_info.qsize() > m:
            while server_info.qsize() != m:
                tmp = server_info.get()
            average_overload,mean_parameter = Calc_Average(server_info)
        else:
            average_overload,mean_parameter = Calc_Average(server_info)

        if average_overload >= t:
            overload_log[server] = average_overload, mean_parameter
        else:
            pass
    return overload_log

# Before  all sequence
def Init_setting():
    json_open = open('../setting.json','r')
    setting_param = json.load(json_open)

    global TESTCASE_PATH,RESULT_DIR
    TESTCASE_PATH = setting_param['Path']['testcase_dir_path']
    RESULT_DIR = setting_param['Path']['result_dir_path']

    global DefaultRoundTime,n_MaxValue,m_MaxValue,t_MaxValue
    DefaultRoundTime = setting_param['DefaultValue']['DefaultRoundTime']
    n_MaxValue = setting_param['DefaultValue']['n_MaxValue']
    m_MaxValue = setting_param['DefaultValue']['m_MaxValue']
    t_MaxValue = setting_param['DefaultValue']['t_MaxValue']

def Analysis_Args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n','--N',help='Min timeout count. default 4',action='store_true')
    parser.add_argument('-o','--overload',action='store_true')
    parser.add_argument('-s','--setting',action='store_true')
    return parser.parse_args()


def main():
    Init_setting()
    args = Analysis_Args()
    if not args.N ^ args.overload:
        print('You should pick either -n or -o( --N or --overload).')
        sys.exit()
    else:
        if args.N:
            try:
                n = int(input('N : '))
            except ValueError as ValE:
                print(ValE)
                print('You pick Natural Number.')
                sys.exit()
            if 1<=n<=n_MaxValue:
                list_allserver_address,list_fix_candidates,dict_opetime,period = Load_Testdata()
                extracted_idlinginfo = Extract_Idlinglog(list_fix_candidates,dict_opetime,n)
                Idlinglog_ToCsv(extracted_idlinginfo,period,n)
            else:
                print(f'You pick natural number between 1 and {n_MaxValue}.(You can change it setting.json)')
                sys.exit()


        if args.overload:
            try:
                m = int(input('Most recent reference count [m] : '))
                t = int(input('Average of response time [t] : '))
            except ValueError as ValE:
                print(ValE)
                print('You pick Natural Number.')
                sys.exit()

            if 1<=m<=m_MaxValue and 1<=t<=t_MaxValue:
                list_allserver_address,list_fix_candidates,dict_opetime,period = Load_Testdata()
                extracted_overloadinfo = Extract_Overloadlog(list_allserver_address,dict_opetime,m,t)
                Overloadlog_ToCsv(extracted_overloadinfo,period,m,t)
            else:
                if m<1 or m_MaxValue<m:
                    print(f'You pick [m] in natural number between 1 and {m_MaxValue}.(You can change it setting.json)')
                if t<1 or t_MaxValue<t:
                    print(f'You pick [t] in natural number between 1 and {t_MaxValue}.(You can change it setting.json)')

                sys.exit()

if __name__ == '__main__':
    main()
