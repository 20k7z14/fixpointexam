import csv,glob,sys,queue,json
import datetime as dt
import argparse

def convert_roundtime(str_rt:str) -> 'int':
    if str_rt == '-':
        return -1
    else:
        return int(str_rt)

def Init_setting():
    json_open = open('../setting.json','r')
    setting_param = json.load(json_open)

    global TESTCASE_PATH,RESULT_DIR
    TESTCASE_PATH = setting_param['Path']['testcase_dir_path']
    RESULT_DIR = setting_param['Path']['result_dir_path']

    global n_MaxValue
    n_MaxValue = setting_param['DefaultValue']['n_MaxValue']

def Load_Testdata() -> 'list,dict':
    opetime_info = {}
    datetime_list = []
    try:
        #modify to the expression windows's path
        testcase_files = [tmp.replace('\\','/') for tmp in glob.glob(TESTCASE_PATH)]
        fix_candidate = []

        for test_file in testcase_files:
            with open(test_file,'r') as infile:
                testcase = csv.reader(infile)
                for t in testcase:
                    try:
                        date_info,ip_address,round_time = t

                        date_info = dt.datetime.strptime(date_info,'%Y%m%d%H%M%S')
                        datetime_list.append(date_info)
                        round_time = convert_roundtime(round_time)
                        if round_time == -1:
                            fix_candidate.append(ip_address)

                        opetime_info.setdefault(ip_address,[]).append([date_info,round_time])
                    except ValueError:
                        continue

    except FileNotFoundError:
        print('Prepare testcase csv files!')
        sys.exit()

    period = min(datetime_list).strftime('%Y%m%d_%H%M%S')+'-'+max(datetime_list).strftime('%Y%m%d_%H%M%S')

    return fix_candidate,opetime_info,period


def Extract_Idlinglog(target_servers:list,opetime_info:dict,max_checktime:int=4) -> 'dict':
    idle_log = {}
    for server in list(set(target_servers)):
        server_info = queue.Queue()
        for info in sorted(opetime_info[server]):
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

                    if server_info.empty() and record_flg: # case: the log file cut
                        idle_log[server] = float('inf')

                    check_index += 1
            else:
                pass
    return idle_log


def Idlinglog_ToCsv(idle_log:dict,period:str,n:int):
    with open(RESULT_DIR + f'/n_{n}_{period}.csv', 'w') as outfile:
        writer = csv.writer(outfile)
        for k,v in idle_log.items():
            writer.writerow([k,v])

    print('output : ' + RESULT_DIR + f'/n_{n}_{period}.csv')

def main():
    Init_setting()
    parser = argparse.ArgumentParser()
    parser.add_argument('-n','--N',help=f'Min timeout count. default 4',default=4,action='store_true')
    args = parser.parse_args()

    if args.N:
        try:
            n = int(input('N : '))
        except ValueError as ValE:
            print(ValE)
            print('You pick Natural Number.')
            sys.exit()
        if 1<=n<=n_MaxValue:
            list_fix_candidates,dict_opetime,period = Load_Testdata()
            extracted_idlinginfo = Extract_Idlinglog(list_fix_candidates,dict_opetime,n)
            Idlinglog_ToCsv(extracted_idlinginfo,period,n)
        else:
            print(f'You pick natural number between 1 and {n_MaxValue}.(You can change it setting.json)')
            sys.exit()

if __name__ == '__main__':
    main()
