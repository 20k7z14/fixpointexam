import csv,glob,sys,queue,json
import datetime as dt
import argparse

def Init_setting():
    json_open = open('../setting.json','r')
    setting_param = json.load(json_open)

    global TESTCASE_PATH,RESULT_DIR
    TESTCASE_PATH = setting_param['Path']['testcase_dir_path']
    RESULT_DIR = setting_param['Path']['result_dir_path']

def Convert_Roundtime(str_rt:str) -> 'int':
    if str_rt == '-':
        return -1
    else:
        return int(str_rt)

def Load_Testdata() -> 'list,dict':
    opetime_info = {}
    datetime_list = []
    try:
        #modify to the expression windows's path
        test_file = TESTCASE_PATH
        fix_candidate = set()

        with open(test_file,'r') as infile:
            testcase = csv.reader(infile)
            for t in testcase:
                try:
                    date_info,ip_address,round_time = t

                    date_info = dt.datetime.strptime(date_info,'%Y%m%d%H%M%S')
                    round_time = Convert_Roundtime(round_time)
                    datetime_list.append(date_info)

                    if round_time == -1:
                        fix_candidate.add(ip_address)

                    opetime_info.setdefault(ip_address,[]).append([date_info,round_time])
                except ValueError:
                    continue
        if datetime_list != []:
            period = min(datetime_list).strftime('%Y%m%d_%H%M%S')+'-'+max(datetime_list).strftime('%Y%m%d_%H%M%S')
        else:
            period = 'empty'
    except FileNotFoundError:
        print('You prepare testcase csv files!')
        sys.exit()

    return list(fix_candidate),opetime_info,period


def extract_log(target_servers:list,opetime_info:dict) -> 'dict':
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
                record_flg = True

                while not server_info.empty():
                    af_time,af_rdtime = server_info.get()

                    if af_rdtime != -1 and record_flg: # case: set record flag and responded
                        between = af_time - begin_idlingtime
                        total_idlingtime += int(between.total_seconds()*1000) + af_rdtime
                        idle_log[server] = total_idlingtime
                        record_flg = False

                    if server_info.empty() and record_flg: # case: the log file cut
                        idle_log[server] = float('inf')
            else:
                pass
    return idle_log


def Idlinglog_ToCsv(idle_log:dict,period:str):
    with open(RESULT_DIR + f'/{period}.csv', 'w') as outfile:
        writer = csv.writer(outfile)
        for k,v in idle_log.items():
            writer.writerow([k,v])

    print('output : ' + RESULT_DIR + f'/{period}.csv')


# Before  all sequence
def Init_setting():
    json_open = open('../setting.json','r')
    setting_param = json.load(json_open)

    global TESTCASE_PATH,RESULT_DIR
    TESTCASE_PATH = setting_param['Path']['testcase_dir_path']
    RESULT_DIR = setting_param['Path']['result_dir_path']

def main():
    Init_setting()
    list_candidates,dict_opetime,period = Load_Testdata()
    extracted_info = extract_log(list_candidates,dict_opetime)
    Idlinglog_ToCsv(extracted_info,period)

if __name__ == '__main__':
    main()
