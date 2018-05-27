from Clawer_Base.logger import logger
from Clawer_Base.proxy_clawers import Fetch_proxy
from Clawer_Base.user_agents import User_agents
from Clawer_Base.clawer_frame import Clawer
from multiprocessing.dummy import Pool as ThreadPool
import json
import pandas as pd
import os
import prettytable

def json_read(file_path):
    """用于读取城市-代码字典"""
    with open(file_path, 'r') as load_f:
        load_dict = json.load(load_f)
        return load_dict

name2codeDict = json_read('name2code.json')
code2nameDict = json_read('code2name.json')

def name2code(city):
    if not city.isalpha():
        return name2codeDict[city]
    else:
        return city


def code2name(city):
    if city.isalpha():
        return code2nameDict[city]
    else:
        return city



class Flight_clawer(Clawer):
    """抓取携程航班数据 http://flights.ctrip.com/booking/XMN-BJS-day-1.html?DDate1=2018-03-15"""
    _res_key = {
            'fn': '航班号',
           'dcc': '出发城市代码',
           'dcn': '出发城市名称',
           'dpbn': '出发机场名称',
           'dpc': '出发机场代码',
           'dsmsn': '出发航站楼',
           'dt'	: '出发时间',
           'acc': '到达城市代码',
           'acn': '到达城市名称',
           'alc': '公司代码',
           'apbn': '到达机场名称',
           'apc': '到达机场代码',
           'asmsn': '到达航站楼',
           'at': '到达时间',
           'lcfp': '公务舱价',
           'lp'	: '经济舱价',
           'pr'	: '准点率',
           'c': '型号',
           's'	: '尺寸',
           'sdft': '共享航班',
           'tax': '税'
           }

    def __init__(self, Dcity='广州', Acity='深圳', DDate='2018-03-15'):
        self.url = 'http://flights.ctrip.com/domesticsearch/search/SearchFirstRouteFlights?DCity1={}&ACity1={}&SearchType=S&DDate1={}'.format(Dcity, Acity, DDate)
        self.Dcity = name2code(Dcity)
        self.Acity = name2code(Acity)
        self.DDate = DDate
        self.params = ''
        self.headers = User_agents().get_headers()
        self.proxys = {'proxies': ''}
        self.cookies = self._cookies
        self.req_id = '%s_%s' % (Dcity, Acity)

    def scheduler(self):
        if isinstance(self.respond, dict):
            if self.respond.get("Error"):
                error_info = self.respond["Error"]["Message"]
                self.req_stat(error_info)
                logger.info('%s %s-%s %s'%(self.DDate, self.Dcity, self.Acity, error_info))
            else:
                return self.parser()
        else:
            return self.requestor()

    def parser(self):
        # print(self.respond)
        if self.respond:
            fli_info = self.respond.get('fis')
            if fli_info:
                info_list = []
                for a_fli in fli_info:
                    if isinstance(a_fli['confort'], dict):
                        confort_dict = a_fli['confort']
                        a_fli.update(confort_dict)
                    else:
                        try:
                            confort_dict = json.loads(a_fli['confort'])
                            a_fli.update(confort_dict)
                        except:
                            pass
                            # logger.info(a_fli['confort'])

                    if isinstance(a_fli['cf'], dict):
                        cf_dict = a_fli['cf']
                        a_fli.update(cf_dict)
                    else:
                        cf_dict = json.loads(a_fli['cf'])
                        a_fli.update(cf_dict)
                    info_list.append(a_fli)
                df = pd.DataFrame(info_list)
                df = df[list(self._res_key.keys())]
                df.rename(columns=self._res_key, inplace=True)
            # print(df)
                return df
            else:
                return
        else:
            return None

    def process(self):
        # logger.info('开始抓取 %s  %s至%s 航班信息' % (self.DDate,
        #                                       code2name(self.Dcity),
        #                                      code2name(self.Acity),))
        print('开始抓取 %s  %s 至 %s 航班信息' % (self.DDate, code2name(self.Dcity), code2name(self.Acity)))
        if self.stat_controller():
            self.requestor()
            return self.scheduler()
        else:
            return None

    def cinit(self):
        self.cookie_init()
        self.stat_init()

    def stat_controller(self):
        """通过历史统计数据确定是否要抓取航线"""
        req_id = '%s-%s' % (self.Dcity, self.Acity)
        if req_id in Flight_clawer.req_info:
            if '不通航' in Flight_clawer.req_info[req_id]:
                return False
            else:
                return True
        else:
            return True

def datelist(start='20180322', end='20180123'):
    date_df = pd.date_range(start, end)
    date_list = [date.strftime('%Y-%m-%d') for date in date_df]
    return date_list

def creat_Qdict(a_list):
    _code2name = {}
    _name2code = {}
    for i in a_list:
        _code2name[i['code']] = i['name']
        _name2code[i['name']] = i['code']
    with open("code2name.json", "w") as f:
        json.dump(_code2name, f)
    with open("name2code.json", "w") as f:
        json.dump(_name2code, f)

def main(start, end):
    dateList = datelist(start, end)
    cityList = list(code2nameDict.keys())
    Flight_clawer().cookie_init()
    Fetch_proxy().cinit()
    def by_date(Ddate):
        def by_Dcity(Dcity):
            def by_Acity(Acity):
                if Dcity != Acity:
                    a_clawer = Flight_clawer(Dcity=Dcity, Acity=Acity, DDate=Ddate)
                    return a_clawer.process()
                else:
                    return
            pool = ThreadPool()
            results = pool.map(by_Acity, cityList)
            pool.close()
            pool.join()
            # print(results)
            if os.path.exists('Flight_result/%s' % Ddate):
                pass
            else:
                os.makedirs('Flight_result/%s' % Ddate)
            results = [i for i in results if type(i) == pd.core.frame.DataFrame]
            # print(results)
            if results:
                df = pd.concat(results)
                df.to_csv('Flight_result/%s/%s_%s.csv' % (Ddate, code2name(Dcity), Ddate))
            else:
                print('%s-%s 没有航班'% (Ddate, code2name(Dcity)))

        pool_lv1 = ThreadPool(4)
        pool_lv1.map(by_Dcity, cityList)
        pool_lv1.close()
        pool_lv1.join()

    pool_lv2 = ThreadPool(1)
    pool_lv2.map(by_date, dateList)
    pool_lv2.close()
    pool_lv2.join()
    Fetch_proxy().save_proxy(Fetch_proxy.proxy_pool)
    Flight_clawer().stat_save()


def param_info(info_dict):
    info_table = prettytable.PrettyTable(['项目', '描述'])
    for key in list(info_dict.keys()):
        info_table.add_row([key, info_dict[key]])
    info_table.align = 'l'
    logger.info('\n' + str(info_table))


if __name__ == '__main__':
    info_dict = {'名称': '航班信息抓取工具V1.0',
                 '邮箱': '575548935@qq.com',
                 '起始时间': '20180528',
                 '终止时间': '20180531'
                 }
    param_info(info_dict)
    start = info_dict['起始时间']
    end = info_dict['终止时间']
    main(start, end)
    # print(datelist('20180322', '20180403'))


