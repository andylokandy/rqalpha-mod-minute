import six
from datetime import date
from dateutil.relativedelta import relativedelta
from rqalpha.data.base_data_source import BaseDataSource, FIELDS_REQUIRE_ADJUSTMENT, adjust_bars
import numpy as np
import pandas as pd
import datetime
from rqdatac import *
import rqdatac as rq


def get_minute_bar(stock, start_datetime, end_datetime):
    """
    stock：输入单个股票代码; 
    start_datetime：开始日期与时间，时间必须为交易时间范围内9:31-15:00; 
    end_datetime：结束日期与时间，时间必须为交易时间范围内9:31-15:00; 
    """

    #读取Price_min_Datas
    Price_min_Datas = pd.read_hdf('Price_min_Datas.h5', 'Price_min_Datas')
    #输入转datetime
    start_datetime = datetime.datetime.strptime(start_datetime,
                                                "%Y-%m-%d %H:%M")
    end_datetime = datetime.datetime.strptime(end_datetime, "%Y-%m-%d %H:%M")

    #内部参数
    start_date = []
    end_date = []
    times = [0]  #获取次数（针对所获数据长度包含已有数据长度）

    #比对
    if stock not in Price_min_Datas.index.levels[0]:
        #没有该股，完整日获取
        print('没有该股，完整日获取')
        start_date = [start_datetime.strftime('%Y-%m-%d')]
        end_date = [end_datetime.strftime('%Y-%m-%d')]

    else:
        #有该股票
        #获取原数据该股票的datetime范围
        first_datetime = Price_min_Datas.loc[stock].index[0]
        last_datetime = Price_min_Datas.loc[stock].index[-1]

        if first_datetime <= start_datetime <= last_datetime and first_datetime <= end_datetime <= last_datetime:
            #所获数据在已有数据范围内，直接返回
            print('所获数据在已有数据范围内，直接返回')
            return Price_min_Datas.loc[stock].loc[start_datetime:end_datetime]

        elif start_datetime <= first_datetime and first_datetime <= end_datetime <= last_datetime:
            #所获数据有重叠在左侧
            print('所获数据有重叠在左侧')
            start_date = [start_datetime.strftime('%Y-%m-%d')]
            end_date = [first_datetime.strftime('%Y-%m-%d')]

        elif start_datetime <= first_datetime and last_datetime <= end_datetime:
            #所获数据长度包含已有数据长度
            print('所获数据长度包含已有数据长度')
            start_date = [
                start_datetime.strftime('%Y-%m-%d'),
                last_datetime.strftime('%Y-%m-%d')
            ]
            end_date = [
                first_datetime.strftime('%Y-%m-%d'),
                end_datetime.strftime('%Y-%m-%d')
            ]

            times = [0, 1]  #运行两次，对应两组参数

        elif first_datetime <= start_datetime <= last_datetime and last_datetime <= end_datetime:
            #所获数据有重叠在右侧
            print('所获数据有重叠在右侧')
            start_date = [last_datetime.strftime('%Y-%m-%d')]
            end_date = [end_datetime.strftime('%Y-%m-%d')]

        else:
            return print('获取数据要求有误！')
    try:
        for i in times:
            #获取所需数据部分
            price_data = rq.get_price(stock,
                                      start_date=start_date[i],
                                      end_date=end_date[i],
                                      frequency='1m',
                                      fields=None,
                                      adjust_type='none',
                                      skip_suspended=False,
                                      market='cn',
                                      expect_df=True)
            #补齐所需数据
            Price_min_Datas = price_data.combine_first(Price_min_Datas)
        #储存
        Price_min_Datas.to_hdf('Price_min_Datas.h5', 'Price_min_Datas')

    except Exception as e:
        print('无法获取该数据')
        # print(e.args) #返回异常的错误编号和描述字符串
        print(repr(e))  #返回较全的异常信息，包括异常信息的类型
    #返回
    return Price_min_Datas.loc[stock].loc[start_datetime:end_datetime]


class MinuteDataSource(BaseDataSource):
    def __init__(self, path):
        super(MinuteDataSource, self).__init__(path)

    @staticmethod
    def get_minute_k_data(instrument, start_dt, end_dt):
        return get_minute_bar(instrument.order_book_id,
                              start_dt.strftime('%Y-%m-%d'),
                              end_dt.strftime('%Y-%m-%d'))

    def get_bar(self, instrument, dt, frequency):
        if frequency != '1m':
            return super(MinuteDataSource,
                         self).get_bar(instrument, dt, frequency)

        bar_data = self.get_minute_k_data(instrument, dt, dt)
        return bar_data.iloc[0].to_dict()

    def history_bars(self,
                     instrument,
                     bar_count,
                     frequency,
                     fields,
                     dt,
                     skip_suspended=True,
                     include_now=False,
                     adjust_type='pre',
                     adjust_orig=None):
        if frequency != '1m':
            return super(MinuteDataSource,
                         self).history_bars(self, instrument, bar_count,
                                            frequency, fields, dt,
                                            skip_suspended, include_now,
                                            adjust_type, adjust_orig)

        start_dt_loc = self.get_trading_calendar().get_loc(
            dt.replace(hour=0, minute=0, second=0,
                       microsecond=0)) - (bar_count / 240) - 1
        start_dt = self.get_trading_calendar()[start_dt_loc].replace(
            hour=9, minute=31, second=0, microsecond=0)

        bar_data = self.get_minute_k_data(instrument, start_dt,
                                          dt).tail(bar_count)

        bar_data = adjust_bars(
            bar_data, self.get_ex_cum_factor(instrument.order_book_id), fields,
            adjust_type, adjust_orig)

        if fields is None:
            return bar_data.reset_index().to_numpy()
        else:
            return bar_data[fields].reset_index().to_numpy()

    def available_data_range(self, frequency):
        return date(2005, 1, 1), date.today()