import pandas as pd
import time
import sqlite3
import datetime


# 时间转时间戳方法
def timestr_to_timestamp(timestr):
    struct_time = time.strptime(timestr, '%Y-%m-%d %H:%M:%S')
    seconds = time.mktime(struct_time)
    return seconds


# 连接数据库
def connect_db(db_name):
    return sqlite3.connect(db_name)


# 初始化table
def init_table(c, role_log_df, social_inter_df, trade_df, level_df):
    c.execute('''CREATE TABLE IF NOT EXISTS dwd_user_role_log_df
                     (dt text, player_id text, device_type text, location text, event_timestamp integer, event_type text)''')
    c.execute('''CREATE TABLE IF NOT EXISTS dwd_user_social_inter_df
                     (dt text, player_id text, device_type text, location text, event_timestamp integer,social_type text,social_content text)''')
    c.execute('''CREATE TABLE IF NOT EXISTS dwd_income_trade_df
                     (dt text, player_id text, device_type text, location text, event_timestamp integer,item_id text,item_name text,trade_amt real)''')
    c.execute('''CREATE TABLE IF NOT EXISTS dwd_user_level_df
                     (dt text, player_id text, device_type text, location text, event_timestamp integer, level_detail integer)''')
    # 提交事务
    conn.commit()
    role_log_df.to_sql('dwd_user_role_log_df', conn, if_exists='replace', index=False)
    social_inter_df.to_sql('dwd_user_social_inter_df', conn, if_exists='replace', index=False)
    trade_df.to_sql('dwd_income_trade_df', conn, if_exists='replace', index=False)
    level_df.to_sql('dwd_user_level_df', conn, if_exists='replace', index=False)


# 原始数据etl
def ods_etl_func(ods_data):
    # 定义etl后数据list
    role_log_list = []
    social_inter_list = []
    trade_list = []
    level_list = []
    for y in range(len(ods_data)):
        event_timestamp = int(timestr_to_timestamp(ods_data['EventTimestamp'][y]))
        dt = ods_data['EventTimestamp'][y][0:10]
        # 抽取公共字段
        tmp_list = [dt, ods_data['PlayerID'][y], ods_data['DeviceType'][y], ods_data['Location'][y], event_timestamp]
        if (ods_data['EventType'][y] == 'LevelComplete'):
            # 加工等级表
            tmp_list.append(ods_data['EventDetails'][y].split(": ")[1])
            level_list.append(tmp_list)
        elif (ods_data['EventType'][y] == 'InAppPurchase'):
            # 加工订单表
            tmp_list.append('')
            tmp_list.append('')
            tmp_list.append(ods_data['EventDetails'][y].split(": ")[1])
            trade_list.append(tmp_list)
        elif (ods_data['EventType'][y] == 'SocialInteraction'):
            # 加工社交表
            tmp_list.append(ods_data['EventDetails'][y].split(": ")[0])
            tmp_list.append(ods_data['EventDetails'][y].split(": ")[1])
            social_inter_list.append(tmp_list)
        else:
            # 加工登入登出表
            if ods_data['EventType'][y] == 'SessionStart':
                tmp_list.append('login')
            else:
                tmp_list.append('logout')
            role_log_list.append(tmp_list)
    # 角色登入登出数据df
    role_log_df = pd.DataFrame(role_log_list,
                               columns=['dt', 'player_id', 'device_type', 'location', 'event_timestamp', 'event_type'])
    # 社交数据df
    social_inter_df = pd.DataFrame(social_inter_list,
                                   columns=['dt', 'player_id', 'device_type', 'location', 'event_timestamp',
                                            'social_type', 'social_content'])
    # 订单数据df
    trade_df = pd.DataFrame(trade_list,
                            columns=['dt', 'player_id', 'device_type', 'location', 'event_timestamp', 'item_id',
                                     'item_name', 'trade_amt'])
    # 等级数据df
    level_df = pd.DataFrame(level_list,
                            columns=['dt', 'player_id', 'device_type', 'location', 'event_timestamp', 'level_detail'])
    return role_log_df, social_inter_df, trade_df, level_df


if __name__ == '__main__':
    conn = connect_db('dataware.db')
    c = conn.cursor()
    # panda读取csv数据
    game_events_df = pd.read_csv('game_events.csv')
    role_log_df, social_inter_df, trade_df, level_df = ods_etl_func(game_events_df)
    # print(role_log_df)
    init_table(c, role_log_df, social_inter_df, trade_df, level_df)
    conn.close()  # 关闭连接
