import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import pandas as pd
import sqlite3

# 连接到 SQLite 数据库
conn = sqlite3.connect('dataware.db')
# SELECT datetime('1092941466','unixepoch','localtime','start of day');

social_cnt_query = """
select tmpb.player_id,login_timestamp,logout_timestamp,count(1) as social_cnt
from
(
    select player_id,location,dt,login_timestamp,logout_timestamp 
    from
    (
        select player_id,location,dt,event_timestamp as login_timestamp,cast(lead(event_timestamp) over(partition by player_id order by event_timestamp asc) as text) as logout_timestamp,event_type
        from dwd_user_role_log_df 
    )tmpa
    where event_type = 'login'
)tmpb
left join
(
    select player_id,social_content,cast(event_timestamp as text) as event_timestamp
    from dwd_user_social_inter_df
)tmpc on tmpb.player_id = tmpc.player_id and tmpc.event_timestamp >= tmpb.login_timestamp and tmpc.event_timestamp <= tmpb.logout_timestamp
group by tmpb.player_id,login_timestamp,logout_timestamp
"""
social_cnt_df = pd.read_sql_query(social_cnt_query, conn)

avg_time_query = """
select dt as date,location,sum((logout_timestamp-login_timestamp)/60)/count(distinct player_id) as avg_time
from
(
    select player_id,location,dt,login_timestamp,logout_timestamp
    from
    (
        select player_id,location,dt,event_timestamp as login_timestamp,coalesce(lead(event_timestamp) over(partition by dt,player_id order by event_timestamp asc),strftime('%s',dt)+16*3600) as logout_timestamp,event_type
        from dwd_user_role_log_df
    )tmpa
    where event_type = 'login'
    union all
    select player_id,location,dt,strftime('%s',dt)-8*3600 as login_timestamp,event_timestamp as logout_timestamp
    from
    (
        select player_id,
               location,
               dt,
               event_timestamp,
               event_type,
               row_number() over(partition by player_id,dt order by event_timestamp asc) as rn
        from dwd_user_role_log_df
    )tmpb
    where event_type = 'logout' and rn = 1
)tmpc
group by dt,location
union all 
select dt as date,'All' as location,sum((logout_timestamp-login_timestamp)/60)/count(distinct player_id) as avg_time
from
(
    select player_id,location,dt,login_timestamp,logout_timestamp
    from
    (
        select player_id,location,dt,event_timestamp as login_timestamp,coalesce(lead(event_timestamp) over(partition by dt,player_id order by event_timestamp asc),strftime('%s',dt)+16*3600) as logout_timestamp,event_type
        from dwd_user_role_log_df
    )tmpa
    where event_type = 'login'
    union all
    select player_id,location,dt,strftime('%s',dt)-8*3600 as login_timestamp,event_timestamp as logout_timestamp
    from
    (
        select player_id,
               location,
               dt,
               event_timestamp,
               event_type,
               row_number() over(partition by player_id,dt order by event_timestamp asc) as rn
        from dwd_user_role_log_df
    )tmpb
    where event_type = 'logout' and rn = 1
)tmpc
group by dt
order by dt, location asc
"""

avg_time_df = pd.read_sql_query(avg_time_query, conn)

# 读取数据
income_query = """
        select 
            dt as date,
            location,
            sum(trade_amt) as amount
        from dwd_income_trade_df 
        group by dt, location
        union all 
        select 
            dt as date,
            'All' as location,
            sum(trade_amt) as amount
        from dwd_income_trade_df 
        group by dt
        order by dt, location asc
    """
income_df = pd.read_sql_query(income_query, conn)

dau_query = """
    select dt as date,location,count(distinct player_id) as dau
    from dwd_user_role_log_df
    group by dt,location
    union all 
    select dt as date,'All' as location,count(distinct player_id) as dau
    from dwd_user_role_log_df
    group by dt
    order by dt, location asc
"""
dau_df = pd.read_sql_query(dau_query, conn)

# 关闭连接
conn.close()

# 创建 Dash 应用
app = dash.Dash(__name__)

app.layout = html.Div([
    html.H2("收入仪表盘"),
    html.H3("城市筛选框"),
    dcc.Dropdown(
        id='location-income',
        options=[{'label': location, 'value': location} for location in income_df['location'].unique()],
        value=income_df['location'].unique()[0],
        clearable=False
    ),

    dcc.Graph(id='income-graph'),
    html.H2("dau仪表盘"),
    html.H3("城市筛选框"),
    dcc.Dropdown(
        id='location-dau',
        options=[{'label': location, 'value': location} for location in dau_df['location'].unique()],
        value=dau_df['location'].unique()[0],
        clearable=False
    ),

    dcc.Graph(id='dau-graph'),

    html.H2("平均会话时长仪表盘"),
    html.H3("城市筛选框"),
    dcc.Dropdown(
        id='location-avg-time',
        options=[{'label': location, 'value': location} for location in avg_time_df['location'].unique()],
        value=avg_time_df['location'].unique()[0],
        clearable=False
    ),

    dcc.Graph(id='avg-time-graph'),

    html.H1("每次会话社交互动次数展示"),
    dash_table.DataTable(
        id='table',
        columns=[{"name": col, "id": col} for col in social_cnt_df.columns],
        data=social_cnt_df.to_dict('records'),
        style_table={'height': '400px', 'overflowY': 'auto'},
        style_cell={
            'textAlign': 'left',
            'padding': '10px',
            'whiteSpace': 'normal'
        },
        style_header={
            'backgroundColor': 'lightblue',
            'fontWeight': 'bold'
        },
        style_data={
            'backgroundColor': 'white',
            'color': 'black'
        },
        editable=False,
        filter_action="native",
        sort_action="native",
        page_action="native",
        page_size=10
    )
])

# 回调函数，根据选择的国家更新图表
@app.callback(
    Output('income-graph', 'figure'),
    [Input('location-income', 'value')]
)


def update_graph(selected_location):
    filtered_df = income_df[income_df['location'] == selected_location]
    return {
        'data': [
            {'x': filtered_df['date'], 'y': filtered_df['amount'], 'type': 'bar', 'name': selected_location}
        ],
        'layout': {
            'title': f'{selected_location} 收入数据'
        }
    }


# 回调函数，根据选择的国家更新图表
@app.callback(
    Output('dau-graph', 'figure'),
    [Input('location-dau', 'value')]
)
def update_graph(selected_location):
    filtered_df = dau_df[dau_df['location'] == selected_location]
    return {
        'data': [
            {'x': filtered_df['date'], 'y': filtered_df['dau'], 'type': 'bar', 'name': selected_location}
        ],
        'layout': {
            'title': f'{selected_location} dau数据'
        }
    }


# 回调函数，根据选择的国家更新图表
@app.callback(
    Output('avg-time-graph', 'figure'),
    [Input('location-avg-time', 'value')]
)
def update_graph(selected_location):
    filtered_df = avg_time_df[avg_time_df['location'] == selected_location]
    return {
        'data': [
            {'x': filtered_df['date'], 'y': filtered_df['avg_time'], 'type': 'bar', 'name': selected_location}
        ],
        'layout': {
            'title': f'{selected_location} 平均回话时长数据'
        }
    }


# 运行应用
if __name__ == '__main__':
    app.run_server(port=8051, debug=True)
