import pymysql
from pyecharts import Geo, Style

import os
print(os.getcwd())


def get_connect():  # 获取连接
    # connect = pymysql.Connect(host='47.98.52.37', port=3306, user='root', passwd='Gcy2018%s', db='weather', charset='utf8')
    connect = pymysql.Connect(host='localhost', port=3306, user='root', passwd='n3483226', db='weather', charset='utf8')
    return connect


def execute_sql(sql):  # 执行SQL语句并返回执行结果的游标
    try:
        connect = get_connect()
        cursor = connect.cursor()
        cursor.execute(sql)
        connect.commit()
        connect.close()
        return cursor
    except:
        connect.rollback()


cursor = execute_sql("SELECT * FROM weather.stations;")

stations_dict = dict()
for e in cursor.fetchall():
    stations_dict.update({e[0]: tuple([e[1], e[2], e[3]])})

sql = "SELECT * FROM all_city;"
cursor = execute_sql(sql)
TAVG, PRCP, TMAX, TMIN = [], [], [], []  # 数据对
x_y = {}  # 坐标对

for key, value in stations_dict.items():
    x_y.update({value[2]: [value[1], value[0]]})

for e in cursor.fetchall():
    if e[0][-4:] == "2018":  # 选择年份
        key = e[0][0:-4]
        TAVG.append(tuple([stations_dict[e[0][:-4]][2], e[3]]))
        PRCP.append(tuple([stations_dict[e[0][:-4]][2], e[4]]))
        TMAX.append(tuple([stations_dict[e[0][:-4]][2], e[1]]))
        TMIN.append(tuple([stations_dict[e[0][:-4]][2], e[2]]))

res = []
for e in PRCP:
    if e[1] is not None:
        res.append(tuple([e[0], int(e[1] * 365)]))
PRCP = res

res = []
for e in TAVG:
    if e[1] is not None:
        res.append(e)
TAVG = res

res = []
for e in TMAX:
    if e[1] is not None:
        res.append(e)
TMAX = res

res = []
for e in TMIN:
    if e[1] is not None:
        res.append(e)
TMIN = res

PRCP_city = [i[0] for i in PRCP]
PRCP_value = [i[1] for i in PRCP]

TAVG_city = [i[0] for i in TAVG]
TAVG_value = [i[1] for i in TAVG]

TMAX_city = [i[0] for i in TMAX]
TMAX_value = [i[1] for i in TMAX]

TMIN_city = [i[0] for i in TMIN]
TMIN_value = [i[1] for i in TMIN]
#
# print(SNWD)
# print(max([i[1] for i in SNWD]))
# print(min([i[1] for i in SNWD]))

# 导入自定义的地点经纬度
# attr：标签名称（在例子里面就是地点）
# value：数值（在例子里就是流动人员）
# visual_range：可视化的数值范围
# symbol_size：散点的大小
# visual_text_color：标签颜色
# is_piecewise ：颜色是否分段显示（False为渐变，True为分段）
# is_visualmap：是否映射（数量与颜色深浅是否挂钩）
# maptype ：地图类型，可以是中国地图，省地图，市地图等等
# visual_split_number ：可视化数值分组
# geo_cities_coords：自定义的经纬度

style = Style(title_color="#fff", title_pos="center", width=1600, height=900, background_color="#404a59")

pieces = [
    {'max': 300, 'label': '0-300', 'color': '#FF7F00'},
    {'min': 300, 'max': 600, 'label': '300-600', 'color': '#EEE685'},
    {'min': 600, 'max': 1200, 'label': '600-1200', 'color': '#FFD700'},
    {'min': 1200, 'max': 1600, 'label': '1200-1600', 'color': '#ccffff'},
    {'min': 1600, 'max': 2400, 'label': '1600-2400', 'color': '#0099ff'},
    {'min': 2400, 'max': 3200, 'label': '2400-3200', 'color': '#0066ff'},
    {'min': 3200, 'label': '3200以上', 'color': '#0000ff'}  # 有下限无上限
]

# ——————————————————————————我是一条分割线—————————————————————————————
geo = Geo('全国2018年各气象站降水量分布图(单位/mm)', **style.init_style)
geo.add("", attr=PRCP_city, value=PRCP_value, symbol_size=10, visual_text_color="#fff", is_piecewise=True,
        is_visualmap=True, maptype='china',
        pieces=pieces,  # 注意，要想pieces生效，必须is_piecewise = True,
        geo_cities_coords=x_y)

geo.render("maps/PRCP.html")
# ——————————————————————————我是一条分割线—————————————————————————————

g = Geo('全国2018年各气象站平均气温分布图(单位/℃)', **style.init_style)
g.add("", attr=TAVG_city, value=TAVG_value, visual_range=[-4, 32], visual_split_number=9, symbol_size=10,
      visual_text_color="#fff", is_piecewise=True,
      is_visualmap=True, maptype='china',
      geo_cities_coords=x_y)

g.render("maps/TAVG.html")
# ——————————————————————————我是一条分割线—————————————————————————————

g = Geo('全国2018年各气象站最高气温分布图(单位/℃)', **style.init_style)
g.add("", attr=TMAX_city, value=TMAX_value, visual_range=[0, 40], visual_split_number=8, symbol_size=10,
      visual_text_color="#fff", is_piecewise=True,
      is_visualmap=True, maptype='china',
      geo_cities_coords=x_y)

g.render("maps/TMAX.html")
# ——————————————————————————我是一条分割线—————————————————————————————
g = Geo('全国2018年各气象站最低气温分布图(单位/℃)', **style.init_style)
g.add("", attr=TMIN_city, value=TMIN_value, visual_range=[-40, 10], visual_split_number=5, symbol_size=10,
      visual_text_color="#fff", is_piecewise=True,
      is_visualmap=True, maptype='china',
      geo_cities_coords=x_y)

g.render("maps/TMIN.html")
