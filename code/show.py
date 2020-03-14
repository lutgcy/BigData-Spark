import pymysql
import matplotlib.pyplot as plt

import pylab as mpl  # import matplotlib as mpl

# 设置汉字格式
# sans-serif就是无衬线字体，是一种通用字体族。
# 常见的无衬线字体有 Trebuchet MS, Tahoma, Verdana, Arial, Helvetica,SimHei 中文的幼圆、隶书等等
mpl.rcParams['font.sans-serif'] = ['FangSong']  # 指定默认字体
mpl.rcParams['axes.unicode_minus'] = False  # 解决保存图像是负号'-'显示为方块的问题


def get_connect():  # 获取连接
    connect = pymysql.Connect(host='47.98.52.37', port=3306, user='root', passwd='Gcy2018%s', db='weather', charset='utf8')
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


sql = '''SELECT * FROM weather.every_year_china 
        where id >= 1951 and id <= 2018;'''
cursor = execute_sql(sql)

TMAX, TMIN, TAVG, PRCP = [], [], [], []

for e in cursor.fetchall():
    if -60 <= float(e[1]) <= 60:
        TMAX.append(e[1])
    else:
        TMAX.append(None)
    TMIN.append(e[2])
    TAVG.append(e[3])
    PRCP.append(e[4] * 365)

index = TMAX.index(None)
TMAX[index] = (TMAX[index - 1] + TMAX[index + 1]) / 2

print(TMAX)
print(TMIN)
print(TAVG)
print(PRCP)


# ——————————————————————————我是一条分割线—————————————————————————————

plt.plot([i for i in range(1951, 2019)], TMAX, 'r', label='最高气温')
plt.plot([i for i in range(1951, 2019)], TMIN, 'b--', label='最低气温')
plt.plot([i for i in range(1951, 2019)], TAVG, 'g-.', label='平均气温')


plt.xticks([i for i in range(1951, 2019)], fontsize=7)

plt.title('全国气温变化曲线', fontproperties='SimHei', fontsize=15, color='green')
plt.xlabel('年份(1951-2018) / 年', fontproperties='SimHei', fontsize=15, color='green')
plt.ylabel('气温 / ℃', fontproperties='SimHei', fontsize=15, color='green')

plt.legend(bbox_to_anchor=[1, 1.1])
plt.grid()
plt.show()

# ——————————————————————————我是一条分割线—————————————————————————————

plt.plot([i for i in range(1951, 2019)], PRCP, 'g.-', label='全年降水量')

plt.xticks([i for i in range(1951, 2019)], fontsize=7)

plt.title('全国气象站年平均降水量变化曲线', fontproperties='SimHei', fontsize=15, color='red')
plt.xlabel('年份(1951-2018) / 年', fontproperties='SimHei', fontsize=15, color='red')
plt.ylabel('降水量 / mm', fontproperties='SimHei', fontsize=15, color='red')

plt.legend(bbox_to_anchor=[0.1, 1.1])
plt.grid()
plt.show()
