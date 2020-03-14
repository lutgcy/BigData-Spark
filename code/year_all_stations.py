#  @data   2019/12/9 12:34
 
from pyspark import SparkContext
import os
import numpy
import re
import pymysql


def get_connect():  # 获取连接
    # connect = pymysql.Connect(host='47.98.52.37', port=3306, user='root', passwd='Gcy2018%s', db='weather', charset='utf8')
    connect = pymysql.Connect(host='localhost', port=3306, user='root', passwd='n3483226', db='test', charset='utf8')
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


sc = SparkContext("local[*]", "weather")


def data_clean(line):  # 去除字母， -9999等无效数据
    temp = re.sub(r'[A-Z]|[a-z]', ' ', line[21:]).replace("-9999", " ")
    res = list(filter(lambda x: False if x == '' else True, (line[:21] + temp).split(" ")))
    if len(res) == 1:  # 数据全为-9999
        return tuple([res[0], '-9999'])
    else:
        return tuple([res[0], " ".join(res[1:])])


def calculate(x):  #
    if x[0][-1] == 'X':
        max_tem = max(list(int(n) for n in x[1].split(" ")))
        return tuple([x[0][:15] + x[0][17:], str(max_tem)])
    elif x[0][-1] == 'N':
        min_tem = min(list(int(n) for n in x[1].split(' ')))
        return tuple([x[0][:15] + x[0][17:], str(min_tem)])
    else:
        ave = "{:.1f}".format(numpy.mean(list(int(n) for n in x[1].split(" "))))
        return tuple([x[0][:15] + x[0][17:], str(ave)])


rdd = (sc.wholeTextFiles("file:///" + os.getcwd() + "/china/")).map(lambda x: x[1].split('\n'))\
        .flatMap(lambda x: x[:-1]).map(data_clean).map(calculate)


def merge_map(tp):  # TMAX   TMIN   TAVG   PRCP   SNWD
    flag = tp[0][-1]
    values = tp[1].split(",")
    values = list(filter(lambda x: False if x == -9999.0 else True, [float(n) for n in values]))
    if len(values) == 0:
        return tuple([tp[0], 'null'])
    if flag == 'X':
        return tuple([tp[0], "{:.1f}".format(max(values) / 10)])
    elif flag == 'N':
        return tuple([tp[0], "{:.1f}".format(min(values) / 10)])
    else:
        return tuple([tp[0], "{:.1f}".format(numpy.mean(values) / 10)])


rdd = rdd.reduceByKey(lambda x, y: x + "," + y).sortByKey(lambda x: x[0]).map(merge_map)


sql = """CREATE TABLE all_city (
         id    varchar(255) NOT NULL PRIMARY KEY,
         TMAX   float,
         TMIN   float,
         TAVG   float,
         PRCP   float,
         SNWD  float
         )"""

execute_sql(sql)
execute_sql("delete from all_city")
db = get_connect()
cursor = db.cursor()

row_dict = {'id': 'null', 'TMAX': 'null', 'TMIN': 'null', 'TAVG': 'null', 'PRCP': 'null', 'SNWD': 'null'}

for e in rdd.collect():  # TMAX   TMIN   TAVG   PRCP   SNWD
    column = e[0][-4:]
    primary_key = e[0][:-4]
    if row_dict['id'] == primary_key or row_dict['id'] == 'null':
        # print(tuple(row_dict.values()))
        row_dict['id'] = primary_key
        row_dict[column] = float(e[1])
    else:
        sql = "INSERT INTO all_city VALUES ('%s', %s, %s, %s, %s, %s)" % tuple(row_dict.values())
        cursor.execute(sql)
        print(tuple(row_dict.values()))
        row_dict['id'], row_dict['TMAX'], row_dict['TMIN'], row_dict['TAVG'], row_dict['PRCP'], row_dict['SNWD'] =\
            'null', 'null', 'null', 'null', 'null', 'null'
        row_dict[column] = float(e[1])
db.commit()
db.close()

sc.stop()
