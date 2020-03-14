#  @data   2019/12/9 16:45
import pymysql


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
    except pymysql.MySQLError:
        connect.rollback()


sql = """CREATE TABLE stations (
         id varchar(255) NOT NULL PRIMARY KEY,
         latitude  float NOT NULL,
         longitude  float NOT NULL,   
         name varchar(255) NOT NULL)"""

# execute_sql(sql)

connect = get_connect()
cursor = connect.cursor()

with open('china_stations.txt') as f:
    for line in f:
        res = line.split(" ")
        res = list(filter(lambda x: False if x == '' else True, res))
        sql = 'INSERT INTO stations VALUES ("%s", %s, %s, "%s")' % tuple([res[0], float(res[1]), float(res[2]), res[4]])
        print(sql)
        cursor.execute(sql)
        print(list(res))

connect.commit()
connect.close()
