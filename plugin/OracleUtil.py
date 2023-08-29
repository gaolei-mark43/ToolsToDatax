import cx_Oracle
from DBUtils.PooledDB import PooledDB


class OracleConfig:
    """
    oracle参数处理
    """

    def __init__(self, config):
        self.host = config['ip'] if 'ip' in config else ''
        self.user = config['username'] if 'username' in config else ''
        self.password = config['password'] if 'password' in config else ''
        self.sid = config['sid'] if 'sid' in config else ''
        self.port = config['port'] if 'port' in config else '3306'
        # mincached : 启动时开启的闲置连接数量(缺省值 0 以为着开始时不创建连接)
        self.min_cached = config['min_cached'] if 'min_cached' in config else 10
        # maxcached : 连接池中允许的闲置的最多连接数量(缺省值 0 代表不闲置连接池大小)
        self.max_cached = config['max_cached'] if 'max_cached' in config else 10
        # maxshared : 共享连接数允许的最大数量(缺省值 0 代表所有连接都是专用的)如果达到了最大数量,被请求为共享的连接将会被共享使用
        self.max_shared = config['max_shared'] if 'max_shared' in config else 20
        # maxconnections : 创建连接池的最大数量(缺省值 0 代表不限制)
        self.max_connections = config['max_connections'] if 'max_connections' in config else 100
        # blocking : 设置在连接池达到最大数量时的行为(缺省值 0 或 False 代表返回一个错误<toMany......>; 其他代表阻塞直到连接数减少,连接被分配)
        self.blocking = config['blocking'] if 'blocking' in config else True
        # maxusage : 单个连接的最大允许复用次数(缺省值 0 或 False 代表不限制的复用).当达到最大数时,连接会自动重新连接(关闭和重新打开)
        self.max_usage = config['max_usage'] if 'max_usage' in config else 0


class OraclePool:
    """
    Oracle连接池
    """

    def __init__(self, oracleconfig):
        self.host = oracleconfig.host
        self.user = oracleconfig.user
        self.password = oracleconfig.password
        self.sid = oracleconfig.sid
        self.port = oracleconfig.port
        self.mincached = oracleconfig.min_cached
        self.maxcached = oracleconfig.max_cached
        self.maxshared = oracleconfig.max_shared
        self.maxconnections = oracleconfig.max_connections
        self.blocking = oracleconfig.blocking
        self.maxusage = oracleconfig.max_usage
        self.pool = None

    def creatPool(self):
        config = {
            "user": self.user,
            "password": self.password,
            "dsn": self.host + ":" + self.port + "/" + self.sid
        }
        self.pool = PooledDB(creator=cx_Oracle, mincached=self.mincached, maxcached=self.maxcached,
                             maxshared=self.maxshared, maxconnections=self.maxconnections, blocking=self.blocking,
                             maxusage=self.maxusage, **config)
        return self.pool


class Oracle:

    def __init__(self, pool):
        self.pool = pool
        self.cursor = None
        self.conn = None

    def __enter__(self):
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        return self.cursor

    def __exit__(self, type, value, tb):
        if tb is None:
            print("提交当前事务")
            self.conn.commit()
        else:
            print("回滚当前事务")
            self.conn.rollback()
        self.cursor.close()
        self.cursor.close()


class OracleUtil:
    """
        Oracle工具类
    """

    def __init__(self, oracle):
        self.oracle = oracle

    def insertOne(self, sql, param=None):
        with self.oracle as cursor:
            return cursor.execute(sql, param)

    def insertMany(self, sql, param=None):
        with self.oracle as cursor:
            return cursor.executemany(sql, param)

    def selectOne(self, sql, param=None):
        with self.oracle as cursor:
            cursor.execute(sql, param)
            result = cursor.fetchone()
        return result

    def selectAll(self, sql, param=None):
        with self.oracle as cursor:
            cursor.execute(sql, param)
            result = cursor.fetchall()
        return result

    def update(self, sql, param=None):
        with self.oracle as cursor:
            count = cursor.execute(sql, param)
            return count

    def delete(self, sql, param=None):
        with self.oracle as cursor:
            count = cursor.execute(sql, param)
            return count


