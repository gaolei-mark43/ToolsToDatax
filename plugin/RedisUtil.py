import redis

pipe_size = 10000


class RedisConfig:
    """
    redis参数处理
    """

    def __init__(self, config):
        self.host = config['ip'] if 'ip' in config else ''
        self.port = config['port'] if 'port' in config else ''
        self.password = config['password'] if 'password' in config else ''
        self.db = config['dbname'] if 'dbname' in config else ''


class RedisPool:
    def __init__(self, redisconfig):
        self.host = redisconfig.host
        self.password = redisconfig.password
        self.db = redisconfig.db
        self.port = redisconfig.port
        self.pool = None

    def creatPool(self):
        self.pool = redis.ConnectionPool(host=self.host,
                                         password=self.password,
                                         port=self.port, db=self.db, decode_responses=True)
        return self.pool


class Redis:
    """
        redis工具类
    """

    def __init__(self, pool):
        self.conn = redis.Redis(connection_pool=pool)

    def get_all_kv(self):
        """
        查询所有kv
        :return:
        """
        keys = self.conn.keys()
        kv_dict = self.get_batch(keys)
        return kv_dict

    def get_one(self, key):
        """
        查询一个kv
        :param key:
        :return:
        """
        r = self.conn
        value = r.get(key)
        return dict(zip([key], value))

    def insert_one(self, kv):
        """
        插入一个kv
        :param kv:
        :return:
        """
        r = self.conn
        boolen = r.set(kv[0], kv[1], nx=True, px=kv[2])  # True--不存在  None--已存在
        return boolen

    def update_one(self, kv):
        """
        更新一个kv
        :param kv:
        :return:
        """
        r = self.conn
        boolen = r.set(kv[0], kv[1], xx=True, px=kv[2])  # True--已存在  None--不存在
        return boolen

    def get_batch(self, keys):
        """
        批量查询
        :param keys:
        :return:
        """
        kv_dict = {}
        key_list = []
        pipe = self.conn.pipeline()
        len = 0
        for key in keys:
            key_list.append(key)
            pipe.get(key)
            if len < pipe_size:
                len += 1
            else:
                kv_dict.update(dict(zip(key_list, pipe.execute())))
        kv_dict.update(dict(zip(key_list, pipe.execute())))
        return kv_dict

