import jaydebeapi

"""

python操作phoenix,phoenixdb不会使用
这里直接调用java的jdbc驱动

"""


class Phoenix:
    """
        phoenix工具类
    """

    def __init__(self, config):
        self.conn = jaydebeapi.connect('org.apache.phoenix.jdbc.PhoenixDriver',
                                       [config['url'], config['user'], config['password']],
                                       ['../lib/phoenix-4.10.0-HBase-1.2-client.jar'])

    def select(self, sql):
        data_rows = []
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            data_rows = cursor.fetchall()
            return data_rows
        except Exception as e:
            print('Excute sql error:{}'.format(e))
            return data_rows
