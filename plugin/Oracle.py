import jaydebeapi

"""

python操作oracle,如果使用cx_Oracle则需要在运行环境中存在oracle驱动
这里直接调用java的jdbc驱动

"""


class Oracle:
    """
        oracle工具类
    """

    def __init__(self, config):
        self.conn = jaydebeapi.connect('oracle.jdbc.driver.OracleDriver',
                                       [config['url'], config['user'], config['password']],
                                       ['lib/ojdbc6-11.2.0.3.jar'])

    def select(self, sql):
        data_rows = []
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql)
            data_rows = cursor.fetchall()
            cursor.close()
            return data_rows
        except Exception as e:
            print('Excute sql error:{}'.format(e))
            return data_rows
