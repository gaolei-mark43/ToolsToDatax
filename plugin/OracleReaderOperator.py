from plugin import common


"""
需要程序运行环境有ORCLE，不做了，任性
==================================
算了，字段不从数据库读了，启动脚本里面写吧
==================================
算了算了，调用jdbc吧
"""


class OracleReaderOperator:
    """
        生成OracleReader配置类
    """

    def __init__(self, config):
        self.url = config['url']
        self.username = config['username']
        self.password = config['password']
        self.table = config['table']
        self.ip = config['ip']
        self.dbname = config['dbname']
        self.where = config['where']
        self.column = config['column']
        self.splitPk = config['splitPk']

    def parse_reader(self):
        print('正在生成OracleReader配置')

        read_dict = common.read_json('config/oracle_reader_template.json')
        read_dict['parameter']['username'] = self.username
        read_dict['parameter']['password'] = self.password
        read_dict['parameter']['where'] = self.where
        read_dict['parameter']['column'] = self.column
        read_dict['parameter']['splitPk'] = self.splitPk
        read_dict['parameter']['connection'][0]['table'].append(self.table)
        read_dict['parameter']['connection'][0]['jdbcUrl'].append(self.url)
        return read_dict
