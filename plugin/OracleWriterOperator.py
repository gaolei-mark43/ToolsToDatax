from plugin import common


class OracleWriterOperator:
    """
        生成OracleWriter配置类
    """

    def __init__(self, config):
        self.url = config['url']
        self.username = config['username']
        self.password = config['password']
        self.table = config['table']
        self.preSql = config['preSql']
        self.postSql = config['postSql']
        self.column = config['column']
        self.session = config['session']

    def parse_writer(self):
        print('正在生成OracleWriter配置')
        read_dict = common.read_json('config/oracle_writer_template.json')
        read_dict['parameter']['username'] = self.username
        read_dict['parameter']['password'] = self.password
        read_dict['parameter']['column'] = self.column
        read_dict['parameter']['preSql'] = self.postSql
        read_dict['parameter']['postSql'] = self.postSql
        read_dict['parameter']['session'] = self.session
        read_dict['parameter']['connection'][0]['table'].append(self.table)
        read_dict['parameter']['connection'][0]['jdbcUrl'] = self.url
        return read_dict
