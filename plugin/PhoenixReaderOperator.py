from plugin import common


class PhoenixReaderOperator:
    """
        生成PhoenixReader配置类
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
        print('正在生成PhoenixReader配置')
        read_dict = common.read_json('config/phoenix_reader_template.json')
        read_dict['parameter']['username'] = self.username
        read_dict['parameter']['password'] = self.password
        read_dict['parameter']['where'] = self.where
        read_dict['parameter']['column'] = self.column
        read_dict['parameter']['splitPk'] = self.splitPk
        read_dict['parameter']['connection'][0]['table'].append(self.table)
        read_dict['parameter']['connection'][0]['jdbcUrl'].append(self.url)
        return read_dict
