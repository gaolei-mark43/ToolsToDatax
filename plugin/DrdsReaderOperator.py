from plugin import common


class DrdsReaderOperator:
    """
        生成DRDSReader配置类
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

    def parse_reader(self):
        print('正在生成DRDSReader配置')
        read_dict = common.read_json('config/drds_reader_template.json')
        read_dict['parameter']['username'] = self.username
        read_dict['parameter']['password'] = self.password
        read_dict['parameter']['where'] = self.where
        read_dict['parameter']['column'] = self.column
        read_dict['parameter']['connection'][0]['table'].append(self.table)
        read_dict['parameter']['connection'][0]['jdbcUrl'].append(self.url)
        return read_dict
