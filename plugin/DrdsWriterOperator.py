from plugin import common


class DrdsWriterOperator:
    """
        生成DRDSWriter配置类
    """
    def __init__(self, config):
        self.url = config['url']
        self.username = config['username']
        self.password = config['password']
        self.table = config['table']
        self.writeMode = config['writeMode']
        # self.preSql = config['preSql']
        # self.postSql = config['postSql']
        self.column = config['column']

    def parse_writer(self):
        print('正在生成DRDSWriter配置')
        read_dict = common.read_json('config/drds_writer_template.json')
        read_dict['parameter']['username'] = self.username
        read_dict['parameter']['password'] = self.password
        read_dict['parameter']['column'] = self.column
        # read_dict['parameter']['preSql'] = self.postSql
        read_dict['parameter']['writeMode'] = self.writeMode
        # read_dict['parameter']['postSql'] = self.postSql
        read_dict['parameter']['connection'][0]['table'].append(self.table)
        read_dict['parameter']['connection'][0]['jdbcUrl']=self.url
        return read_dict