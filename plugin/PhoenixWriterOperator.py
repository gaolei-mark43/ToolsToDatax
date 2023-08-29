from plugin import common


class PhoenixWriterOperator:
    """
        生成PhoenixWriter配置类
    """

    def __init__(self, config):
        self.url = config['url']
        self.username = config['username']
        self.password = config['password']
        self.table = config['table']

        self.column = config['column']

        self.writeMode = config['writeMode']

    def parse_writer(self):
        print('正在生成PhoenixWriter配置')
        read_dict = common.read_json('config/phoenix_writer_template.json')
        read_dict['parameter']['username'] = self.username
        read_dict['parameter']['password'] = self.password
        read_dict['parameter']['column'] = self.column
        read_dict['parameter']['writeMode'] = self.writeMode
        read_dict['parameter']['connection'][0]['table'].append(self.table)
        read_dict['parameter']['connection'][0]['jdbcUrl'] = self.url
        return read_dict
