from plugin import common


class HbaseWriterOperator:
    """
        生成HbaseWriter配置类
    """

    def __init__(self, config):
        self.hbaseConfig = config['hbaseConfig']
        self.table = config['table']
        self.table_name = config['table_name']
        self.mode = config['mode']
        self.column = config['column']
        self.encoding = config['encoding']
        self.rowkeyColumn = config['rowkeyColumn']

    def parse_writer(self):
        print('正在生成HbaseWriter配置')
        read_dict = common.read_json('config/hbase11x_writer_template.json')
        hbase_data, rowkey_data = self.deal_column()
        read_dict['parameter']['column'] = hbase_data
        read_dict['parameter']['rowkeyColumn'] = rowkey_data
        read_dict['parameter']['hbaseConfig'] = self.hbaseConfig
        read_dict['parameter']['table'] = self.table_name
        read_dict['parameter']['mode'] = self.mode
        read_dict['parameter']['encoding'] = self.encoding
        return read_dict

    def deal_column(self):
        hbase_data = []
        rowkey_data = []
        index = 0
        for drds_column in self.column:
            hbase_col_dict = {}
            hbase_col_dict["index"] = index
            hbase_col_dict["name"] = self.table + ":" + drds_column
            hbase_col_dict["type"] = "string"
            index = index + 1
            if 'REVERSE' in drds_column:
                continue
            hbase_data.append(hbase_col_dict)
        for rowkey in self.rowkeyColumn:
            if rowkey in self.column:
                rowkey_col_dict = {}
                rowkey_col_dict["index"] = self.column.index(rowkey)
                rowkey_col_dict["type"] = "string"
                rowkey_data.append(rowkey_col_dict)

        return hbase_data, rowkey_data
