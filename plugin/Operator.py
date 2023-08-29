from plugin.dependence import *


class Operator:
    def __init__(self, config):
        self.config = config

    def operater(self):
        reader_pool = self.choose_pool(self.config['source'])
        source_dict = self.config['source']
        target_dict = self.config['target']

        source_contents = source_dict['content']
        tagert_contents = target_dict['content'] if 'content' in target_dict else ''

        for i in source_contents:
            table = i['table'].upper()
            print('正在生成%s表的datax脚本' % table)
            if tagert_contents != '':
                for tagert_content in tagert_contents:
                    if table in str(tagert_content['table']).upper().split(':'):
                     #   target_dict['rowkeyColumn'] = tagert_content['rowkeyColumn']
                        target_dict['table_name'] = tagert_content['table'].upper()
                        break
            source_dict['table'] = table
            target_dict['table'] = table
            source_dict['column'] = i['column']
            reader_dict = self.set_reader(source_dict, target_dict, reader_pool)
            list = reader_dict['parameter']['column'].copy()
            if target_dict['type'].lower() == 'phoenix':
                if 'rowkeyColumn' in target_dict:
                    list[-1] = "ROW_KEY"
            target_dict['column'] = list
            writer_dict = self.get_writer(target_dict)
            content_dict = {'reader': reader_dict, 'writer': writer_dict}
            template = common.read_json('config/datax_template.json')
            template['job']['content'].append(content_dict)
            filename = source_dict['type'].upper() + 'To' + target_dict['type'].upper() + '_' + \
                       source_dict['table'].upper() + '.json'
            print('正在写入datax脚本')
            common.write_json(filename, template)

    def set_reader(self, source_dict, target_dict, pool):
        reader_dict = {}
        if len(source_dict['column']) == 0:
            columns = self.get_columns(source_dict, target_dict, pool)
            source_dict['column'] = columns
        if source_dict['type'].lower() == 'drds':
            reader_dict = DrdsReaderOperator(source_dict).parse_reader()
        elif source_dict['type'].lower() == 'mysql':
            reader_dict = MysqlReaderOperator(source_dict).parse_reader()
        elif source_dict['type'].lower() == 'oracle':
            reader_dict = OracleReaderOperator(source_dict).parse_reader()
        elif source_dict['type'].lower() == 'phoenix':
            reader_dict = PhoenixReaderOperator(source_dict).parse_reader()
        return reader_dict

    def get_writer(self, target_dict):
        writer_dict = {}
        if target_dict['type'].lower() == 'drds':
            writer_dict = DrdsWriterOperator(target_dict).parse_writer()
        elif target_dict['type'].lower() == 'mysql':
            writer_dict = MysqlWriterOperator(target_dict).parse_writer()
        elif target_dict['type'].lower() == 'oracle':
            writer_dict = OracleWriterOperator(target_dict).parse_writer()
        elif target_dict['type'].lower() == 'phoenix':
            writer_dict = PhoenixWriterOperator(target_dict).parse_writer()
        elif target_dict['type'].lower() == 'hbase':
            writer_dict = HbaseWriterOperator(target_dict).parse_writer()
        return writer_dict

    @staticmethod
    def choose_pool(config):
        pool = None
        if config['type'].lower() == 'drds' or config['type'].lower() == 'mysql':
            pool = MysqlPool(MysqlConfig(config)).creatPool()
        elif config['type'].lower() == 'oracle':
            pool = OraclePool(OracleConfig(config)).creatPool()
        return pool

    @staticmethod
    def get_columns(source_config, target_config, pool):
        columns = []
        if source_config['type'].lower() == 'drds' or source_config['type'].lower() == 'mysql':
            mysqlutil = MysqlUtil(Mysql(pool))
            columns = []
            # table_rows = mysqlutil.selectAll(
            #     "select COLUMN_NAME from information_schema.COLUMNS where table_name = '%s' and table_schema = '%s';" % (
            #         source_config['table'], source_config['dbname']))
            table_rows = mysqlutil.selectAll(
                "show columns from %s from %s" % (
                    source_config['table'], source_config['dbname']))
            for row in table_rows:
                columns.append(row[0].decode().upper())

        elif source_config['type'].lower() == 'oracle':
            oracleutil = OracleUtil(Oracle(pool))
            columns = []
            table_rows = oracleutil.selectAll(
                "select t.column_name from user_col_comments t where t.table_name = :table_name",
                {"table_name": source_config['table']})
            for row in table_rows:
                columns.append(row[0].upper())
        elif source_config['type'].lower() == 'phoenix':
            phoenix_configs = {'url': source_config['url'], 'user': source_config['username'],
                               'password': source_config['password']}
            table_rows = Phoenix(phoenix_configs).select(
                "SELECT COLUMN_NAME FROM SYSTEM.\"CATALOG\" where TABLE_NAME = '%s'" % source_config['table'])
            for row in table_rows:
                if row[0] is None:
                    continue
                columns.append(row[0].upper())
        # 添加反转主键
        if target_config['type'].lower() == 'hbase':
            rowkeyColumns = target_config['rowkeyColumn'] if 'rowkeyColumn' in target_config else ''
            for rowkeyColumn in rowkeyColumns:
                if 'REVERSE' in rowkeyColumn:
                    columns.append(rowkeyColumn)
        if target_config['type'].lower() == 'phoenix':
            if 'rowkeyColumn' in target_config:
                rowkeyColumns = target_config['rowkeyColumn']
                column = "CONCAT(" + ",".join(rowkeyColumns) + ")"
                columns.append(column)
        return columns
