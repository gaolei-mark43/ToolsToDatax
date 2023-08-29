from plugin.Operator import *

def main():
    param_dict = parse_options()
    Operator(param_dict).operater()


def parse_options():
    param_dict = common.read_json('ptm_ddzx.json')
    if param_dict['source']['type'] == '':
        print('请输入源数据库类型')
        exit()
    elif param_dict['target']['type'] == '':
        print('请输入目标数据库类型')
        exit()
    print('读取启动配置成功')
    return param_dict


if __name__ == '__main__':
    main()
