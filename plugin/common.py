import json

def read_json(file):
    with open(file, 'r') as json_f:
        json_dict = json.load(json_f)
    return json_dict


def write_json(filename, dict):
    with open('target/' + filename, 'w') as f:
        json.dump(dict, f, indent=4, )
        print('脚本已生成，路径：target/' + filename)