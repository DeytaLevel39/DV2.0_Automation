import yaml

def read_yaml(filename):
    with open(filename, encoding='utf-8') as file:
        try:
            yamlfile = yaml.safe_load(file)
        except yaml.YAMLError as exc:
            print(exc)
    return yamlfile