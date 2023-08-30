import os
from configparser import ConfigParser

def config(filename, section):
    parser = ConfigParser()
    parser.read(f'C:\Python\mjdw\{filename}')
    dbDict = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            dbDict[param[0]] = os.getenv(param[1])
    else:
        raise Exception(f'Section {section} is not found in the {filename} file.')
    return dbDict

### DEBUG ONLY ###
if __name__ == '__main__':
    pass