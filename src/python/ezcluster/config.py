import os
from ConfigParser import ConfigParser

class ConfigException(Exception):
    pass

def get_root_dir():
    fpath = os.path.abspath(__file__)
    (cwdir, fname) = os.path.split(fpath)
    rdir = os.path.abspath(os.path.join(cwdir, '..', '..', '..'))
    return rdir

ROOT_DIR = get_root_dir()
SRC_DIR = os.path.join(ROOT_DIR, 'src')
SH_DIR = os.path.join(SRC_DIR, 'sh')
CONF_FILE = os.path.join(ROOT_DIR, 'config.properties')
if not os.path.exists(CONF_FILE):
    raise ConfigException('Cannot find config file %s! Make one.')
    
config = ConfigParser()
config.read(CONF_FILE)
