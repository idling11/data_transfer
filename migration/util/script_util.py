import logging
import os

logger = logging.getLogger(__name__)


def get_meta_cmd_path():
    if os.name == 'nt':
        meta_cmd_path = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                                     'scripts/linux/meta_cmd.exe')
        logger.info(f'meta_cmd_path: {meta_cmd_path}')
        return meta_cmd_path
    elif os.name == 'posix':
        meta_cmd_path = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                                     'scripts/linux/meta_cmd')
        logger.info(f'meta_cmd_path: {meta_cmd_path}')
        return meta_cmd_path
    elif os.name == 'mac':
        meta_cmd_path = os.path.join(os.path.abspath(os.path.dirname(os.path.dirname(__file__))),
                                     'scripts/linux/meta_cmd')
        logger.info(f'meta_cmd_path: {meta_cmd_path}')
        return meta_cmd_path
    else:
        raise Exception(f'Unsupported OS:{os.name}')


def get_files_path(instance_id, workspace, schema, table, meta_conf_path):
    get_files_cmd = f'{get_meta_cmd_path()} list-data-files -i {instance_id} -n {workspace},{schema} -t {table} -c {meta_conf_path}'
    logger.info(f'get_files_cmd: {get_files_cmd}')
    try:
        result = os.popen(get_files_cmd).readlines()
    except Exception as e:
        logger.error(f'get_files_cmd error: {e}')
        raise Exception(f'get_files_cmd error: {e}')
    files = set()
    for line in result:
        if line.startswith('file_path'):
            last_item_length = len(line.replace('\"', '').split('/')[-1])
            file_path = line.replace('\"', '')[:-last_item_length].split('/', 3)[-1]
            logger.info(f'Add Object storage file_path: {file_path}')
            files.add(file_path)
    return files
