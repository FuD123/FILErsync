import os
import time
import configparser
import csv
import logging
import smtplib
import zipfile
import hashlib
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from smbclient import register_session, open_file, scandir, mkdir, delete_session
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import psutil
import schedule
from email.mime.text import MIMEText
from email.utils import formatdate

# 全局变量
config = configparser.ConfigParser()
hosts = []
transfer_stats = {'success': 0, 'failed': 0, 'retries': 0}
logging_handlers = {}

class ConfigHandler(FileSystemEventHandler):
    def on_modified(self, event):
        if event.src_path.endswith('config.ini'):
            load_config()
            logging.info(f"Config reloaded (v{config['General'].getfloat('version', 1.0)})")

def setup_logger():
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    
    success_log = setup_file_logger('success', 'logs/success.log', logging.INFO, log_formatter)
    error_log = setup_file_logger('error', 'logs/error.log', logging.ERROR, log_formatter)
    hash_log = setup_file_logger('hash', 'logs/hash_verify.log', logging.INFO, log_formatter)
    
    return [success_log, error_log, hash_log]

def setup_file_logger(name, log_file, level, formatter):
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    handler.setLevel(level)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logging_handlers[name] = handler
    return logger

def load_config():
    config.read('config.ini')
    if not os.path.exists(config['General']['local_base']):
        os.makedirs(config['General']['local_base'])

def get_hosts():
    global hosts
    try:
        with open('hosts.csv', 'r') as f:
            hosts = list(csv.DictReader(f))
    except Exception as e:
        logging.getLogger('error').error(f"读取主机列表失败: {str(e)}")

def connect_smb(host):
    try:
        register_session(host['ip'], 
                        username=host['username'],
                        password=host['password'],
                        encrypt=True)
        return True
    except Exception as e:
        logging.getLogger('error').error(f"{host['ip']} 连接失败: {str(e)}")
        return False

def transfer_file(host, file_entry):
    retry = 0
    max_retries = config.getint('General', 'retries')
    file_path = file_entry.path
    file_size = file_entry.stat().st_size
    
    # 创建本地目录结构
    local_dir = os.path.join(config['General']['local_base'], 
                            datetime.now().strftime('%Y-%m-%d'),
                            host['ip'].replace('.', '_'))
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, os.path.basename(file_path))
    
    while retry <= max_retries:
        try:
            with open_file(file_path, mode='rb') as remote_file:
                with open(local_path, 'wb') as local_file:
                    while chunk := remote_file.read(config.getint('Advanced', 'chunk_size')):
                        local_file.write(chunk)
            
            # 哈希校验
            if verify_hash(local_path):
                logging.getLogger('success').info(f"{host['ip']} {os.path.basename(file_path)} 传输成功")
                transfer_stats['success'] += 1
                return True
            else:
                os.remove(local_path)
                raise Exception("哈希校验失败")
                
        except Exception as e:
            retry += 1
            transfer_stats['retries'] += 1
            if retry > max_retries:
                logging.getLogger('error').error(f"{host['ip']} {file_path} 传输失败: {str(e)}")
                transfer_stats['failed'] += 1
                return False
            time.sleep(2 ** retry)
    
    return False

def verify_hash(local_path):
    try:
        hasher = hashlib.new(config['Advanced']['hash_algorithm'])
        with open(local_path, 'rb') as f:
            while chunk := f.read(config.getint('Advanced', 'chunk_size')):
                hasher.update(chunk)
        logging.getLogger('hash').info(f"{local_path}|{hasher.hexdigest()}")
        return True
    except Exception as e:
        logging.getLogger('error').error(f"{local_path} 哈希校验错误: {str(e)}")
        return False

def cleanup_old_logs():
    keep_days = config.getint('General', 'log_keep_days')
    cutoff = datetime.now() - timedelta(days=keep_days)
    
    for root, dirs, files in os.walk('logs'):
        for file in files:
            file_path = os.path.join(root, file)
            if datetime.fromtimestamp(os.path.getmtime(file_path)) < cutoff:
                os.remove(file_path)
                logging.info(f"已清理旧日志: {file_path}")

def compress_logs():
    today = datetime.now().strftime('%Y-%m-%d')
    with zipfile.ZipFile(f'logs/archive_{today}.zip', 'w') as zipf:
        for root, _, files in os.walk('logs'):
            for file in files:
                if file.endswith('.log') and not file.startswith('archive'):
                    zipf.write(os.path.join(root, file))
                    os.remove(os.path.join(root, file))

def send_email():
    msg = MIMEText(
        f"""传输统计报告 ({datetime.now().strftime('%Y-%m-%d')})
        
        成功文件: {transfer_stats['success']}
        失败文件: {transfer_stats['failed']}
        重试次数: {transfer_stats['retries']}
        
        完整日志请查看附件日志文件""")
    
    msg['Subject'] = f'文件传输报告 {datetime.now().strftime("%Y-%m-%d")}'
    msg['From'] = config['Email']['sender']
    msg['To'] = config['Email']['receivers']
    msg['Date'] = formatdate()
    
    try:
        with smtplib.SMTP(config['Email']['smtp_server'], config.getint('Email', 'smtp_port')) as server:
            server.starttls()
            server.login(config['Email']['sender'], config['Email']['password'])
            server.sendmail(config['Email']['sender'], 
                           config['Email']['receivers'].split(','), 
                           msg.as_string())
        logging.info("邮件发送成功")
    except Exception as e:
        logging.getLogger('error').error(f"邮件发送失败: {str(e)}")

def monitor_resources():
    mem = psutil.virtual_memory()
    if mem.percent > 80:
        ThreadPoolExecutor._max_workers = max(5, ThreadPoolExecutor._max_workers - 2)
    elif mem.percent < 40:
        ThreadPoolExecutor._max_workers = min(config.getint('General', 'threads'), 
                                             ThreadPoolExecutor._max_workers + 2)

def main_job():
    global transfer_stats
    transfer_stats = {'success': 0, 'failed': 0, 'retries': 0}
    
    with ThreadPoolExecutor(max_workers=config.getint('General', 'threads')) as executor:
        futures = []
        for host in hosts:
            if connect_smb(host):
                try:
                    for entry in scandir(f"\\\\{host['ip']}\\{host['share_path']}"):
                        if entry.is_file():
                            futures.append(executor.submit(transfer_file, host, entry))
                except Exception as e:
                    logging.getLogger('error').error(f"{host['ip']} 扫描目录失败: {str(e)}")
                finally:
                    delete_session(host['ip'])
        
        for future in as_completed(futures):
            pass  # 结果已经在transfer_file中处理

    cleanup_old_logs()
    if datetime.now().hour == 0:
        compress_logs()

def run_scheduler():
    schedule.every(config.getint('Network', 'scan_interval')).seconds.do(main_job)
    schedule.every().day.at(config['Email']['daily_report_time']).do(send_email)
    schedule.every(5).minutes.do(monitor_resources)
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    # 初始化
    os.makedirs('logs', exist_ok=True)
    logging.basicConfig(level=logging.INFO)
    setup_logger()
    load_config()
    get_hosts()
    
    # 配置热加载
    observer = Observer()
    observer.schedule(ConfigHandler(), path='.', recursive=False)
    observer.start()
    
    try:
        main_job()  # 首次立即运行
        run_scheduler()
    except KeyboardInterrupt:
        observer.stop()
    observer.join()
