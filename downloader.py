import os
import time
import requests
import argparse
import configparser
import hashlib
import signal
import threading
from typing import Optional, List, Tuple
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse, parse_qs

# 模拟浏览器请求头
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive"
}

# 全局变量与线程锁
stop_signal = False
stop_lock = threading.Lock()
progress_lock = threading.Lock()

def signal_handler(signum, frame):
    """处理终止信号"""
    global stop_signal
    with stop_lock:
        stop_signal = True
    print("\n接收到终止信号，正在清理并退出...")

def load_config() -> dict:
    """加载配置文件，增加存在性检查"""
    config = configparser.ConfigParser()
    config_path = 'downloader.ini'
    
    if not os.path.exists(config_path):
        print(f"警告：未找到配置文件 {config_path}，使用默认配置")
    else:
        config.read(config_path)
    
    defaults = {
        'threads': 8,
        'temp_dir': '.',
        'retries': 3,
        'timeout': 10,
        'speed_limit': 0,
        'verify_ssl': True
    }
    
    if 'downloader' in config:
        for key in defaults:
            if key in config['downloader']:
                if key == 'verify_ssl':
                    defaults[key] = config.getboolean('downloader', key, fallback=True)
                else:
                    try:
                        defaults[key] = type(defaults[key])(config['downloader'][key])
                    except ValueError:
                        print(f"配置文件中 {key} 的值无效，使用默认值")
    return defaults

def check_url_expiry(expires_timestamp: Optional[int]) -> bool:
    """检查URL是否过期，增加None检查"""
    if expires_timestamp is None:
        return True  # 无法检测过期时间，默认不过期
        
    current_time = time.time()
    if current_time > expires_timestamp:
        print(f"⚠️ 下载链接已过期（有效期至：{time.ctime(expires_timestamp)}）")
        return False
    return True

def is_presigned_url(url: str) -> bool:
    """判断是否为预签名URL，扩展判断条件"""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    # 增加常见签名参数
    signature_keys = {'Signature', 'X-Amz-Signature', 'signed'}
    expires_keys = {'Expires', 'X-Amz-Expires'}
    
    has_signature = any(key in query_params for key in signature_keys)
    has_expires = any(key in query_params for key in expires_keys)
    
    return has_signature and has_expires

def parse_expires_time(url: str) -> Optional[int]:
    """解析URL中的过期时间戳，安全获取参数"""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    expires_list = query_params.get("Expires")
    if not expires_list or len(expires_list) == 0:
        return None
        
    try:
        return int(expires_list[0])
    except ValueError:
        print("无法解析Expires参数为整数")
        return None

def download_range(session: requests.Session, url: str, start: int, end: int, 
                  file_path: str, part_id: int, progress_bar: tqdm, config: dict) -> bool:
    """带重试和限速的分块下载，支持中断检查"""
    global stop_signal
    
    headers = {'Range': f'bytes={start}-{end}'} if start < end else {}
    retries_left = config['retries']
    
    file_path_obj = Path(file_path)
    part_path = file_path_obj.with_suffix(f".part{part_id}{file_path_obj.suffix}")
    
    # 检查是否有可恢复的部分
    resume_start = start
    if os.path.exists(part_path):
        part_size_exist = os.path.getsize(part_path)
        expected_part_size = end - start + 1
        
        if part_size_exist > expected_part_size:
            print(f"分块文件 {part_path} 大小异常，重新下载")
            os.remove(part_path)
        elif part_size_exist == expected_part_size:
            with progress_lock:
                progress_bar.update(expected_part_size)
            return True
        else:
            resume_start = start + part_size_exist
            headers['Range'] = f'bytes={resume_start}-{end}'
            with progress_lock:
                progress_bar.update(part_size_exist)
            print(f"恢复第{part_id}部分下载，从 {resume_start} 字节开始")

    while retries_left > 0:
        if check_stop():
            print(f"下载第{part_id}部分已中断")
            return False
            
        try:
            # 设置超时，以便能及时响应中断信号
            response = session.get(
                url,
                headers=headers,
                stream=True,
                timeout=config['timeout'],
                verify=config['verify_ssl']
            )
            response.raise_for_status()
            
            mode = 'ab' if resume_start > start else 'wb'
            with open(part_path, mode) as f:
                start_time = time.time()
                bytes_downloaded = 0
                
                for chunk in response.iter_content(chunk_size=1024*1024):
                    if check_stop():
                        print(f"下载第{part_id}部分已中断")
                        return False
                        
                    if not chunk:
                        continue
                        
                    f.write(chunk)
                    chunk_len = len(chunk)
                    
                    with progress_lock:
                        progress_bar.update(chunk_len)
                        
                    bytes_downloaded += chunk_len
                    
                    # 优化限速逻辑，累积到1MB再计算
                    if config['speed_limit'] > 0 and bytes_downloaded % (1024*1024) < chunk_len:
                        speed_limit_bps = config['speed_limit'] * 1024 * 1024
                        elapsed = time.time() - start_time
                        expected_time = bytes_downloaded / speed_limit_bps
                        if elapsed < expected_time:
                            time.sleep(expected_time - elapsed)
            
            return True
            
        except requests.exceptions.Timeout:
            retries_left -= 1
            print(f"第{part_id}部分下载超时（剩余重试次数：{retries_left}）")
        except requests.exceptions.HTTPError as e:
            retries_left -= 1
            print(f"第{part_id}部分HTTP错误（剩余重试次数：{retries_left}）：{str(e)}")
        except Exception as e:
            retries_left -= 1
            print(f"第{part_id}部分下载失败（剩余重试次数：{retries_left}）：{str(e)}")
            
        if retries_left == 0:
            print(f"第{part_id}部分下载失败，已达到最大重试次数")
            return False
            
        time.sleep(2 ** (config['retries'] - retries_left))  # 指数退避重试
        
    return False

def merge_parts(file_path: str, total_parts: int) -> bool:
    """合并分块文件，支持中断检查"""
    global stop_signal
    
    if total_parts <= 0:
        print("没有分块需要合并")
        return False
        
    try:
        with open(file_path, 'wb') as outfile:
            for i in range(total_parts):
                if check_stop():
                    print("文件合并已中断")
                    return False
                    
                file_path_obj = Path(file_path)
                part_path = file_path_obj.with_suffix(f".part{i}{file_path_obj.suffix}")
                
                if not os.path.exists(part_path):
                    raise FileNotFoundError(f"分块文件丢失：{part_path}")
                    
                with open(part_path, 'rb') as infile:
                    outfile.write(infile.read())
                    
                # 合并后立即删除分块文件，减少磁盘占用
                for _ in range(3):  # 重试3次
                    try:
                        os.remove(part_path)
                        break
                    except PermissionError:
                        time.sleep(1)
                    except Exception as e:
                        print(f"无法删除临时文件 {part_path}：{str(e)}")
                        break
                        
        return True
        
    except Exception as e:
        print(f"文件合并失败：{str(e)}")
        return False

def cleanup_temp_files(file_path: str, total_parts: int) -> None:
    """清理临时分块文件，增加重试机制"""
    print("正在清理临时文件...")
    
    for i in range(total_parts):
        file_path_obj = Path(file_path)
        part_path = file_path_obj.with_suffix(f".part{i}{file_path_obj.suffix}")
        
        if os.path.exists(part_path):
            for _ in range(3):  # 重试3次
                try:
                    os.remove(part_path)
                    print(f"已删除临时文件：{part_path}")
                    break
                except PermissionError:
                    time.sleep(1)  # 等待1秒再试
                except Exception as e:
                    print(f"无法删除临时文件 {part_path}：{str(e)}")
                    break

def verify_file(file_path: str, expected_hash: Optional[str] = None, algorithm: str = 'md5') -> bool:
    """验证文件哈希值，增加算法合法性检查"""
    if not expected_hash:
        return True
        
    # 检查算法是否支持
    if algorithm not in hashlib.algorithms_available:
        print(f"错误：不支持的哈希算法 {algorithm}")
        return False
        
    try:
        hash_obj = hashlib.new(algorithm)
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_obj.update(chunk)
                
        computed_hash = hash_obj.hexdigest()
        
        if computed_hash.lower() == expected_hash.lower():
            print(f"文件哈希验证成功 ({algorithm}: {computed_hash})")
            return True
        else:
            print(f"文件哈希验证失败，期望: {expected_hash}，实际: {computed_hash}")
            return False
            
    except Exception as e:
        print(f"哈希计算出错: {str(e)}")
        return False

def download_single_thread(url: str, output_path: Optional[str] = None, num_threads: Optional[int] = None, 
                 resume: bool = False, expected_hash: Optional[str] = None, 
                 hash_algorithm: str = 'md5') -> None:
    """单线程下载模式"""
    global stop_signal
    
    try:
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)
        
        print(f"使用单线程下载")
        progress = tqdm(unit='B', unit_scale=True, desc=output_path,
                       bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}, {remaining}]")
        
        # 检查是否可以续传
        resume_pos = 0
        if os.path.exists(output_path):
            resume_pos = os.path.getsize(output_path)
            print(f"发现已存在文件，大小为 {resume_pos/1024/1024:.2f} MB")
            
            overwrite = input("是否覆盖？(1=覆盖, 2=续传, 其他=取消): ").strip()
            if overwrite == '1':
                os.remove(output_path)
                resume_pos = 0
            elif overwrite != '2':
                print("已取消下载")
                progress.close()
                return
                
            progress.update(resume_pos)
        
        headers = {'Range': f'bytes={resume_pos}-'} if resume_pos > 0 else {}
        mode = 'ab' if resume_pos > 0 else 'wb'
        
        response = session.get(url, headers=headers, stream=True, 
                              verify=config['verify_ssl'], timeout=config['timeout'])
        response.raise_for_status()
        
        with open(output_path, mode) as f:
            start_time = time.time()
            bytes_downloaded = resume_pos
            
            for chunk in response.iter_content(chunk_size=1024*1024):
                if check_stop():
                    print("下载已中断")
                    progress.close()
                    return
                    
                if chunk:
                    f.write(chunk)
                    chunk_len = len(chunk)
                    progress.update(chunk_len)
                    bytes_downloaded += chunk_len
                    
                    # 限速
                    if config['speed_limit'] > 0:
                        speed_limit_bps = config['speed_limit'] * 1024 * 1024
                        elapsed = time.time() - start_time
                        expected_time = bytes_downloaded / speed_limit_bps
                        if elapsed < expected_time:
                            time.sleep(expected_time - elapsed)
        
        progress.close()
        print(f"下载完成: {output_path}")
        
    except requests.exceptions.MissingSchema:
        print("错误: 无效的URL格式，请包含协议（如https://）")
    except requests.exceptions.SSLError:
        print("SSL证书验证失败，可在配置文件中设置verify_ssl=false尝试绕过（不推荐）")
    except requests.exceptions.ConnectionError:
        print("错误: 无法连接到服务器，请检查网络和URL")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP错误: {str(e)}")
    except Exception as e:
        print(f"下载过程出错: {str(e)}")
    finally:
        session.close()

def download_file(url: str, output_path: Optional[str] = None, num_threads: Optional[int] = None, 
                 resume: bool = False, expected_hash: Optional[str] = None, 
                 hash_algorithm: str = 'md5') -> None:
    """主下载函数，支持中断处理"""
    global stop_signal
    
    config = load_config()
    
    # 处理输出路径
    if not output_path:
        # 从URL中提取文件名，若失败则使用时间戳
        filename = url.split("/")[-1].split("?")[0] or f"download_{int(time.time())}"
        output_path = filename
    
    # 确保目录存在
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    
    # 检查文件是否已存在
    if os.path.exists(output_path):
        if resume:
            has_resumable = any(
                os.path.exists(Path(output_path).with_suffix(f".part{i}{Path(output_path).suffix}")) 
                for i in range(config['threads'])
            )
            if not has_resumable:
                print("未找到可续传的进度，将从头开始下载")
        else:
            overwrite = input(f"文件 {output_path} 已存在，是否覆盖？(y/N) ").strip().lower()
            if overwrite != 'y':
                print("已取消下载")
                return
    
    # 检查预签名URL
    if is_presigned_url(url):
        expires = parse_expires_time(url)
        if not check_url_expiry(expires):
            return
    
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    
    try:
        # 获取文件信息
        response = session.head(url, allow_redirects=True, verify=config['verify_ssl'])
        response.raise_for_status()
        
        # 检查最终URL是否与原URL不同
        if response.url != url:
            print(f"检测到重定向至 {response.url}，重新验证文件信息")
            response = session.head(response.url, allow_redirects=True, verify=config['verify_ssl'])
            response.raise_for_status()
        
        # 获取文件大小
        file_size = int(response.headers.get('Content-Length', 0))
        
        # 检查是否支持断点续传
        accept_ranges = response.headers.get('Accept-Ranges', '') == 'bytes'
        
        # 设置线程数
        if num_threads is None:
            num_threads = config['threads']
        
        # 验证线程数
        if num_threads <= 0:
            print("线程数必须为正整数，已自动调整为8")
            num_threads = 8
        
        # 处理文件大小为0的情况
        if file_size <= 0:
            print("无法获取文件大小或文件为空，使用单线程下载")
            download_single_thread(url, output_path, config)
            return
        
        print(f"文件大小: {file_size/1024/1024:.2f} MB")
        
        # 不支持断点续传时使用单线程
        if not accept_ranges:
            print("服务器不支持断点续传，使用单线程下载")
            download_single_thread(url, output_path, config)
            return
        
        print(f"使用多线程下载 (线程数: {num_threads})")
        
        part_size = file_size // num_threads
        progress = tqdm(total=file_size, unit='B', unit_scale=True, desc=output_path,
                       bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}, {remaining}]")

        # 准备分块下载任务
        futures = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            for i in range(num_threads):
                if check_stop():
                    print("下载已中断")
                    progress.close()
                    cleanup_temp_files(output_path, num_threads)
                    return
                    
                start = i * part_size
                end = start + part_size - 1 if i < num_threads - 1 else file_size - 1
                
                future = executor.submit(
                    download_range,
                    session, response.url, start, end, output_path, i, progress, config
                )
                futures.append((i, future))

        # 检查所有任务是否成功
        all_success = True
        for i, future in futures:
            if check_stop():
                print("下载已中断")
                progress.close()
                cleanup_temp_files(output_path, num_threads)
                return
                
            if not future.result():
                all_success = False
                print(f"第{i}部分下载失败")

        progress.close()

        if check_stop():
            print("下载已中断")
            cleanup_temp_files(output_path, num_threads)
            return

        if all_success:
            print("所有分块下载完成，开始合并文件...")
            if merge_parts(output_path, num_threads):
                print(f"文件合并完成: {output_path}")
                
                # 验证文件
                if expected_hash:
                    if verify_file(output_path, expected_hash, hash_algorithm):
                        print("文件验证成功")
                    else:
                        print("文件验证失败，可能已损坏")
            else:
                print("文件合并失败，请检查分块文件")
        else:
            print("部分分块下载失败，请重试")

    except requests.exceptions.MissingSchema:
        print("错误: 无效的URL格式，请包含协议（如https://）")
    except requests.exceptions.ConnectionError:
        print("错误: 无法连接到服务器，请检查网络和URL")
    except requests.exceptions.HTTPError as e:
        print(f"HTTP错误: {str(e)}")
    except requests.exceptions.SSLError:
        print("SSL证书验证失败，可在配置文件中设置verify_ssl=false尝试绕过（不推荐）")
    except Exception as e:
        print(f"下载过程出错: {str(e)}")
    finally:
        # 确保session被关闭
        session.close()

def check_stop() -> bool:
    """安全检查停止信号"""
    with stop_lock:
        return stop_signal

if __name__ == "__main__":
    # 设置信号处理（支持Windows）
    if os.name == 'nt':  # Windows系统
        import win32api
        # 修复：回调函数需要返回bool类型
        def handle_win_interrupt():
            # 定义符合类型要求的回调函数（返回True表示已处理）
            def ctrl_handler(ctrl_type: int) -> bool:
                signal_handler(None, None)  # 调用原信号处理函数
                return True  # 必须返回bool类型
            win32api.SetConsoleCtrlHandler(ctrl_handler, True)
        handle_win_interrupt()
    else:  # Unix/Linux/macOS
        signal.signal(signal.SIGINT, signal_handler)
    
    # 命令行参数解析
    parser = argparse.ArgumentParser(description='增强版多线程下载器，支持预签名URL处理')
    
    # 将url改为可选参数
    parser.add_argument('--url', help='直接指定下载URL，无需交互式输入')
    parser.add_argument('-o', '--output', help='保存文件名')
    parser.add_argument('-t', '--threads', type=int, help=f'线程数（默认：配置文件中的{load_config()["threads"]}）')
    parser.add_argument('--resume', action='store_true', help='尝试恢复未完成的下载')
    parser.add_argument('--hash', help='预期的文件哈希值，用于验证完整性')
    parser.add_argument('--algorithm', default='md5', help='哈希算法（默认：md5，支持sha1, sha256等）')
    
    args = parser.parse_args()
    
    # 获取URL：优先使用命令行参数，否则交互式输入
    url = args.url
    if not url:
        url = input("请输入下载URL: ").strip()
        if not url:
            print("错误: URL不能为空")
            exit(1)
    
    # 其他参数处理
    output = args.output
    threads = args.threads
    
    # 交互式获取缺少的参数
    if not output:
        output = input("请输入保存文件名(可选，直接回车使用默认): ").strip() or None
    
    if threads is None:
        threads_input = input("请输入线程数(默认8): ").strip()
        threads = int(threads_input) if threads_input else 8
    
    # 验证线程数
    if threads <= 0:
        print("线程数必须为正整数，已自动调整为8")
        threads = 8
    
    # 调用下载函数
    download_file(
        url=url,
        output_path=output,
        num_threads=threads,
        resume=args.resume,
        expected_hash=args.hash,
        hash_algorithm=args.algorithm
    )