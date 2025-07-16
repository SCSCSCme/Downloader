import os
import time
import requests
import argparse
import configparser
import hashlib
import signal
import threading
import sys
from typing import Optional
from pathlib import Path
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    config_path = '.\\downloader.ini'
    
    if not os.path.exists(config_path):
        print(f"警告：未找到配置文件 {config_path}，使用默认配置")
    else:
        try:
            config.read(config_path)
        except Exception as e:
            print(f"读取配置文件出错: {str(e)}，使用默认配置")
    
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
                    try:
                        defaults[key] = config.getboolean('downloader', key)
                    except ValueError:
                        print(f"配置文件中 {key} 的值无效，使用默认值")
                else:
                    try:
                        # 尝试转换为正确的类型
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
    signature_keys = {'Signature', 'X-Amz-Signature', 'signed', 'signature', 'sig', 'token'}
    expires_keys = {'Expires', 'X-Amz-Expires', 'expires', 'validity', 'validuntil'}
    
    has_signature = any(key in query_params for key in signature_keys)
    has_expires = any(key in query_params for key in expires_keys)
    
    return has_signature or has_expires

def parse_expires_time(url: str) -> Optional[int]:
    """解析URL中的过期时间戳，安全获取参数"""
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    # 检查所有可能的过期时间参数
    for key in ['Expires', 'X-Amz-Expires', 'expires', 'validity', 'validuntil']:
        expires_list = query_params.get(key)
        if expires_list and len(expires_list) > 0:
            try:
                return int(expires_list[0])
            except ValueError:
                # 尝试从ISO时间格式转换
                try:
                    import dateutil.parser
                    dt = dateutil.parser.parse(expires_list[0])
                    return int(dt.timestamp())
                except:
                    pass  # 继续尝试下一个可能的参数
                
    # 尝试从URL路径中解析
    try:
        path = parsed_url.path
        if 'expires=' in path:
            parts = path.split('expires=')
            if len(parts) > 1:
                timestamp = parts[1].split('&')[0]
                return int(timestamp)
    except:
        pass
    
    return None

def download_range(session: requests.Session, url: str, start: int, end: int, 
                  file_path: str, part_id: int, progress_bar: tqdm, config: dict) -> bool:
    """带重试和限速的分块下载，支持中断检查"""
    global stop_signal
    
    # 计算分块大小
    chunk_size = 1024 * 1024  # 1MB
    headers = {'Range': f'bytes={start}-{end}'} if start < end else {}
    retries_left = config['retries']
    
    file_path_obj = Path(file_path)
    part_path = file_path_obj.with_suffix(f".part{part_id}{file_path_obj.suffix}")
    
    # 检查是否有可恢复的部分
    resume_start = start
    if part_path.exists():
        part_size_exist = part_path.stat().st_size
        expected_part_size = end - start + 1
        
        if part_size_exist > expected_part_size:
            print(f"分块文件 {part_path} 大小异常，重新下载")
            try:
                part_path.unlink()
            except Exception as e:
                print(f"无法删除异常分块文件: {e}")
                return False
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
            
            # 检查实际内容长度
            content_length = int(response.headers.get('Content-Length', 0))
            if content_length == 0 and end - resume_start + 1 > 0:
                raise ValueError(f"服务器返回空内容，但预期大小为 {end - resume_start + 1} 字节")
            
            mode = 'ab' if resume_start > start else 'wb'
            with part_path.open(mode) as f:
                start_time = time.time()
                bytes_downloaded = 0
                last_update_time = start_time
                
                for chunk in response.iter_content(chunk_size=chunk_size):
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
                    
                    # 优化限速逻辑，定期检查速度
                    current_time = time.time()
                    if config['speed_limit'] > 0 and current_time - last_update_time > 0.5:
                        speed_limit_bps = config['speed_limit'] * 1024 * 1024
                        elapsed = current_time - start_time
                        expected_time = bytes_downloaded / speed_limit_bps
                        if elapsed < expected_time:
                            sleep_time = max(0, expected_time - elapsed)
                            time.sleep(sleep_time)
                        last_update_time = current_time
            
            # 验证分块大小
            downloaded_size = part_path.stat().st_size
            expected_size = end - resume_start + 1
            if downloaded_size != expected_size:
                raise ValueError(f"分块大小不符: 预期 {expected_size}, 实际 {downloaded_size}")
                
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
            # 清理损坏的分块文件
            if part_path.exists():
                try:
                    part_path.unlink()
                    print(f"已删除损坏的分块文件: {part_path}")
                except Exception as e:
                    print(f"无法删除分块文件: {str(e)}")
            return False
            
        time.sleep(2 ** (config['retries'] - retries_left))  # 指数退避重试
        
    return False

def merge_parts(file_path: str, total_parts: int) -> bool:
    """合并分块文件，支持中断检查"""
    global stop_signal
    
    if total_parts <= 0:
        print("没有分块需要合并")
        return False
        
    file_path_obj = Path(file_path)
    
    try:
        # 确保目标目录存在
        os.makedirs(file_path_obj.parent, exist_ok=True)
        
        with file_path_obj.open('wb') as outfile:
            for i in range(total_parts):
                if check_stop():
                    print("文件合并已中断")
                    return False
                    
                part_path = file_path_obj.with_suffix(f".part{i}{file_path_obj.suffix}")
                
                if not part_path.exists():
                    raise FileNotFoundError(f"分块文件丢失：{part_path}")
                    
                with part_path.open('rb') as infile:
                    # 分块读取写入，减少内存占用
                    while True:
                        chunk = infile.read(1024*1024)  # 1MB chunks
                        if not chunk:
                            break
                        outfile.write(chunk)
                    
                # 合并后立即删除分块文件，减少磁盘占用
                for _ in range(3):  # 重试3次
                    try:
                        part_path.unlink()
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
    
    file_path_obj = Path(file_path)
    
    for i in range(total_parts):
        part_path = file_path_obj.with_suffix(f".part{i}{file_path_obj.suffix}")
        
        if part_path.exists():
            for _ in range(3):  # 重试3次
                try:
                    part_path.unlink()
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
        
    file_path_obj = Path(file_path)
    
    try:
        if not file_path_obj.exists():
            print(f"文件不存在：{file_path}")
            return False
            
        hash_obj = hashlib.new(algorithm)
        with file_path_obj.open('rb') as f:
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

def download_single_thread(url: str, output_path: str, config: dict) -> bool:
    """单线程下载模式"""
    global stop_signal
    
    output_path_obj = Path(output_path)
    
    try:
        # 确保目录存在
        os.makedirs(output_path_obj.parent, exist_ok=True)
        
        session = requests.Session()
        session.headers.update(DEFAULT_HEADERS)
        
        print(f"使用单线程下载")
        
        # 检查是否可以续传
        resume_pos = 0
        if output_path_obj.exists():
            resume_pos = output_path_obj.stat().st_size
            print(f"发现已存在文件，大小为 {resume_pos/1024/1024:.2f} MB")
            
            overwrite = input("是否覆盖？(1=覆盖, 2=续传, 其他=取消): ").strip()
            if overwrite == '1':
                output_path_obj.unlink()
                resume_pos = 0
            elif overwrite == '2':
                # 检查服务器是否支持续传
                try:
                    head_resp = session.head(
                        url, 
                        allow_redirects=True, 
                        verify=config['verify_ssl'], 
                        timeout=config['timeout']
                    )
                    accept_ranges = head_resp.headers.get('Accept-Ranges', '') == 'bytes'
                    if not accept_ranges:
                        print("服务器不支持断点续传，无法续传，将从头下载")
                        output_path_obj.unlink()
                        resume_pos = 0
                except Exception as e:
                    print(f"检查续传支持时出错: {str(e)}，将从头下载")
                    output_path_obj.unlink()
                    resume_pos = 0
            else:
                print("已取消下载")
                return False
                
        headers = {'Range': f'bytes={resume_pos}-'} if resume_pos > 0 else {}
        mode = 'ab' if resume_pos > 0 else 'wb'
        
        response = session.get(url, headers=headers, stream=True, 
                              verify=config['verify_ssl'], timeout=config['timeout'])
        response.raise_for_status()
        
        # 获取实际文件大小
        content_length = response.headers.get('Content-Length')
        total_size = int(content_length) + resume_pos if content_length else None
        
        # 创建进度条
        progress = tqdm(
            total=total_size if total_size else None,
            unit='B',
            unit_scale=True,
            desc=str(output_path_obj),
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}, {remaining}]",
            initial=resume_pos
        )
        
        with output_path_obj.open(mode) as f:
            start_time = time.time()
            bytes_downloaded = resume_pos
            last_update_time = start_time
            
            for chunk in response.iter_content(chunk_size=1024*1024):
                if check_stop():
                    print("下载已中断")
                    progress.close()
                    return False
                    
                if not chunk:
                    continue
                    
                f.write(chunk)
                chunk_len = len(chunk)
                progress.update(chunk_len)
                bytes_downloaded += chunk_len
                
                # 优化限速逻辑，定期检查速度
                current_time = time.time()
                if config['speed_limit'] > 0 and current_time - last_update_time > 0.5:
                    speed_limit_bps = config['speed_limit'] * 1024 * 1024
                    elapsed = current_time - start_time
                    expected_time = bytes_downloaded / speed_limit_bps
                    if elapsed < expected_time:
                        sleep_time = max(0, expected_time - elapsed)
                        time.sleep(sleep_time)
                    last_update_time = current_time
        
        progress.close()
        print(f"下载完成: {output_path_obj}")
        return True
        
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
        if 'session' in locals():
            session.close()
    
    return False

def download_file(url: str, output_path: str = "Downloads/", num_threads: int = 8, 
                 resume: bool = False, expected_hash: Optional[str] = None, 
                 hash_algorithm: str = 'md5') -> bool:
    """主下载函数，支持中断处理，返回是否成功"""
    global stop_signal
    
    config = load_config()
    
    # 处理输出路径
    if not output_path:
        # 从URL中提取文件名，若失败则使用时间戳
        filename = url.split("/")[-1].split("?")[0] 
        if not filename or '.' not in filename:
            filename = f"download_{int(time.time())}"
        output_path = filename
    
    # 确保目录存在
    output_path_obj = Path(output_path)
    os.makedirs(output_path_obj.parent, exist_ok=True)
    
    # 安全检查文件是否存在
    if output_path_obj.exists():
        if resume:
            # 检查是否有可恢复的分块文件
            has_resumable = any(
                (output_path_obj.with_suffix(f".part{i}{output_path_obj.suffix}")).exists() 
                for i in range(config['threads'])
            )
            if not has_resumable:
                print("未找到可续传的进度，将从头开始下载")
        else:
            overwrite = input(f"文件 {output_path_obj} 已存在，是否覆盖？(y/N) ").strip().lower()
            if overwrite != 'y':
                print("已取消下载")
                return False
    
    # 检查预签名URL
    if is_presigned_url(url):
        expires = parse_expires_time(url)
        if not check_url_expiry(expires):
            return False
    
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    
    try:
        # 获取文件信息
        response = session.head(url, allow_redirects=True, verify=config['verify_ssl'])
        response.raise_for_status()
        
        # 检查最终URL是否与原URL不同
        if response.url != url:
            print(f"检测到重定向至 {response.url}，重新验证文件信息")
            # 检查重定向后的URL是否过期
            if is_presigned_url(response.url):
                expires = parse_expires_time(response.url)
                if not check_url_expiry(expires):
                    return False
                    
            response = session.head(response.url, allow_redirects=True, verify=config['verify_ssl'])
            response.raise_for_status()
        
        # 获取文件大小
        file_size = int(response.headers.get('Content-Length', 0))
        
        # 检查是否支持断点续传
        accept_ranges = response.headers.get('Accept-Ranges', '') == 'bytes'
        
        # 设置线程数 - 确保 num_threads 是整数
        if num_threads is None:
            num_threads = config['threads']
        else:
            # 确保 num_threads 是整数
            try:
                num_threads = int(num_threads)
            except (TypeError, ValueError):
                print(f"警告: 线程数无效，使用默认值 {config['threads']}")
                num_threads = config['threads']
        
        # 验证线程数 - 确保为正整数
        if num_threads <= 0:
            print(f"线程数必须为正整数，已自动调整为{config['threads']}")
            num_threads = config['threads']
        
        # 处理文件大小为0的情况
        if file_size <= 0:
            print("无法获取文件大小或文件为空，使用单线程下载")
            success = download_single_thread(url, str(output_path_obj), config)
            if success and expected_hash:
                return verify_file(str(output_path_obj), expected_hash, hash_algorithm)
            return success
        
        print(f"文件大小: {file_size/1024/1024:.2f} MB")
        
        # 不支持断点续传时使用单线程
        if not accept_ranges:
            print("服务器不支持断点续传，使用单线程下载")
            success = download_single_thread(url, str(output_path_obj), config)
            if success and expected_hash:
                return verify_file(str(output_path_obj), expected_hash, hash_algorithm)
            return success
        
        print(f"使用多线程下载 (线程数: {num_threads})")
        
        # 计算分块大小
        part_size = file_size // num_threads
        progress = tqdm(total=file_size, unit='B', unit_scale=True, desc=str(output_path_obj),
                       bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{rate_fmt}, {remaining}]")

        # 准备分块下载任务
        futures = []
        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            for i in range(num_threads):
                if check_stop():
                    print("下载已中断")
                    progress.close()
                    cleanup_temp_files(str(output_path_obj), num_threads)
                    return False
                    
                start = i * part_size
                end = start + part_size - 1 if i < num_threads - 1 else file_size - 1
                
                future = executor.submit(
                    download_range,
                    session, response.url, start, end, str(output_path_obj), i, progress, config
                )
                futures.append(future)

        # 检查所有任务是否成功
        all_success = True
        for future in as_completed(futures):
            if check_stop():
                print("下载已中断")
                progress.close()
                cleanup_temp_files(str(output_path_obj), num_threads)
                return False
                
            if not future.result():
                all_success = False

        progress.close()

        if check_stop():
            print("下载已中断")
            cleanup_temp_files(str(output_path_obj), num_threads)
            return False

        if all_success:
            print("所有分块下载完成，开始合并文件...")
            if merge_parts(str(output_path_obj), num_threads):
                print(f"文件合并完成: {output_path_obj}")
                
                # 验证文件
                if expected_hash:
                    if verify_file(str(output_path_obj), expected_hash, hash_algorithm):
                        print("文件验证成功")
                        return True
                    else:
                        print("文件验证失败，可能已损坏")
                        return False
                return True
            else:
                print("文件合并失败，请检查分块文件")
                return False
        else:
            print("部分分块下载失败，请重试")
            return False

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
        # 确保session被关闭
        session.close()
    
    return False

def check_stop() -> bool:
    """安全检查停止信号"""
    with stop_lock:
        return stop_signal

def main():
    """主程序入口"""
    # 设置信号处理（支持Windows）
    if os.name == 'nt':  # Windows系统
        try:
            import win32api
            # 修复：回调函数需要返回bool类型
            def ctrl_handler(ctrl_type: int) -> bool:
                signal_handler(None, None)
                return True  # 必须返回True表示已处理
            win32api.SetConsoleCtrlHandler(ctrl_handler, True)
        except ImportError:
            print("警告：未安装pywin32，Windows下的中断处理可能不可靠")
    else:  # Unix/Linux/macOS
        signal.signal(signal.SIGINT, signal_handler)
    
    # 命令行参数解析
    parser = argparse.ArgumentParser(description='增强版多线程下载器，支持预签名URL处理')
    
    # 将url改为可选参数
    parser.add_argument('--url', help='直接指定下载URL，无需交互式输入')
    parser.add_argument('-o', '--output', help='保存文件名')
    parser.add_argument('-t', '--threads', type=int, help='线程数')
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
            sys.exit(1)
    
    # 其他参数处理
    output = args.output
    
    # 线程数处理
    config = load_config()
    default_threads = config['threads']
    
    if args.threads is not None:
        threads = args.threads
    else:
        threads_input = input(f"请输入线程数(默认{default_threads}): ").strip()
        if threads_input:
            try:
                threads = int(threads_input)
            except ValueError:
                print(f"输入无效，使用默认线程数 {default_threads}")
                threads = default_threads
        else:
            threads = default_threads
    
    # 验证线程数 - 确保为正整数
    try:
        threads = int(threads)
        if threads <= 0:
            raise ValueError("线程数必须为正整数")
    except (TypeError, ValueError):
        print(f"线程数无效，已自动调整为{default_threads}")
        threads = default_threads
    
    # 调用下载函数
    success = download_file(
        url=url,
        output_path=output,
        num_threads=threads,
        resume=args.resume,
        expected_hash=args.hash,
        hash_algorithm=args.algorithm
    )
    
    if success:
        print("下载成功完成！")
        sys.exit(0)
    else:
        print("下载失败！")
        sys.exit(1)

if __name__ == "__main__":
    main()