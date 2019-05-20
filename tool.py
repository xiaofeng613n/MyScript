#!/usr/bin/python
# coding:utf-8
import time
import requests
import hashlib

# 约定加密字符串
encryp_str = "P0ryBqVy8Hq8Fp2p"

# 时间戳
timestamp = int(time.time())
timestamp = str(timestamp)

# 时间戳 + 加密字符
encryp_str_time = encryp_str + timestamp

# 反转
encryp_str_time = encryp_str_time[::-1]

# md5 加密生成 token
# 有效期 1 分钟
devops_token = hashlib.md5(encryp_str_time).hexdigest()

# token 和 timestamp 加入请求头
# devopstoken: token
headers = {
    "devopstoken": devops_token,
    "timestamp": timestamp
}

# 请求示例
url = "http://xx:8000/api/external/host_apps"
params = {
    "ip": "xx",
}
data = requests.get(url=url, headers=headers, params=params)

print data.json()