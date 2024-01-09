#！/usr/bin/env python3
#! -*- encoding: UTF-8 *-

import random
import time

lines = []
with open("shuihu.csv", "rb") as fp:
    for line in fp.readlines():
        lines.append(line.decode('utf-8').strip())
lines[0] += ''',"布尔","浮点","整数","日期"'''
print(lines[0])
for i in range(1, len(lines)):
    lines[i] += ","
    lines[i] += '"' + ("true" if random.randint(1,10) > 5 else "false") + '",'
    lines[i] += str(random.random() * 10000.0) + ","
    lines[i] += str(random.randint(0, 10000)) + ","
    lines[i] += '"' + time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(random.random() * 86400 + 1605969199)) + '"'
    print(lines[i])
with open("shuihu1.csv", "wb") as fp:
    for line in lines:
        fp.write(('%s\r\n' % line).encode('utf-8'))
    