import psutil
import time
while(True):
    print(psutil.cpu_percent())
    time.sleep(0.1)