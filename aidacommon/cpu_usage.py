import psutil
import time
#Print every 0.1s
while(True):
    print(psutil.cpu_percent())
    time.sleep(0.1)