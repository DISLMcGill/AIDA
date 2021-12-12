
import psutil
import time

# Print every 0.1s
whil e(True):
    print(psutil.cpu_percent())
    time.sleep(0.1)