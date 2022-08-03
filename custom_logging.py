import time 
import json


# prints json and flushes before file is closed
def log(message, level="INFO", **extra):
    out = {"timestamp": time.time(), "severity": level, "message": message}
    if extra: out |= extra
    print(json.dumps(out), flush=True)
    return True
  
