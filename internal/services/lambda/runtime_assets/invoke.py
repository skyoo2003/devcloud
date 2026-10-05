import sys
import time
import urllib.error
import urllib.request

url = "http://127.0.0.1:8080/2015-03-31/functions/function/invocations"
if sys.argv[1] == "--ready":
    while True:
        try:
            urllib.request.urlopen(url, timeout=1).close()
            break
        except urllib.error.HTTPError:
            break
        except (urllib.error.URLError, TimeoutError):
            time.sleep(0.05)
else:
    request = urllib.request.Request(
        url, data=sys.stdin.buffer.read(), headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(request) as response:
        sys.stdout.buffer.write(response.read())
