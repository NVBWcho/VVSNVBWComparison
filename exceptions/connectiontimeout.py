from datetime import datetime
class ConnectionTimeoutException(Exception):
    def __init__(self, message="Connection Timed out at"+ datetime.today().strftime("%Y-%m-%d %H:%M:%S")) -> None:
        self.message=message
        super().__init__(self.message)