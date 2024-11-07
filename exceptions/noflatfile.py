from datetime import datetime
class NoFlatFileForTodayException(Exception):
    def __init__(self, message="No flat file available  "+ datetime.today().strftime("%Y-%m-%d %H:%M:%S")) -> None:
        self.message=message
        super().__init__(self.message)
    