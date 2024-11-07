class DatabaseAlreadyFilledException(Exception):
    def __init__(self, message="Database already filled for current day") -> None:
        self.message=message
        super().__init__(self.message)
        
    