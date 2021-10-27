from datetime import datetime


class App_Logger:
    """
    Custome Logger for the Application.

    """
    def __init__(self):
        pass

    def log(self, file_object, log_message):
        """
        Method to write the log message into the log file.
        input: file_object, log_message
        Output: None. 

        Version: 1.0
        """
        self.now = datetime.now()
        self.date = self.now.date()
        self.current_time = self.now.strftime("%H:%M:%S")
        file_object.write(
            str(self.date) + "/" + str(self.current_time) + "\t\t" + log_message +"\n")
