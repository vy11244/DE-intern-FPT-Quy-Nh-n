from datetime import datetime, time

class DateExtractor:
    def __init__(self, file_path):
        self.file_path = file_path

    def extract_date(self):
        """
        Extracts the date from the file path and returns a datetime object with the date set to noon.

        Returns:
            datetime: A datetime object with the extracted date.
        """
        components = self.file_path.split('/')
        file_name = components[-1]
        date_parts = file_name.split('_')
        date_part = '-'.join(date_parts[1:4])
        date_str = date_part.split('.')[0]
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        return datetime.combine(date_obj.date(), time(12, 0, 0))