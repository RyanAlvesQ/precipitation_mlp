import re
from datetime import datetime

class ExtractDate:
    def get_datetime_from_filename(filename: str) -> datetime | None:
        """
        Extract the datetime in timestamp format of a filename
        """
        
        match = re.search(r'(\d{8})\.nc$', filename)
        print (filename)
        if match:
            date_str = match.group(1)
            try:
                return datetime.strptime(f"{date_str} 12:00:00","%Y%m%d %H:%M:%S")
            except ValueError as e:
                print(f"Erro ao converter data: {e}")
        return None