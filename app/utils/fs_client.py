import os
from pathlib import Path
from datetime import datetime

class FSClient:
    def __init__(
        self,
        location: str
    ):
        self.location = location

    def get_files_by_created_date(self) -> list:
        folder = Path(self.location)

        files = {}
        for f in folder.iterdir():
            if f.is_file():
                files[f] = created = datetime.fromtimestamp(f.stat().st_ctime).strftime("%Y-%m-%d %H:%M:%S")
        
        sorted_files = dict(sorted(files.items(), key=lambda item: item[1]))
        
        return sorted_files