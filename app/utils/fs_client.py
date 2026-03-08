from datetime import datetime
from pathlib import Path


class FSClient:
    def __init__(self, location: str) -> None:
        self.location = location

    def get_files_by_created_date(self) -> dict[Path, str]:
        folder = Path(self.location)

        files = {}
        for f in folder.iterdir():
            if f.is_file():
                files[f] = datetime.fromtimestamp(f.stat().st_ctime).strftime(  # noqa: DTZ006
                    '%Y-%m-%d %H:%M:%S'
                )

        sorted_files = dict(sorted(files.items(), key=lambda item: item[1]))

        return sorted_files
