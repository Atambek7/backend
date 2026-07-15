import os
import shutil
from config import MAIN_HOST
from .base import ImageStorage


class LocalImageStorage(ImageStorage):
    def __init__(
        self,
        base_dir: str = "/app/media/upload-images",
        url_prefix: str = "/upload-images",
        public_base_url: str | None = None,
    ):
        self.base_dir = base_dir
        self.url_prefix = "/" + url_prefix.strip("/")
        env_public_base_url = os.getenv("LOCAL_IMAGE_PUBLIC_BASE_URL")
        self.public_base_url = (
            public_base_url or env_public_base_url or f"https://{MAIN_HOST}"
        ).rstrip("/")
        os.makedirs(self.base_dir, exist_ok=True)

    def _public_url_for_filename(self, filename: str) -> str:
        return f"{self.public_base_url}{self.url_prefix}/{filename}"

    def upload(self, local_path: str) -> str:
        filename = os.path.basename(local_path)
        target_path = os.path.join(self.base_dir, filename)

        if os.path.abspath(local_path) != os.path.abspath(target_path):
            shutil.move(local_path, target_path)

        return self._public_url_for_filename(filename)
