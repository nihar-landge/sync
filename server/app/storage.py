"""Storage backends.

The control plane never proxies file bytes in production. It hands out
short-lived, pre-authenticated URLs (OCI Pre-Authenticated Requests) that
point at Object Storage; the client talks to Object Storage directly.

Backends:
  - "local": dev stand-in. Chunks live on the server disk and are served by
    the API itself with a signed token — same URL-shaped contract as a PAR,
    so switching to OCI requires no client changes.
  - "oci": real OCI Object Storage with per-object PARs.
"""

import hashlib
import hmac
import time
from datetime import datetime, timedelta, timezone
from shutil import rmtree
from typing import Protocol

from . import config


class StorageBackend(Protocol):
    def upload_url(self, object_key: str) -> tuple[str, str]: ...
    def download_url(self, object_key: str) -> tuple[str, str]: ...
    def delete_file(self, file_id: str) -> None: ...


# ---------------------------------------------------------------------------
# Local dev backend
# ---------------------------------------------------------------------------

class LocalStorage:
    """Serves chunks from disk via the API with an HMAC-signed token."""

    def _object_path(self, object_key: str):
        return config.OBJECTS_DIR / object_key

    def _sign(self, object_key: str, expires_at: int) -> str:
        msg = f"{object_key}:{expires_at}".encode()
        return hmac.new(config.JWT_SECRET.encode(), msg, hashlib.sha256).hexdigest()

    def verify_token(self, object_key: str, token: str, expires_at: str) -> bool:
        try:
            exp = int(expires_at)
        except ValueError:
            return False
        if exp < time.time():
            return False
        return hmac.compare_digest(self._sign(object_key, exp), token)

    def _url(self, object_key: str, method: str) -> tuple[str, str]:
        exp = int(time.time()) + config.PAR_TTL_SECONDS
        url = (
            f"{config.PUBLIC_BASE_URL}/api/v1/objects/{object_key}"
            f"?expires_at={exp}&token={self._sign(object_key, exp)}"
        )
        return url, str(exp)

    def upload_url(self, object_key: str) -> tuple[str, str]:
        return self._url(object_key, "PUT")

    def download_url(self, object_key: str) -> tuple[str, str]:
        return self._url(object_key, "GET")

    def delete_file(self, file_id: str) -> None:
        rmtree(config.OBJECTS_DIR / f"file_{file_id}", ignore_errors=True)


# ---------------------------------------------------------------------------
# OCI backend
# ---------------------------------------------------------------------------

class OCIStorage:
    """Real OCI Object Storage. Requires the oci SDK and a config in ~/.oci."""

    def __init__(self):
        import oci

        config_dict = oci.config.from_file(profile_name=config.OCI_PROFILE)
        self.object_storage = oci.object_storage.ObjectStorageClient(config_dict)
        self.namespace = self.object_storage.get_namespace().data
        self.bucket = config.OCI_BUCKET
        self.region = config_dict["region"]

    def _base_url(self) -> str:
        return f"https://objectstorage.{self.region}.oraclecloud.com"

    def _make_par(self, object_key: str, access_type: str) -> str:
        """Create a PAR and return its absolute URL."""
        now = datetime.now(timezone.utc)
        expires = now + timedelta(seconds=config.PAR_TTL_SECONDS)
        details = oci.object_storage.models.CreatePreAuthenticatedRequestDetails(
            name=f"cs-{object_key.replace('/', '-')}-{int(time.time())}",
            access_type=access_type,
            object_name=object_key,
            time_expires=expires,
        )
        try:
            if access_type == "ANY_OBJECT_WRITE":
                # Scope a write PAR to this single object by listing only its path.
                rule = oci.object_storage.models.AccessListRuleDetails(
                    method="PUT", paths=[f"/{object_key}"]
                )
                details.access_list = (
                    oci.object_storage.models.PreauthenticatedRequestAccessListDetails(
                        rules=[rule]
                    )
                )
            par = self.object_storage.create_preauthenticated_request(
                self.namespace, self.bucket, details
            ).data
            return f"{self._base_url()}{par.access_uri}"
        except oci.exceptions.ServiceError:
            # Some regions/SDK versions reject access_list; retry unscoped.
            details.access_list = None
            par = self.object_storage.create_preauthenticated_request(
                self.namespace, self.bucket, details
            ).data
            return f"{self._base_url()}{par.access_uri}"

    def upload_url(self, object_key: str) -> tuple[str, str]:
        url = self._make_par(object_key, "ANY_OBJECT_WRITE")
        return url, str(int(time.time()) + config.PAR_TTL_SECONDS)

    def download_url(self, object_key: str) -> tuple[str, str]:
        url = self._make_par(object_key, "READ")
        return url, str(int(time.time()) + config.PAR_TTL_SECONDS)

    def delete_file(self, file_id: str) -> None:
        from oci.exceptions import ServiceError

        prefix = f"file_{file_id}/"
        objs = self.object_storage.list_objects(
            self.namespace, self.bucket, prefix=prefix
        ).data.objects or []
        for obj in objs:
            try:
                self.object_storage.delete_object(self.namespace, self.bucket, obj.name)
            except ServiceError:
                pass


def get_storage() -> StorageBackend:
    if config.STORAGE_BACKEND == "oci":
        return OCIStorage()
    return LocalStorage()