.. SPDX-License-Identifier: CECILL-B
.. Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
.. Copyright © 2019-2023 Contributors to the Noiz project.

=====================================
S3-Compatible Storage Implementation
=====================================

**Goal**: Make Noiz compatible with S3-compatible filesystems (AWS S3, MinIO, Ceph, etc.)
while maintaining support for local storage through a unified interface.

:Status: Proposed (not yet implemented)
:Estimated Effort: 3-4 weeks
:Priority: Medium (enables cloud deployment and scalability)

.. contents:: Table of Contents
   :local:
   :depth: 2

Motivation
==========

Problems with Current Architecture
-----------------------------------

1. **Cloud Deployment Impossible**

   * All file operations assume local filesystem
   * Can't scale horizontally (workers need shared storage)
   * Can't leverage cloud storage (S3, GCS, Azure Blob)

2. **Limited Scalability**

   * Processing nodes need direct access to same filesystem
   * Network file systems (NFS) are slow and unreliable
   * No built-in caching for remote data

3. **No Storage Abstraction**

   * Tight coupling to local filesystem (``pathlib.Path`` everywhere)
   * Hard to test without real filesystem
   * Can't mock storage in tests

Benefits of S3-Compatible Storage
----------------------------------

1. **Cloud Native**

   * Deploy on AWS, GCP, Azure
   * Use managed storage services
   * Pay-per-use pricing

2. **Horizontal Scalability**

   * Workers don't need shared filesystem
   * Process data from anywhere
   * Independent scaling of compute and storage

3. **Cost Effective**

   * S3 Glacier for archival storage
   * Lifecycle policies for old data
   * No need to manage storage infrastructure

4. **Reliability**

   * Built-in replication
   * Automatic backups
   * 99.999999999% durability (11 9's)

5. **Flexibility**

   * Local storage for development
   * MinIO for self-hosted S3
   * Cloud storage for production

Technology Choice
=================

Recommended: fsspec (Filesystem Spec)
--------------------------------------

Why fsspec?
~~~~~~~~~~~

1. **Already a Dependency**

   * Dask uses fsspec internally
   * No new dependency to add
   * Tested and proven

2. **Universal Interface**

   * Single API for all storage backends
   * Drop-in replacement for ``open()``
   * Works with URI schemes: ``s3://``, ``file://``, ``gs://``, ``az://``

3. **Scientific Python Integration**

   * Works with numpy, pandas, xarray
   * ObsPy can read from fsspec filesystems
   * Seamless integration

4. **Built-in Features**

   * **Caching**: Cache remote files locally
   * **Compression**: Automatic compression handling
   * **Buffering**: Smart buffering for performance
   * **Retries**: Automatic retry on failures

5. **Wide Backend Support**

   * Local filesystem (``file://``)
   * S3 (``s3://``, ``s3a://``)
   * Google Cloud Storage (``gs://``, ``gcs://``)
   * Azure Blob Storage (``az://``, ``abfs://``)
   * HTTP/HTTPS (``http://``, ``https://``)
   * FTP (``ftp://``)
   * Memory (``memory://``)
   * Many more via plugins

Alternative: cloudpathlib
~~~~~~~~~~~~~~~~~~~~~~~~~

**Why NOT cloudpathlib?**

* Smaller ecosystem
* Less integration with scientific libraries
* Doesn't work as seamlessly with Dask
* Fewer features (no built-in caching)

Alternative: PyFilesystem2 (fs)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Why NOT PyFilesystem2?**

* Different API (not compatible with ``open()``)
* Less adoption in scientific Python
* More verbose code
* Requires more refactoring

Current File Handling Architecture
===================================

File Operation Patterns
-----------------------

Noiz currently uses **4 main patterns** for file operations:

Pattern 1: Direct Path Operations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # In processing/path_helpers.py
    def parent_directory_exists_or_create(filepath: Path) -> bool:
        directory = filepath.parent
        if not directory.exists():  # ← Assumes local filesystem
            directory.mkdir(parents=True, exist_ok=True)
        return directory.exists()

**Usage**: Throughout ``processing/`` layer for directory management

Pattern 2: ObsPy File I/O
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # In processing/datachunk.py:826
    trimmed_st.write(datachunk_file.filepath, format="mseed")

    # In processing/datachunk.py:720
    st = obspy.read(mseed_file)

**Usage**: Reading/writing seismic data (10+ locations)

Pattern 3: NumPy File I/O
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # In processing/ppsd.py:151
    np.savez_compressed(file=psd_file.filepath, **results_to_save)

    # In processing/beamforming.py
    data = np.load(file=filepath)

**Usage**: Saving/loading processed results (15+ locations)

Pattern 4: Matplotlib Plot Saving
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # In processing/ppsd.py:212
    fig.savefig(filepath, bbox_inches="tight")

**Usage**: Saving plots (5+ locations)

File Location Management
------------------------

**Current System**:

.. code-block:: python

    # In globals.py
    PROCESSED_DATA_DIR = os.environ.get("PROCESSED_DATA_DIR", "")

    # In path_helpers.py
    def assembly_filepath(processed_data_dir: Union[str, Path],
                          processing_type: Union[str, Path],
                          filepath: Union[str, Path]) -> Path:
        return Path(processed_data_dir).joinpath(processing_type).joinpath(filepath)

**Problems**:

* Assumes ``PROCESSED_DATA_DIR`` is local path
* Uses ``pathlib.Path`` (local filesystem only)
* No URI support

Path Usage Statistics
---------------------

Found in codebase:

* **23 occurrences** of ``Path()`` or ``.exists()`` or ``.mkdir()`` in ``processing/``
* **21 occurrences** of file read/write operations (``obspy.read``, ``st.write``, ``np.save``)
* **9 files** with heavy Path usage

**Key Files to Update**:

1. ``processing/path_helpers.py`` (182 lines) - **CRITICAL**: All path operations
2. ``processing/datachunk.py`` (886 lines) - ObsPy read/write
3. ``processing/datachunk_processing.py`` (452 lines) - ObsPy write
4. ``processing/ppsd.py`` (360 lines) - NumPy save, plot save
5. ``processing/beamforming.py`` (1439 lines) - NumPy save
6. ``processing/component.py`` (188 lines) - Inventory write
7. ``processing/event_detection.py`` (767 lines) - Multiple writes
8. ``processing/io.py`` (68 lines) - Generic save/load
9. ``settings.py`` (55 lines) - Config

Proposed Architecture
======================

New Filesystem Abstraction Layer
---------------------------------

Create a new module ``src/noiz/storage/`` to centralize all storage operations:

::

    src/noiz/storage/
    ├── __init__.py          # Public API
    ├── filesystem.py        # fsspec wrapper
    ├── path_helpers.py      # Updated path operations
    ├── cache.py             # Caching strategies
    └── config.py            # Storage configuration

Design Principles
-----------------

1. **URI-Based Paths**

   * All paths stored as URIs in database
   * Examples: ``s3://bucket/path``, ``file:///local/path``
   * Backward compatible: ``/local/path`` → ``file:///local/path``

2. **Transparent Caching**

   * Remote files cached locally on first access
   * Configurable cache size and eviction
   * Cache invalidation strategies

3. **Backward Compatible**

   * Existing code works without changes (where possible)
   * Gradual migration path
   * Local filesystem remains default

4. **Configuration-Driven**

   * Storage backend configured via environment variables
   * No code changes needed to switch backends
   * Support multiple storage backends simultaneously

Core Abstraction: NoizFileSystem
---------------------------------

.. code-block:: python

    # In storage/filesystem.py

    from typing import Union, Optional, BinaryIO, TextIO
    from pathlib import Path
    import fsspec
    from contextlib import contextmanager

    class NoizFileSystem:
        """
        Unified filesystem interface for Noiz.

        Supports local, S3, and other fsspec-compatible backends.
        Handles caching, retries, and URI resolution.
        """

        def __init__(self,
                     base_uri: str,
                     cache_storage: Optional[str] = None,
                     cache_size_mb: int = 1000):
            """
            Initialize filesystem.

            Args:
                base_uri: Base URI for storage (e.g., "s3://my-bucket" or "file:///data")
                cache_storage: Local directory for caching remote files
                cache_size_mb: Maximum cache size in MB
            """
            self.base_uri = base_uri
            self.protocol = self._parse_protocol(base_uri)

            # Create fsspec filesystem
            if cache_storage and self.protocol != "file":
                # Use caching filesystem for remote storage
                self.fs = fsspec.filesystem(
                    "filecache",
                    target_protocol=self.protocol,
                    cache_storage=cache_storage,
                    cache_check=3600,  # Check freshness every hour
                    expiry_time=86400,  # 24 hours
                )
            else:
                # Direct filesystem (local or remote without cache)
                self.fs = fsspec.filesystem(self.protocol)

        def open(self, path: str, mode: str = "rb", **kwargs) -> Union[BinaryIO, TextIO]:
            """
            Open a file. Drop-in replacement for built-in open().

            Args:
                path: Relative path from base_uri or absolute URI
                mode: File mode ('r', 'w', 'rb', 'wb', etc.)

            Returns:
                File-like object
            """
            full_path = self._resolve_path(path)
            return self.fs.open(full_path, mode=mode, **kwargs)

        def exists(self, path: str) -> bool:
            """Check if path exists."""
            full_path = self._resolve_path(path)
            return self.fs.exists(full_path)

        def makedirs(self, path: str, exist_ok: bool = True) -> None:
            """Create directory (no-op for S3)."""
            full_path = self._resolve_path(path)
            if self.protocol == "file":
                self.fs.makedirs(full_path, exist_ok=exist_ok)
            # S3 doesn't need directory creation

        def ls(self, path: str) -> list:
            """List directory contents."""
            full_path = self._resolve_path(path)
            return self.fs.ls(full_path)

        def rm(self, path: str, recursive: bool = False) -> None:
            """Delete file or directory."""
            full_path = self._resolve_path(path)
            self.fs.rm(full_path, recursive=recursive)

        def get_path(self, relative_path: str) -> str:
            """
            Get full URI for a relative path.

            Args:
                relative_path: Path relative to base_uri

            Returns:
                Full URI (e.g., "s3://bucket/path/file.npz")
            """
            return self._resolve_path(relative_path)

        def _resolve_path(self, path: str) -> str:
            """Resolve relative path to full URI."""
            if "://" in path:
                # Already a full URI
                return path

            # Relative path, join with base_uri
            base = self.base_uri.rstrip("/")
            rel = path.lstrip("/")
            return f"{base}/{rel}"

        @staticmethod
        def _parse_protocol(uri: str) -> str:
            """Extract protocol from URI."""
            if "://" in uri:
                return uri.split("://")[0]
            return "file"  # Default to local filesystem

        @contextmanager
        def open_context(self, path: str, mode: str = "rb", **kwargs):
            """Context manager for opening files."""
            f = self.open(path, mode=mode, **kwargs)
            try:
                yield f
            finally:
                f.close()


    # Global filesystem instance
    _noiz_fs: Optional[NoizFileSystem] = None

    def get_filesystem() -> NoizFileSystem:
        """Get the global NoizFileSystem instance."""
        global _noiz_fs
        if _noiz_fs is None:
            from noiz.storage.config import get_storage_config
            config = get_storage_config()
            _noiz_fs = NoizFileSystem(
                base_uri=config.base_uri,
                cache_storage=config.cache_dir,
                cache_size_mb=config.cache_size_mb,
            )
        return _noiz_fs

    def reset_filesystem() -> None:
        """Reset the global filesystem (useful for testing)."""
        global _noiz_fs
        _noiz_fs = None

Updated Path Helpers
--------------------

.. code-block:: python

    # In storage/path_helpers.py (replaces processing/path_helpers.py)

    from typing import Union
    from noiz.storage.filesystem import get_filesystem
    from noiz.models import Component, Timespan

    def assembly_filepath(
        processing_type: str,
        filepath: str,
    ) -> str:
        """
        Assembles a filepath for processed files.
        Returns URI string instead of Path object.

        Args:
            processing_type: Type of processing (e.g., "datachunk", "crosscorrelation")
            filepath: Relative filepath

        Returns:
            Full URI to file
        """
        fs = get_filesystem()
        relative_path = f"{processing_type}/{filepath}"
        return fs.get_path(relative_path)


    def ensure_parent_directory(filepath: str) -> bool:
        """
        Ensure parent directory exists.
        For S3, this is a no-op. For local, creates directories.

        Args:
            filepath: Full URI or relative path

        Returns:
            True if directory exists/created
        """
        fs = get_filesystem()

        # Extract parent directory
        if "/" in filepath:
            parent = "/".join(filepath.rsplit("/", 1)[:-1])
            fs.makedirs(parent, exist_ok=True)

        return True


    def file_exists(filepath: str) -> bool:
        """Check if file exists."""
        fs = get_filesystem()
        return fs.exists(filepath)


    def increment_filename_counter(filepath: str) -> str:
        """
        Find next free filepath by incrementing counter.

        Args:
            filepath: Base filepath

        Returns:
            Next available filepath
        """
        fs = get_filesystem()

        counter = 0
        while fs.exists(filepath):
            # Parse and increment counter
            parts = filepath.rsplit(".", 2)
            if len(parts) >= 2:
                parts[-2] = str(int(parts[-2]) + 1)
                filepath = ".".join(parts)
            else:
                filepath = f"{filepath}.{counter}"
            counter += 1

        return filepath

Storage Configuration
---------------------

.. code-block:: python

    # In storage/config.py

    from dataclasses import dataclass
    from typing import Optional
    from environs import Env

    @dataclass
    class StorageConfig:
        """Configuration for storage backend."""

        # Base URI for storage
        base_uri: str

        # Caching configuration
        cache_dir: Optional[str] = None
        cache_size_mb: int = 1000

        # S3-specific configuration
        s3_endpoint_url: Optional[str] = None  # For MinIO, Ceph, etc.
        s3_access_key: Optional[str] = None
        s3_secret_key: Optional[str] = None
        s3_region: Optional[str] = None

        # Performance tuning
        block_size: int = 5 * 1024 * 1024  # 5MB chunks

        @property
        def is_remote(self) -> bool:
            """Check if storage is remote (not local filesystem)."""
            return not self.base_uri.startswith("file://")


    def get_storage_config() -> StorageConfig:
        """Load storage configuration from environment."""
        env = Env()
        env.read_env()

        # Base storage location
        base_uri = env.str("STORAGE_URI", default=None)

        # Backward compatibility: PROCESSED_DATA_DIR → file:// URI
        if base_uri is None:
            processed_data_dir = env.str("PROCESSED_DATA_DIR", default="")
            if processed_data_dir:
                # Convert local path to file:// URI
                base_uri = f"file://{processed_data_dir}"
            else:
                raise ValueError(
                    "Either STORAGE_URI or PROCESSED_DATA_DIR must be set. "
                    "Example: STORAGE_URI=s3://my-bucket/noiz-data"
                )

        # Caching (for remote storage)
        cache_dir = env.str("STORAGE_CACHE_DIR", default="/tmp/noiz-cache")
        cache_size_mb = env.int("STORAGE_CACHE_SIZE_MB", default=1000)

        # S3 configuration
        s3_endpoint = env.str("S3_ENDPOINT_URL", default=None)
        s3_access_key = env.str("AWS_ACCESS_KEY_ID", default=None)
        s3_secret_key = env.str("AWS_SECRET_ACCESS_KEY", default=None)
        s3_region = env.str("AWS_DEFAULT_REGION", default="us-east-1")

        return StorageConfig(
            base_uri=base_uri,
            cache_dir=cache_dir,
            cache_size_mb=cache_size_mb,
            s3_endpoint_url=s3_endpoint,
            s3_access_key=s3_access_key,
            s3_secret_key=s3_secret_key,
            s3_region=s3_region,
        )

Implementation Plan
===================

Phase 1: Foundation (Week 1)
-----------------------------

**Goal**: Create storage abstraction layer without breaking existing code

**Tasks**:

1. **Create storage/ module structure**

   .. code-block:: bash

       mkdir -p src/noiz/storage
       touch src/noiz/storage/__init__.py
       touch src/noiz/storage/filesystem.py
       touch src/noiz/storage/path_helpers.py
       touch src/noiz/storage/config.py
       touch src/noiz/storage/cache.py

2. **Implement NoizFileSystem class** (see code above)

3. **Implement StorageConfig** (see code above)

4. **Add fsspec to dependencies**

   .. code-block:: toml

       # In pyproject.toml
       [project]
       dependencies = [
           ...
           "fsspec>=2023.1.0",
           "s3fs>=2023.1.0",  # For S3 support
       ]

5. **Write unit tests for storage layer**

   .. code-block:: python

       # tests/storage/test_filesystem.py
       def test_local_filesystem():
           fs = NoizFileSystem("file:///tmp/test")
           with fs.open("test.txt", "w") as f:
               f.write("hello")
           assert fs.exists("test.txt")

       def test_s3_filesystem(moto_s3):
           fs = NoizFileSystem("s3://test-bucket")
           # ... test S3 operations

**Deliverable**: Storage layer ready but not used yet

Phase 2: Path Helpers Migration (Week 1-2)
-------------------------------------------

**Goal**: Replace ``processing/path_helpers.py`` with storage-aware versions

**Tasks**:

1. **Create new path helpers** in ``storage/path_helpers.py``

   * Port all functions from ``processing/path_helpers.py``
   * Use ``NoizFileSystem`` instead of ``pathlib.Path``
   * Return URI strings instead of Path objects

2. **Add backward compatibility layer**

   .. code-block:: python

       # In processing/path_helpers.py (deprecated)
       import warnings
       from noiz.storage.path_helpers import *

       warnings.warn(
           "noiz.processing.path_helpers is deprecated, "
           "use noiz.storage.path_helpers instead",
           DeprecationWarning,
           stacklevel=2
       )

3. **Update imports throughout codebase**

   .. code-block:: bash

       # Find all imports
       grep -r "from noiz.processing.path_helpers" src/

       # Replace with
       from noiz.storage.path_helpers import ...

**Deliverable**: All path operations go through storage layer

Phase 3: File I/O Migration (Week 2-3)
---------------------------------------

**Goal**: Update all file read/write operations to use storage layer

**Tasks**:

1. **Create I/O wrapper functions**

   .. code-block:: python

       # In storage/io.py

       import numpy as np
       import obspy
       from noiz.storage.filesystem import get_filesystem

       def save_numpy(filepath: str, **arrays) -> None:
           """Save numpy arrays to storage."""
           fs = get_filesystem()
           with fs.open(filepath, "wb") as f:
               np.savez_compressed(f, **arrays)

       def load_numpy(filepath: str) -> np.lib.npyio.NpzFile:
           """Load numpy arrays from storage."""
           fs = get_filesystem()
           with fs.open(filepath, "rb") as f:
               return np.load(f)

       def write_stream(stream: obspy.Stream, filepath: str, format: str = "MSEED") -> None:
           """Write ObsPy stream to storage."""
           fs = get_filesystem()
           with fs.open(filepath, "wb") as f:
               stream.write(f, format=format)

       def read_stream(filepath: str) -> obspy.Stream:
           """Read ObsPy stream from storage."""
           fs = get_filesystem()
           with fs.open(filepath, "rb") as f:
               return obspy.read(f)

       def save_figure(fig, filepath: str, **kwargs) -> None:
           """Save matplotlib figure to storage."""
           fs = get_filesystem()
           with fs.open(filepath, "wb") as f:
               fig.savefig(f, **kwargs)

2. **Update processing functions** to use wrappers

   **Before**:

   .. code-block:: python

       # In processing/ppsd.py
       np.savez_compressed(file=psd_file.filepath, **results_to_save)

   **After**:

   .. code-block:: python

       # In processing/ppsd.py
       from noiz.storage.io import save_numpy
       save_numpy(psd_file.filepath, **results_to_save)

3. **Update all 21 file I/O locations**:

   * ``datachunk.py``: 4 locations (obspy read/write)
   * ``datachunk_processing.py``: 1 location (obspy write)
   * ``ppsd.py``: 4 locations (numpy save, plot save)
   * ``beamforming.py``: 5 locations (numpy save, plot save)
   * ``component.py``: 2 locations (inventory write)
   * ``event_detection.py``: 4 locations (multiple writes)
   * ``io.py``: 1 location (generic save)

**Deliverable**: All file operations storage-aware

Phase 4: Database URI Support (Week 3)
---------------------------------------

**Goal**: Store URIs instead of local paths in database

**Tasks**:

1. **Add migration to support URIs in File models**

   .. code-block:: python

       # In migrations/versions/xxx_add_uri_support.py

       def upgrade():
           # File paths can now be URIs
           # No schema change needed, but add validation
           pass

2. **Update File model validators**

   .. code-block:: python

       # In models/datachunk.py

       class DatachunkFile(db.Model):
           filepath = db.Column(db.UnicodeText, nullable=False, unique=True)

           def __init__(self, filepath: str):
               # Normalize to URI format
               if not "://" in filepath:
                   # Convert local path to file:// URI
                   filepath = f"file://{filepath}"
               self.filepath = filepath

3. **Add URI normalization utility**

   .. code-block:: python

       # In storage/utils.py

       def normalize_path_to_uri(path: str) -> str:
           """Convert local path to file:// URI if needed."""
           if "://" in path:
               return path  # Already a URI
           return f"file://{path}"

       def uri_to_local_path(uri: str) -> str:
           """Convert file:// URI to local path."""
           if uri.startswith("file://"):
               return uri[7:]  # Strip "file://"
           raise ValueError(f"Not a local file URI: {uri}")

**Deliverable**: Database can store both local paths and URIs

Phase 5: Configuration & Documentation (Week 4)
------------------------------------------------

**Goal**: Document new storage system and provide examples

**Tasks**:

1. **Update settings.py** to use ``StorageConfig``

2. **Create configuration examples**

   .. code-block:: bash

       # config_examples/storage_local.env
       STORAGE_URI=file:///data/noiz

       # config_examples/storage_s3.env
       STORAGE_URI=s3://my-bucket/noiz-data
       AWS_ACCESS_KEY_ID=xxx
       AWS_SECRET_ACCESS_KEY=xxx
       AWS_DEFAULT_REGION=us-east-1
       STORAGE_CACHE_DIR=/tmp/noiz-cache
       STORAGE_CACHE_SIZE_MB=5000

       # config_examples/storage_minio.env
       STORAGE_URI=s3://noiz-bucket
       S3_ENDPOINT_URL=http://minio:9000
       AWS_ACCESS_KEY_ID=minioadmin
       AWS_SECRET_ACCESS_KEY=minioadmin
       STORAGE_CACHE_DIR=/tmp/noiz-cache

3. **Update documentation**

   * Add storage backend guide
   * Update environment setup guide
   * Add migration guide from local to S3

4. **Create Docker Compose example with MinIO**

   .. code-block:: yaml

       # docker-compose.s3.yml
       version: '3.8'

       services:
         minio:
           image: minio/minio:latest
           command: server /data --console-address ":9001"
           environment:
             MINIO_ROOT_USER: minioadmin
             MINIO_ROOT_PASSWORD: minioadmin
           ports:
             - "9000:9000"
             - "9001:9001"
           volumes:
             - minio_data:/data

         noiz:
           build: .
           environment:
             STORAGE_URI: s3://noiz-bucket
             S3_ENDPOINT_URL: http://minio:9000
             AWS_ACCESS_KEY_ID: minioadmin
             AWS_SECRET_ACCESS_KEY: minioadmin
           depends_on:
             - minio

       volumes:
         minio_data:

5. **Add CLI command to test storage**

   .. code-block:: python

       # In cli.py

       @app.cli.command()
       def test_storage():
           """Test storage backend connectivity."""
           from noiz.storage.filesystem import get_filesystem

           fs = get_filesystem()
           click.echo(f"Storage backend: {fs.protocol}")
           click.echo(f"Base URI: {fs.base_uri}")

           # Test write
           test_path = "test/connectivity.txt"
           with fs.open(test_path, "w") as f:
               f.write("test")

           # Test read
           with fs.open(test_path, "r") as f:
               content = f.read()

           # Test delete
           fs.rm(test_path)

           click.echo("Storage backend working correctly!")

**Deliverable**: Fully documented storage system ready for use

Configuration Changes
=====================

New Environment Variables
--------------------------

.. code-block:: bash

    # Storage backend (required)
    STORAGE_URI=s3://my-bucket/noiz-data
    # Options:
    #   file:///local/path       - Local filesystem
    #   s3://bucket/path         - AWS S3
    #   gs://bucket/path         - Google Cloud Storage
    #   az://container/path      - Azure Blob Storage

    # S3-specific configuration (for non-AWS S3)
    S3_ENDPOINT_URL=http://minio:9000  # For MinIO, Ceph, etc.

    # AWS credentials (if not using IAM roles)
    AWS_ACCESS_KEY_ID=xxx
    AWS_SECRET_ACCESS_KEY=xxx
    AWS_DEFAULT_REGION=us-east-1

    # Caching (for remote storage)
    STORAGE_CACHE_DIR=/tmp/noiz-cache
    STORAGE_CACHE_SIZE_MB=5000

    # Backward compatibility (deprecated)
    PROCESSED_DATA_DIR=/data/processed  # Auto-converted to file:// URI

Docker Environment
------------------

.. code-block:: dockerfile

    # Dockerfile
    FROM python:3.10

    # Install dependencies
    RUN pip install noiz[s3]  # Include S3 dependencies

    # For AWS credentials from IAM role
    ENV AWS_METADATA_SERVICE_TIMEOUT=5
    ENV AWS_METADATA_SERVICE_NUM_ATTEMPTS=3

Code Changes Summary
====================

Files to Create (New)
---------------------

1. ``src/noiz/storage/__init__.py`` - Public storage API
2. ``src/noiz/storage/filesystem.py`` - NoizFileSystem class
3. ``src/noiz/storage/path_helpers.py`` - Storage-aware path helpers
4. ``src/noiz/storage/config.py`` - StorageConfig class
5. ``src/noiz/storage/io.py`` - I/O wrapper functions
6. ``src/noiz/storage/utils.py`` - URI utilities
7. ``src/noiz/storage/cache.py`` - Caching strategies
8. ``tests/storage/`` - Storage layer tests
9. ``config_examples/storage_*.env`` - Config examples

Files to Update (Existing)
---------------------------

1. **pyproject.toml** - Add fsspec, s3fs dependencies
2. **settings.py** - Use StorageConfig
3. **globals.py** - Update PROCESSED_DATA_DIR handling
4. **processing/path_helpers.py** - Deprecate in favor of storage/
5. **processing/datachunk.py** - Use storage I/O wrappers (4 changes)
6. **processing/datachunk_processing.py** - Use storage I/O (1 change)
7. **processing/ppsd.py** - Use storage I/O (4 changes)
8. **processing/beamforming.py** - Use storage I/O (5 changes)
9. **processing/component.py** - Use storage I/O (2 changes)
10. **processing/event_detection.py** - Use storage I/O (4 changes)
11. **processing/io.py** - Use storage layer (1 change)
12. **models/*.py** - Add URI normalization to File models
13. **cli.py** - Add storage test command

**Total**: 9 new files, 13 updated files

Migration Strategy
==================

Backward Compatibility
----------------------

**Goal**: Existing deployments continue to work without changes

**Strategy**:

1. **Automatic URI conversion**

   .. code-block:: python

       # Old way (still works)
       PROCESSED_DATA_DIR=/data/processed

       # Automatically converted to
       STORAGE_URI=file:///data/processed

2. **Path normalization in models**

   .. code-block:: python

       # Old database records: /data/processed/datachunk/2023/...
       # New database records: file:///data/processed/datachunk/2023/...
       # Both work transparently

3. **Gradual adoption**

   * Phase 1: Deploy storage layer (no behavior change)
   * Phase 2: Test with local filesystem
   * Phase 3: Test with MinIO (local S3)
   * Phase 4: Deploy to cloud S3

Migration from Local to S3
---------------------------

**Option 1: Sync existing data to S3**

.. code-block:: bash

    # Use aws cli to sync
    aws s3 sync /data/processed/ s3://my-bucket/noiz-data/

    # Update environment
    export STORAGE_URI=s3://my-bucket/noiz-data

    # Restart Noiz

**Option 2: Dual storage during migration**

Configure both local and S3, write to both, read from S3 first
(Requires custom implementation)

**Option 3: Fresh start on S3**

.. code-block:: bash

    # Keep local data for reference
    # Start fresh on S3
    export STORAGE_URI=s3://my-bucket/noiz-data

    # Re-run processing pipeline

Testing Strategy
================

Unit Tests
----------

.. code-block:: python

    # tests/storage/test_filesystem.py

    import pytest
    from noiz.storage.filesystem import NoizFileSystem

    def test_local_filesystem(tmp_path):
        """Test local filesystem operations."""
        fs = NoizFileSystem(f"file://{tmp_path}")

        # Write
        with fs.open("test.txt", "w") as f:
            f.write("hello")

        # Exists
        assert fs.exists("test.txt")

        # Read
        with fs.open("test.txt", "r") as f:
            assert f.read() == "hello"

        # Delete
        fs.rm("test.txt")
        assert not fs.exists("test.txt")


    @pytest.mark.s3
    def test_s3_filesystem(moto_s3):
        """Test S3 filesystem operations (mocked)."""
        fs = NoizFileSystem("s3://test-bucket")

        # Same tests as local
        with fs.open("test.txt", "w") as f:
            f.write("hello")

        assert fs.exists("test.txt")


    def test_caching(tmp_path, moto_s3):
        """Test that remote files are cached locally."""
        cache_dir = tmp_path / "cache"
        fs = NoizFileSystem("s3://test-bucket",
                           cache_storage=str(cache_dir))

        # Write to S3
        with fs.open("data.npz", "wb") as f:
            np.savez(f, data=np.array([1, 2, 3]))

        # Read (should cache)
        with fs.open("data.npz", "rb") as f:
            data = np.load(f)

        # Check cache exists
        assert len(list(cache_dir.glob("*"))) > 0

Integration Tests
-----------------

.. code-block:: python

    # tests/integration/test_storage_pipeline.py

    def test_datachunk_processing_with_s3(s3_storage, test_data):
        """Test full datachunk processing pipeline with S3 storage."""
        from noiz.api.datachunk import prepare_datachunks
        from noiz.processing.datachunk import prepare_datachunk

        # Process datachunk (should write to S3)
        result = prepare_datachunk(...)

        # Verify file exists in S3
        fs = get_filesystem()
        assert fs.exists(result.file.filepath)

        # Verify can read back
        st = result.load_data()
        assert len(st) > 0

Manual Testing with MinIO
--------------------------

.. code-block:: bash

    # 1. Start MinIO
    docker-compose -f docker-compose.s3.yml up -d

    # 2. Create bucket
    aws --endpoint-url http://localhost:9000 s3 mb s3://noiz-test

    # 3. Configure Noiz
    export STORAGE_URI=s3://noiz-test
    export S3_ENDPOINT_URL=http://localhost:9000
    export AWS_ACCESS_KEY_ID=minioadmin
    export AWS_SECRET_ACCESS_KEY=minioadmin

    # 4. Test storage
    uv run noiz test-storage

    # 5. Run processing
    uv run noiz processing prepare_datachunks ...

    # 6. Verify files in MinIO
    aws --endpoint-url http://localhost:9000 s3 ls s3://noiz-test/ --recursive

Integration with Existing TODO
===============================

Dependencies
------------

**Blocking**: None (can implement independently)

**Blocked by**:

* Phase 0.1: Fix scipy version (must work before testing)
* Phase 0.4: SQLAlchemy 2.0 migration (DB changes)

**Synergies**:

* Phase 1: UUID migration - Can implement together (both change File models)
* Phase 4: Remove Dask - Dask already uses fsspec, makes this easier
* Architecture: File tracking fixes - Should fix tracking BEFORE adding S3

Recommended Order
-----------------

1. **First**: Complete Phase 0 (Infrastructure fixes)
2. **Second**: Fix file tracking issues (:doc:`architecture` Priority 2)
3. **Third**: Implement S3 storage (this document)
4. **Fourth**: UUID migration (:doc:`refactoring_roadmap` Phase 1)

**Rationale**:

* Fix infrastructure first (unblocks everything)
* Fix file tracking so all files are properly tracked before S3
* Add S3 storage while file handling is fresh in mind
* UUID migration last (most complex, benefits from clean storage layer)

Updated Timeline
----------------

::

    Week 1-5:   Phase 0 (Infrastructure fixes)
    Week 6-7:   File tracking fixes
    Week 8-11:  S3 storage implementation  ← NEW
    Week 12-17: UUID migration
    Week 18-22: QC renaming + code quality
    Week 23-26: Modernization (Dask removal, etc.)

**New Total**: 26-30 weeks (6-7.5 months)

Appendix
========

fsspec URI Schemes
------------------

+----------+----------------------------+----------------------------+--------------------------------------+
| Scheme   | Backend                    | Example                    | Notes                                |
+==========+============================+============================+======================================+
| file://  | Local filesystem           | file:///data/noiz          | Default, no special deps             |
+----------+----------------------------+----------------------------+--------------------------------------+
| s3://    | AWS S3                     | s3://bucket/path           | Requires s3fs                        |
+----------+----------------------------+----------------------------+--------------------------------------+
| s3a://   | AWS S3 (Spark compat)      | s3a://bucket/path          | Same as s3://                        |
+----------+----------------------------+----------------------------+--------------------------------------+
| gs://    | Google Cloud Storage       | gs://bucket/path           | Requires gcsfs                       |
+----------+----------------------------+----------------------------+--------------------------------------+
| az://    | Azure Blob Storage         | az://container/path        | Requires adlfs                       |
+----------+----------------------------+----------------------------+--------------------------------------+
| http://  | HTTP                       | http://example.com/data    | Read-only                            |
+----------+----------------------------+----------------------------+--------------------------------------+
| ftp://   | FTP                        | ftp://server/path          | Requires credentials                 |
+----------+----------------------------+----------------------------+--------------------------------------+
| memory://| In-memory                  | memory://temp              | For testing                          |
+----------+----------------------------+----------------------------+--------------------------------------+

fsspec Features
---------------

* **Caching**: filecache protocol wraps any backend with local cache
* **Compression**: Automatic handling of .gz, .bz2, .xz
* **Buffering**: Smart buffering for network filesystems
* **Retries**: Automatic retry on transient failures
* **Globbing**: Glob patterns work across all backends
* **Directory operations**: mkdir, ls, rm work everywhere
* **Async**: Async I/O support for high performance

Performance Tuning
------------------

.. code-block:: python

    # Increase block size for large files
    fs = NoizFileSystem("s3://bucket",
                       block_size=10*1024*1024)  # 10MB blocks

    # Disable caching for write-only workloads
    fs = NoizFileSystem("s3://bucket",
                       cache_storage=None)

    # Aggressive caching for read-heavy workloads
    fs = NoizFileSystem("s3://bucket",
                       cache_storage="/mnt/fast-ssd/cache",
                       cache_size_mb=50000)  # 50GB cache

Cost Considerations
-------------------

**AWS S3 Pricing** (us-east-1, as of 2023):

* Storage: $0.023/GB/month (Standard)
* Storage: $0.0125/GB/month (Infrequent Access)
* Storage: $0.004/GB/month (Glacier)
* GET requests: $0.0004/1000 requests
* PUT requests: $0.005/1000 requests
* Data transfer out: $0.09/GB (first 10TB)

**Example**: 10TB of seismic data

* Storage: 10,000 GB × $0.023 = $230/month
* Processing (1M GETs): 1,000 × $0.0004 = $0.40
* Download (100GB): 100 × $0.09 = $9

**Total**: ~$240/month for 10TB + processing

**Cost optimization**:

* Use Glacier for old data (80% savings)
* Use caching to reduce GET requests
* Use CloudFront for data delivery
* Use S3 Transfer Acceleration for uploads

References
==========

* fsspec documentation: https://filesystem-spec.readthedocs.io/
* s3fs documentation: https://s3fs.readthedocs.io/
* ObsPy I/O: https://docs.obspy.org/packages/autogen/obspy.core.stream.Stream.write.html
* NumPy I/O: https://numpy.org/doc/stable/reference/routines.io.html
* MinIO: https://min.io/
* AWS S3: https://aws.amazon.com/s3/

See Also
========

* :doc:`refactoring_roadmap` - Complete refactoring plan
* :doc:`architecture` - Code quality and architecture analysis
* :doc:`../coding_standards` - Coding conventions
* :doc:`../environment_setup` - Environment setup guide

Document Information
====================

:Version: 1.0
:Last Updated: 2025-10-15
:Author: S3 Storage Implementation Plan (Claude Code)
