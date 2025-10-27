# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

from typing import Union, Collection, Generator, TypedDict, List, Dict, Any
import subprocess
import json
import tempfile
from datetime import datetime
from loguru import logger
from pathlib import Path


def run_mseedindex_on_passed_dir(
    basedir: Union[Path, Collection[Path]],
    current_dir: Path,
    mseedindex_executable: str,
    filename_pattern: str = "*",
    parallel: bool = True,
) -> int:
    """
    Processes all miniSEED files with mseedindex in a single bulk operation.

    Uses mseedindex JSON mode to process all files at once, then inserts metadata
    into raw_data_index table in bulk. This is much more efficient than processing
    files individually.

    :param basedir: Directory to rglob for files
    :type basedir: Union[Path, Collection[Path]]
    :param current_dir: Current directory for execution
    :type current_dir: Path
    :param mseedindex_executable: Path to mseedindex executable
    :type mseedindex_executable: str
    :param filename_pattern: Pattern to rglob with
    :type filename_pattern: str
    :param parallel: Whether to process files in parallel
    :type parallel: bool
    :return: Number of entries inserted
    :rtype: int
    """
    # Import here to avoid circular dependencies
    from noiz.models.timeseries import Tsindex
    from noiz.database import db

    # Collect all file paths
    if isinstance(basedir, Path):
        filepaths = list(basedir.absolute().rglob(filename_pattern))
    elif isinstance(basedir, str):
        filepaths = list(Path(basedir).absolute().rglob(filename_pattern))
    else:
        filepaths = []
        for dirpath in basedir:
            dirpath = Path(dirpath)
            filepaths.extend(list(dirpath.absolute().rglob(filename_pattern)))

    # Filter to only files
    filepaths = [f for f in filepaths if f.is_file()]

    if not filepaths:
        logger.warning("No files found to index")
        return 0

    logger.info(f"Found {len(filepaths)} files to index")

    # Process all files in a single mseedindex call
    all_entries = _call_mseedindex_bulk(
        filepaths=filepaths,
        current_dir=current_dir,
        mseedindex_executable=mseedindex_executable,
    )

    # Bulk insert all entries into database
    logger.info(f"Inserting {len(all_entries)} entries into raw_data_index table")

    # Use bulk insert for better performance
    if all_entries:
        db.session.bulk_insert_mappings(Tsindex, all_entries)
        db.session.commit()
        logger.info(f"Successfully inserted {len(all_entries)} entries")
    else:
        logger.warning(f"No entries to insert (found {len(filepaths)} files but mseedindex produced 0 entries)")
        if len(filepaths) > 0:
            logger.error(f"Expected entries from {len(filepaths)} files, but none were produced by mseedindex")

    return len(all_entries)


def _call_mseedindex_bulk(
    filepaths: List[Path],
    current_dir: Path,
    mseedindex_executable: str,
) -> List[Dict[str, Any]]:
    """
    Runs mseedindex in JSON mode on multiple files at once (bulk operation).

    This is much more efficient than processing files individually as it:
    - Makes a single mseedindex call for all files
    - Parses one JSON output
    - Returns all entries for bulk database insertion

    :param filepaths: List of file paths to process
    :type filepaths: List[Path]
    :param current_dir: Current directory for execution
    :type current_dir:  Path
    :param mseedindex_executable: Path to mseedindex executable
    :type mseedindex_executable:  str
    :return: List of parsed index entries for all files
    :rtype: List[Dict[str, Any]]
    """
    try:
        # Create temporary JSON output file
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as tmp_file:
            json_output_path = tmp_file.name

        try:
            cmd = [mseedindex_executable]
            cmd.extend(["-json", json_output_path])
            cmd.append("-ns")  # No sync - don't try to connect to database
            cmd.append("-v")

            # Add all file paths
            for filepath in filepaths:
                cmd.append(str(filepath.absolute()))

            logger.info(f"Running mseedindex command on {len(filepaths)} files")
            logger.info(f"Command: {' '.join(cmd[:5])}... (+ {len(cmd) - 5} more args)")
            proc = subprocess.Popen(
                cmd,
                cwd=str(current_dir.absolute()),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            out, err = proc.communicate()

            if proc.returncode != 0:
                logger.error(f"mseedindex failed with return code {proc.returncode}")
                logger.error(f"STDOUT: {out.decode()}")
                logger.error(f"STDERR: {err.decode()}")
                return []
            else:
                logger.info("mseedindex completed successfully")
                if out:
                    logger.info(f"STDOUT: {out.decode()}")
                if err:
                    logger.warning(f"STDERR: {err.decode()}")

            # Parse JSON output
            with open(json_output_path, "r") as f:
                json_data = json.load(f)

            logger.info(f"Parsed JSON with {len(json_data)} file entries")

            # Convert JSON data to database-ready format
            parsed_entries = _parse_mseedindex_json(json_data)
            logger.info(f"Parsed {len(parsed_entries)} database entries from JSON")

            return parsed_entries

        finally:
            # Clean up temporary file
            Path(json_output_path).unlink(missing_ok=True)

    except Exception as err:
        logger.error(f"Error running command `{mseedindex_executable}` - {err}")
        raise OSError(f"Error running command `{mseedindex_executable}` - {err}") from err


def _parse_mseedindex_json(json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Parse mseedindex JSON output into database-ready format.

    :param json_data: JSON output from mseedindex
    :type json_data: Dict[str, Any]
    :return: List of parsed entries for raw_data_index table
    :rtype: List[Dict[str, Any]]
    """
    entries = []
    skipped_count = 0

    for file_path, file_data in json_data.items():
        # file_data contains metadata for one file
        # file_path is the key (absolute path to the file)
        content_list = file_data.get("content", [])
        logger.info(f"Processing file {file_path} with {len(content_list)} content entries")

        for content_entry in content_list:
            # Parse source_id: "FDSN:TD_TD11_00_C_H_N" -> network, station, location, band, instrument, component
            source_id = content_entry.get("source_id", "")
            if source_id.startswith("FDSN:"):
                source_id = source_id[5:]  # Remove "FDSN:" prefix

            # Split source_id into parts
            parts = source_id.split("_")
            if len(parts) >= 6:
                network = parts[0]
                station = parts[1]
                location = parts[2]
                # Channel is band + instrument + component (e.g., "CHN")
                channel = parts[3] + parts[4] + parts[5]
            else:
                logger.warning(f"Could not parse source_id: {source_id} (parts: {parts})")
                skipped_count += 1
                continue

            # Convert timestamps from nanoseconds to datetime (UTC)
            start_ns = content_entry.get("start")
            end_ns = content_entry.get("end")
            starttime = datetime.utcfromtimestamp(start_ns / 1e9) if start_ns else None
            endtime = datetime.utcfromtimestamp(end_ns / 1e9) if end_ns else None

            # Extract sample rate from timespans
            samplerate = None
            timespans = content_entry.get("ts_timespans", [])
            if timespans and len(timespans) > 0:
                samplerate = timespans[0].get("sample_rate")

            # Parse filemodtime
            filemodtime_str = file_data.get("path_modtime")
            filemodtime = datetime.fromisoformat(filemodtime_str.replace("Z", "+00:00")) if filemodtime_str else None

            # Parse updated time
            updated_str = content_entry.get("updated")
            updated = datetime.fromisoformat(updated_str.replace("Z", "+00:00")) if updated_str else None

            entry = {
                "network": network,
                "station": station,
                "location": location,
                "channel": channel,
                "starttime": starttime,
                "endtime": endtime,
                "samplerate": samplerate,
                "filename": file_path,  # Use the file path from JSON key
                "quality": None,  # Not in JSON output
                "version": content_entry.get("publication_version"),
                "byteoffset": content_entry.get("byte_offset"),
                "bytes": content_entry.get("byte_count"),
                "hash": content_entry.get("md5"),
                "format": file_data.get("content_type"),
                "filemodtime": filemodtime,
                "updated": updated,
                "scanned": datetime.utcnow(),
            }
            entries.append(entry)

    logger.info(f"Parsed {len(entries)} total entries, skipped {skipped_count} entries due to parse errors")
    return entries
