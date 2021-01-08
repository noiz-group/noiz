from loguru import logger
import obspy
import numpy as np
from pathlib import Path
from sqlalchemy.dialects.postgresql import insert

from noiz.database import db
from noiz.models.datachunk import Datachunk, ProcessedDatachunk
from noiz.models.processing_params import DatachunkParams
from noiz.models.timespan import Timespan
from noiz.models.component import Component
from noiz.processing.path_helpers import directory_exists_or_create


def whiten_trace(tr: obspy.Trace) -> obspy.Trace:
    """
    Spectrally whitens the trace. Calculates a spectrum of trace,
    divides it by its absolute value and
    inverts that spectrum back to time domain with a type conversion to real.

    :param tr: trace to be whitened
    :type tr: obspy.Trace
    :return: Spectrally whitened trace
    :rtype: obspy.Trace
    """
    spectrum = np.fft.fft(tr.data)
    inv_trace = np.fft.ifft(spectrum / abs(spectrum))
    tr.data = np.real(inv_trace)
    return tr


def one_bit_normalization(tr: obspy.Trace) -> obspy.Trace:
    """
    One-bit amplitude normalization. Uses numpy.sign

    :param tr: Trace object to be normalized
    :type tr: obspy.Trace
    :return: Normalized Trace
    :rtype: obspy.Trace
    """

    tr.data = np.sign(tr.data)

    return tr


def process_datachunk(datachunk, processing_params):
    logger.info(f"Loading data for {datachunk}")
    st = datachunk.load_data()
    if len(st) != 1:
        logger.info(
            "There are more than one trace in stream, trying to merge with default params"
        )
        st.merge()
        if len(st) != 1:
            raise ValueError("There still are more traces in stream than one.")
    if processing_params.spectral_whitening:
        logger.info("Performing spectral whitening")
        st[0] = whiten_trace(st[0])
    if processing_params.one_bit:
        logger.info("Performing one bit normalization")
        st[0] = one_bit_normalization(st[0])
    new_filepath = datachunk.filepath.replace("datachunk", "processed_datachunk")
    logger.info(f"Output file will be saved to {new_filepath}")
    directory_exists_or_create(Path(new_filepath))
    proc_dc = ProcessedDatachunk(
        processing_params_id=processing_params.id,
        datachunk_id=datachunk.id,
        filepath=new_filepath,
    )
    logger.info(
        "Checking if there are some chunks fot tht timespan and component in db"
    )
    existing_chunks = (
        db.session.query(ProcessedDatachunk)
        .filter(
            ProcessedDatachunk.datachunk_id == proc_dc.datachunk_id,
            ProcessedDatachunk.processing_params_id == proc_dc.processing_params_id,
        )
        .all()
    )
    logger.info(
        "Checking if there are some timeseries files  for tht timespan and component on the disc"
    )
    if len(existing_chunks) == 0:
        logger.info("Writing file to disc and adding entry to db")
        st.write(proc_dc.filepath, format="mseed")
        db.session.add(proc_dc)
    else:
        if not Path(proc_dc.filepath).exists():
            logger.info(
                "There is some chunk in the db so I will update it and write/overwrite file to the disc."
            )
        st.write(proc_dc.filepath, format="mseed")
        insert_command = (
            insert(ProcessedDatachunk)
            .values(
                processing_params_id=proc_dc.processing_params_id,
                datachunk_id=proc_dc.datachunk_id,
                filepath=proc_dc.filepath,
            )
            .on_conflict_do_update(
                constraint="unique_processing_per_datachunk",
                set_=dict(filepath=proc_dc.filepath),
            )
        )
        db.session.execute(insert_command)
    db.session.commit()
    return
