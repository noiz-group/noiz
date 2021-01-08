from loguru import logger
import obspy
import numpy as np

from noiz.models.datachunk import Datachunk, ProcessedDatachunk, ProcessedDatachunkFile
from noiz.models.processing_params import ProcessedDatachunkParams
from noiz.models.timespan import Timespan
from noiz.models.component import Component
from noiz.processing.path_helpers import directory_exists_or_create, assembly_filepath, assembly_preprocessing_filename, \
    assembly_sds_like_dir, increment_filename_counter
from noiz.globals import PROCESSED_DATA_DIR


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


def process_datachunk(datachunk: Datachunk, params: ProcessedDatachunkParams) -> ProcessedDatachunk:
    """
    filldocs
    """

    if not isinstance(datachunk.timespan, Timespan):
        raise ValueError('The Timespan is not loaded with the Datachunk. Correct that.')
    if not isinstance(datachunk.component, Component):
        raise ValueError('The Component is not loaded with the Datachunk. Correct that.')

    logger.info(f"Loading data for {datachunk}")
    st = datachunk.load_data()

    if len(st) != 1:
        msg = f"There are more than one trace in stream in {datachunk}"
        logger.error(msg)
        raise ValueError(msg)

    if params.spectral_whitening:
        logger.info("Performing spectral whitening")
        st[0] = whiten_trace(st[0])

    if params.one_bit:
        logger.info("Performing one bit normalization")
        st[0] = one_bit_normalization(st[0])

    filepath = assembly_filepath(
        PROCESSED_DATA_DIR,  # type: ignore
        "processed_datachunk",
        assembly_sds_like_dir(datachunk.component, datachunk.timespan) \
        .joinpath(assembly_preprocessing_filename(
            component=datachunk.component,
            timespan=datachunk.timespan,
            count=0
        )),
    )

    if filepath.exists():
        logger.info(f'Filepath {filepath} exists. '
                    f'Trying to find next free one.')
        filepath = increment_filename_counter(filepath=filepath)
        logger.info(f"Free filepath found. "
                    f"Datachunk will be saved to {filepath}")

    logger.info(f"Chunk will be written to {str(filepath)}")
    directory_exists_or_create(filepath)

    proc_datachunk_file = ProcessedDatachunkFile(filepath=str(filepath))
    st.write(proc_datachunk_file.filepath, format="mseed")

    processed_datachunk = ProcessedDatachunk(
        processed_datachunk_params_id=params.id,
        datachunk_id=datachunk.id,
        processed_datachunk_file=proc_datachunk_file,
    )

    return processed_datachunk
