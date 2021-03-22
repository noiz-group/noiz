from typing import Tuple, Optional

from loguru import logger
import obspy
import numpy as np

from noiz.api.type_aliases import ProcessDatachunksInputs
from noiz.models.datachunk import Datachunk, ProcessedDatachunk, ProcessedDatachunkFile, DatachunkFile
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


def process_datachunk_wrapper(
        inputs: ProcessDatachunksInputs,
) -> Tuple[ProcessedDatachunk, ...]:
    """
    Thin wrapper around :py:meth:`noiz.processing.datachunk_processing.process_datachunk` that converts a single
    TypedDict of input to standard keyword arguments. It also converts a single output to tuple so the upsertion method
    is able to properly process it.

    :param inputs: TypedDict with all required inputs
    :type inputs: noiz.api.type_aliases.ProcessDatachunksInputs
    :return: Tuple with processing result
    :rtype: Tuple[noiz.models.datachunk.ProcessedDatachunk, ...]
    """

    return (
        process_datachunk(
            datachunk=inputs["datachunk"],
            params=inputs["params"],
            datachunk_file=inputs["datachunk_file"],
        ),
    )


def process_datachunk(
        datachunk: Datachunk,
        params: ProcessedDatachunkParams,
        datachunk_file: Optional[DatachunkFile] = None,
) -> ProcessedDatachunk:
    """
    Method that allows for processing of the datachunks.
    It can perform spectral whitening in full spectrum as well as one bit normalization.

    :param datachunk: Datachunk to be processed
    :type datachunk: ~noiz.models.datachunk.Datachunk
    :param params: Processing parameters
    :type params: ~noiz.models.processing_params.ProcessedDatachunkParams
    :param datachunk_file: Optional DatachunkFile to be have data loaded from
    :type datachunk_file: Optional[~noiz.models.datachunk.DatachunkFile]
    :return:
    :rtype: noiz.models.datachunk.ProcessedDatachunk
    """

    if not isinstance(datachunk.timespan, Timespan):
        msg = 'The Timespan is not loaded with the Datachunk. Correct that.'
        logger.error('The Timespan is not loaded with the Datachunk. Correct that.')
        raise ValueError(msg)
    if not isinstance(datachunk.component, Component):
        msg = 'The Component is not loaded with the Datachunk. Correct that.'
        logger.error(msg)
        raise ValueError(msg)

    logger.info(f"Starting processing of {datachunk}")

    logger.debug("Loading data")
    st = datachunk.load_data(datachunk_file=datachunk_file)

    if len(st) != 1:
        msg = f"There are more than one trace in stream in {datachunk}"
        logger.error(msg)
        raise ValueError(msg)

    if params.spectral_whitening:
        logger.debug("Performing spectral whitening")
        st[0] = whiten_trace(st[0])

    if params.one_bit:
        logger.debug("Performing one bit normalization")
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
        logger.debug(f"Filepath {filepath} exists. Trying to find next free one.")
        filepath = increment_filename_counter(filepath=filepath, extension=False)
        logger.debug(f"Free filepath found. Datachunk will be saved to {filepath}")

    logger.info(f"Chunk will be written to {str(filepath)}")
    directory_exists_or_create(filepath)

    proc_datachunk_file = ProcessedDatachunkFile(filepath=str(filepath))

    logger.debug("Trying to write mseed file.")
    st.write(proc_datachunk_file.filepath, format="mseed")
    logger.info("File written succesfully")

    processed_datachunk = ProcessedDatachunk(
        processed_datachunk_params_id=params.id,
        datachunk_id=datachunk.id,
        file=proc_datachunk_file,
    )

    return processed_datachunk
