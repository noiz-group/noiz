# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import operator as ope
from loguru import logger
from typing import Optional, Any, Callable, Union, Tuple

from noiz.models.type_aliases import QCOneRunnerInputs
from noiz.models import (
    CrosscorrelationCartesian, Datachunk, QCOneConfig, QCOneResults, Timespan, QCTwoResults,
    QCTwoConfig, DatachunkStats, AveragedSohGps
)
from noiz.processing.time_utils import check_if_two_timeperiods_have_any_overlap


def calculate_qcone_results_wrapper(inputs: QCOneRunnerInputs) -> Tuple[QCOneResults, ...]:
    return (
        calculate_qcone_results(
            datachunk=inputs["datachunk"],
            qcone_config=inputs["qcone_config"],
            stats=inputs["stats"],
            avg_soh_gps=inputs["avg_soh_gps"],
        ),
    )


def calculate_qcone_results(
        datachunk: Datachunk,
        qcone_config: QCOneConfig,
        stats: Optional[DatachunkStats],
        avg_soh_gps: Optional[AveragedSohGps],
) -> QCOneResults:
    """
    Performs all checks of the QCOne step. It compares values in the :class:`noiz.models.datachunk.DatachunkStats`
     and :class:`noiz.models.soh.AveragedSohGps` against :class:`noiz.models.qc.QCOneConfig` and saves the values in
     :class:`noiz.models.qc.QCOneResults` instance that can be added to db afterwards.

    :param datachunk: Datachunk to be compared
    :type datachunk: Datachunk
    :param qcone_config: QCOneConfig to have reference values to compare against
    :type qcone_config: QCOneConfig
    :param stats: Statistics of the provided Datachunk
    :type stats: DatachunkStats
    :param avg_soh_gps: Object with values of AveragedSohGps data
    :type avg_soh_gps: Optional[AveragedSohGps]
    :return: Object containing values of all performed comparisons
    :rtype: QCOneResults
    """

    if not isinstance(datachunk.timespan, Timespan):
        raise ValueError('You should load timespan together with the Datachunk.')

    logger.debug("Creating an empty QCOneResults")
    qcone_res = QCOneResults(datachunk_id=datachunk.id, qcone_config_id=qcone_config.id)
    logger.debug("Checking datachunk for main time bounds")
    qcone_res = _determine_qc_time(results=qcone_res, timespan=datachunk.timespan, config=qcone_config)
    logger.debug("Checking if datachunk within rejected time")
    qcone_res.accepted_time = _determine_if_datachunk_is_in_qcone_accepted_time(
        datachunk=datachunk,
        timespan=datachunk.timespan,
        config=qcone_config
    )
    logger.debug("Checking datachunk gps params")
    qcone_res = _determine_qcone_gps(result=qcone_res, config=qcone_config, avg_soh_gps=avg_soh_gps)
    logger.debug("Checking datachunk stats")
    qcone_res = _determine_qcone_stats(results=qcone_res, stats=stats, config=qcone_config)
    logger.debug(f"QCOneResults calculation finished for datachunk_id {datachunk.id}, qcone_config_id {qcone_config.id}")

    return qcone_res


def calculate_qctwo_results(
        crosscorrelation_cartesian: CrosscorrelationCartesian,
        qctwo_config: QCTwoConfig,
) -> QCTwoResults:
    """

    :param crosscorrelation_cartesian: CrosscorrelationCartesian to be compared
    :type crosscorrelation_cartesian: CrosscorrelationCartesian
    :param qctwo_config: QCTwoConfig to have reference values to compare against
    :type qctwo_config: QCTwoConfig
    :return: Object containing values of all performed comparisons
    :rtype: QCTwoResults
    """

    if not isinstance(crosscorrelation_cartesian.timespan, Timespan):
        raise ValueError('You should load timespan together with the Datachunk.')

    logger.debug("Creating an empty QCTwoResults")
    qctwo_res = QCTwoResults(crosscorrelation_cartesian_id=crosscorrelation_cartesian.id, qctwo_config_id=qctwo_config.id)
    logger.debug("Checking datachunk for main time bounds")
    qctwo_res = _determine_qc_time(results=qctwo_res, timespan=crosscorrelation_cartesian.timespan, config=qctwo_config)
    logger.debug("Checking if datachunk within rejected time")
    qctwo_res = _determine_qctwo_accepted_times(results=qctwo_res, crosscorrelation_cartesian=crosscorrelation_cartesian,
                                                timespan=crosscorrelation_cartesian.timespan, config=qctwo_config)

    return qctwo_res


def _determine_qc_time(
        results: Union[QCOneResults, QCTwoResults],
        timespan: Timespan,
        config: Union[QCOneConfig, QCTwoConfig],
) -> Union[QCOneResults, QCTwoResults]:
    """
    filldocs
    """
    results.starttime = compare_vals_null_safe(
        config.starttime, timespan.starttime, ope.le, null_value=config.null_value)
    results.endtime = compare_vals_null_safe(
        config.endtime, timespan.endtime, ope.ge, null_value=config.null_value)

    return results


def _determine_if_datachunk_is_in_qcone_accepted_time(
        datachunk: Datachunk,
        timespan: Timespan,
        config: QCOneConfig,
) -> bool:
    """
    This method checks if given :class:`noiz.models.datachunk.Datachunk` is contained within a time period defined
    for rejection within :class:`noiz.models.qc.QCOneConfig`.
    This check is performed based on the time of when given datachunk took place as well as the component_id
    of a datachunk defined as :py:attr:`noiz.models.datachunk.Datachunk.component_id`.



    :param datachunk: Datachunk to determine if it should be rejected
    :type datachunk: Datachunk
    :param timespan: timespan that datachunk is associated with
    :type timespan: Timespan
    :param config: QCOneConfig to check for rejected times
    :type config: QCOneConfig
    :return: If given datachunk should be accepted based on time (if it doesn't belong to any of rejected time periods)
    :rtype: bool
    """

    if datachunk.timespan_id != timespan.id:
        raise ValueError(f"Passed timespan should be the same as the one associated with datachunk. It is not!"
                         f"datachunk.timespan_id: {datachunk.timespan_id}"
                         f"timespan.id {timespan.id}")

    if datachunk.component_id not in config.component_ids_rejected_times:
        return True

    reject_checks = [True, ]

    for rej in config.time_periods_rejected:
        if not rej.component_id == datachunk.component_id:
            continue

        if not rej.starttime < rej.endtime:
            raise ValueError("Rejected starttime is after rejected endtime. It shouldn't happen. "
                             "Please check your QCOneConfig and report a bugfix to developers.")

        reject_checks.append(
            not check_if_two_timeperiods_have_any_overlap(
                rej.starttime,
                rej.endtime,
                timespan.starttime,
                timespan.endtime,
            )
        )
    return all(reject_checks)


def _determine_qctwo_accepted_times(
        results: QCTwoResults,
        crosscorrelation_cartesian: CrosscorrelationCartesian,
        timespan: Timespan,
        config: QCTwoConfig,
) -> QCTwoResults:
    """
    filldocs
    """

    reject_checks = [True, ]

    if crosscorrelation_cartesian.componentpair_id in config.componentpair_ids_rejected_times:
        for rej in config.time_periods_rejected:
            if not rej.componentpair_id == crosscorrelation_cartesian.componentpair_id:
                continue

            if not rej.starttime < rej.endtime:
                raise ValueError("Rejected starttime is after rejected endtime. It shouldn't happen. "
                                 "Please check your QCOneConfig and report a bugfix to developers.")

            reject_checks.append(
                not check_if_two_timeperiods_have_any_overlap(
                    rej.starttime,
                    rej.endtime,
                    timespan.starttime,
                    timespan.endtime,
                )
            )

    results.accepted_time = all(reject_checks)

    return results


def _determine_qcone_stats(
        results: QCOneResults,
        stats: Optional[DatachunkStats],
        config: QCOneConfig,
) -> QCOneResults:
    """
    filldocs
    """
    if stats is not None:
        results.signal_energy_max = compare_vals_null_safe(
            config.signal_energy_max, stats.energy, ope.le, config.null_value)
        results.signal_energy_min = compare_vals_null_safe(
            config.signal_energy_min, stats.energy, ope.ge, config.null_value)
        results.signal_min_value_max = compare_vals_null_safe(
            config.signal_min_value_max, stats.min, ope.le, config.null_value)
        results.signal_min_value_min = compare_vals_null_safe(
            config.signal_min_value_min, stats.min, ope.ge, config.null_value)
        results.signal_max_value_max = compare_vals_null_safe(
            config.signal_max_value_max, stats.max, ope.le, config.null_value)
        results.signal_max_value_min = compare_vals_null_safe(
            config.signal_max_value_min, stats.max, ope.ge, config.null_value)
        results.signal_mean_value_max = compare_vals_null_safe(
            config.signal_mean_value_max, stats.mean, ope.le, config.null_value)
        results.signal_mean_value_min = compare_vals_null_safe(
            config.signal_mean_value_min, stats.mean, ope.ge, config.null_value)
        results.signal_variance_max = compare_vals_null_safe(
            config.signal_variance_max, stats.variance, ope.le, config.null_value)
        results.signal_variance_min = compare_vals_null_safe(
            config.signal_variance_min, stats.variance, ope.ge, config.null_value)
        results.signal_skewness_max = compare_vals_null_safe(
            config.signal_skewness_max, stats.skewness, ope.le, config.null_value)
        results.signal_skewness_min = compare_vals_null_safe(
            config.signal_skewness_min, stats.skewness, ope.ge, config.null_value)
        results.signal_kurtosis_max = compare_vals_null_safe(
            config.signal_kurtosis_max, stats.kurtosis, ope.le, config.null_value)
        results.signal_kurtosis_min = compare_vals_null_safe(
            config.signal_kurtosis_min, stats.kurtosis, ope.ge, config.null_value)
    else:
        results.signal_energy_max = config.null_value
        results.signal_energy_min = config.null_value
        results.signal_min_value_max = config.null_value
        results.signal_min_value_min = config.null_value
        results.signal_max_value_max = config.null_value
        results.signal_max_value_min = config.null_value
        results.signal_mean_value_max = config.null_value
        results.signal_mean_value_min = config.null_value
        results.signal_variance_max = config.null_value
        results.signal_variance_min = config.null_value
        results.signal_skewness_max = config.null_value
        results.signal_skewness_min = config.null_value
        results.signal_kurtosis_max = config.null_value
        results.signal_kurtosis_min = config.null_value

    return results


def _determine_qcone_gps(
        result: QCOneResults,
        config: QCOneConfig,
        avg_soh_gps: Optional[AveragedSohGps]
) -> QCOneResults:
    """
    Compares values of provided instance of :class:`noiz.models.soh.AveragedSohGps` with values defined in
    :class:`noiz.models.qc.QCOneConfig`. If as :paramref:`noiz.api.qc.determine_qcone_gps.avg_soh_gps` will me provided
    None, all values will be set to the :py:attr:`noiz.models.qc.QCOneConfig.null_value`.
    If any of the config or real data values will also be None, the result of comparison will be set to
    :py:attr:`noiz.models.qc.QCOneConfig.null_value`.

    :param result: Object to which the results of comparisons will be saved
    :type result: QCOneResults,
    :param config: Object to take the reference values to compare against
    :type config: QCOneConfig,
    :param avg_soh_gps: Real data values that will be used in comparison
    :type avg_soh_gps: Optional[AveragedSohGps]
    :return: Object that will include results of the comparison
    :rtype: QCOneResults
    """

    if avg_soh_gps is not None:
        result.avg_gps_time_error_max = compare_vals_null_safe(
            config.avg_gps_time_error_max, avg_soh_gps.time_error, ope.ge, config.null_value)
        result.avg_gps_time_error_min = compare_vals_null_safe(
            config.avg_gps_time_error_min, avg_soh_gps.time_error, ope.le, config.null_value)
        result.avg_gps_time_uncertainty_max = compare_vals_null_safe(
            config.avg_gps_time_uncertainty_max, avg_soh_gps.time_uncertainty, ope.ge, config.null_value)
        result.avg_gps_time_uncertainty_min = compare_vals_null_safe(
            config.avg_gps_time_uncertainty_min, avg_soh_gps.time_uncertainty, ope.le, config.null_value)
    else:
        result.avg_gps_time_error_max = config.null_value
        result.avg_gps_time_error_min = config.null_value
        result.avg_gps_time_uncertainty_max = config.null_value
        result.avg_gps_time_uncertainty_min = config.null_value
    return result


def compare_vals_null_safe(a: Any, b: Any, op: Callable[[Any, Any], bool], null_value: bool):
    """
    Compares two values with provided callable. Callable, should be coming from the :py:mod:`operator`.
    It first checks if any of provided values is None and if yes, returns a provided
    :paramref:`noiz.api.qc.compare_vals_null_safe.null_value`.

    :param a: First value to compare
    :type a: Any
    :param b: Second value to compare
    :type b: Any
    :param op: Callable to perform comparison with.
    :type op: Callable[[Any, Any], bool]
    :param null_value:
    :type null_value: bool
    :return: Returns result of a call or a value of :paramref:`noiz.api.qc.compare_vals_null_safe.null_value`
    :rtype: bool
    """
    if a is None or b is None:
        return null_value
    elif (not a == a) or (not b == b):  # taking into account nan
        return null_value
    else:
        return op(a, b)
