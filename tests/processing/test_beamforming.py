import pandas as pd
import numpy as np
import pytest

from noiz.processing.beamforming import BeamformerKeeper


@pytest.fixture()
def beamformerkeeper():
    axis = np.array([1, 2, 3])
    time = np.array([10, 11, 12])
    st = pd.Timestamp(2020, 1, 1, 1, 3, 5).to_datetime64()
    mt = pd.Timestamp(2020, 1, 1, 1, 18, 5).to_datetime64()
    et = pd.Timestamp(2020, 1, 1, 1, 33, 5).to_datetime64()
    bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=True, save_relpow=True,
                          starttime=st, midtime=mt, endtime=et)
    return (axis, time, st, mt, et, bk)


class TestBeamformerKeeper:
    def test_get_midtimes(self, beamformerkeeper):
        (_, time, _, _, _, bk) = beamformerkeeper

        apows = [
            np.ones((3, 3)) * 10,
            np.ones((3, 3)) * 20,
            np.ones((3, 3)) * 30,
        ]
        relpows = [
            np.ones((3, 3)) * 100,
            np.ones((3, 3)) * 200,
            np.ones((3, 3)) * 300,
        ]
        i_vals = [0, 1, 2]

        for apow, relpow, i_val in zip(apows, relpows, i_vals):
            bk.save_beamformers(relpow, apow, i_val)

        assert np.array_equal(bk.get_midtimes(), time)

    def test_save_beamformers_both(self, beamformerkeeper):
        (_, _, _, _, _, bk) = beamformerkeeper

        apows = [
            np.ones((3, 3)) * 10,
            np.ones((3, 3)) * 20,
            np.ones((3, 3)) * 30,
        ]
        relpows = [
            np.ones((3, 3)) * 100,
            np.ones((3, 3)) * 200,
            np.ones((3, 3)) * 300,
        ]
        i_vals = [1000, 2000, 3000]

        for apow, relpow, i_val in zip(apows, relpows, i_vals):
            bk.save_beamformers(relpow, apow, i_val)

        assert bk.midtime_samples == i_vals
        for expected, got in zip(apows, bk.abs_pows):
            assert np.array_equal(expected, got)
        for expected, got in zip(relpows, bk.rel_pows):
            assert np.array_equal(expected, got)

    def test_save_beamformers_abspows_only(self, beamformerkeeper):
        (axis, time, st, mt, et, _) = beamformerkeeper

        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=True, save_relpow=False,
                              starttime=st, midtime=mt, endtime=et)

        apows = [
            np.ones((3, 3)) * 10,
            np.ones((3, 3)) * 20,
            np.ones((3, 3)) * 30,
        ]
        relpows = [
            np.ones((3, 3)) * 100,
            np.ones((3, 3)) * 200,
            np.ones((3, 3)) * 300,
        ]
        i_vals = [1000, 2000, 3000]

        for apow, relpow, i_val in zip(apows, relpows, i_vals):
            bk.save_beamformers(relpow, apow, i_val)

        assert bk.midtime_samples == i_vals
        for expected, got in zip(apows, bk.abs_pows):
            assert np.array_equal(expected, got)
        assert bk.rel_pows == []

    def test_save_beamformers_relpows_only(self, beamformerkeeper):
        (axis, time, st, mt, et, _) = beamformerkeeper

        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=False, save_relpow=True,
                              starttime=st, midtime=mt, endtime=et)

        apows = [
            np.ones((3, 3)) * 10,
            np.ones((3, 3)) * 20,
            np.ones((3, 3)) * 30,
        ]
        relpows = [
            np.ones((3, 3)) * 100,
            np.ones((3, 3)) * 200,
            np.ones((3, 3)) * 300,
        ]
        i_vals = [1000, 2000, 3000]

        for apow, relpow, i_val in zip(apows, relpows, i_vals):
            bk.save_beamformers(relpow, apow, i_val)

        assert bk.midtime_samples == i_vals
        assert bk.abs_pows == []
        for exp, got in zip(relpows, bk.rel_pows):
            assert np.array_equal(got, exp)

    def test_calculate_average_relpower_beamformer(self, beamformerkeeper):
        (_, _, _, _, _, bk) = beamformerkeeper

        apows = [
            np.ones((3, 3)) * 10,
            np.ones((3, 3)) * 20,
            np.ones((3, 3)) * 30,
        ]
        relpows = [
            np.ones((3, 3)) * 100,
            np.ones((3, 3)) * 200,
            np.ones((3, 3)) * 300,
        ]
        i_vals = [1000, 2000, 3000]

        expected_average_relpow = np.ones((3, 3)) * 200.

        for apow, relpow, i_val in zip(apows, relpows, i_vals):
            bk.save_beamformers(relpow, apow, i_val)

        bk.calculate_average_relpower_beamformer()

        assert isinstance(bk.average_relpow, np.ndarray)
        assert np.array_equal(bk.average_relpow, expected_average_relpow)

    def test_calculate_average_relpower_beamformer_data_not_saved(self, beamformerkeeper):
        (axis, time, st, mt, et, _) = beamformerkeeper

        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=False, save_relpow=False,
                              starttime=st, midtime=mt, endtime=et)

        apows = [
            np.ones((3, 3)) * 10,
            np.ones((3, 3)) * 20,
            np.ones((3, 3)) * 30,
        ]
        relpows = [
            np.ones((3, 3)) * 100,
            np.ones((3, 3)) * 200,
            np.ones((3, 3)) * 300,
        ]
        i_vals = [1000, 2000, 3000]

        for apow, relpow, i_val in zip(apows, relpows, i_vals):
            bk.save_beamformers(relpow, apow, i_val)

        with pytest.raises(ValueError):
            bk.calculate_average_relpower_beamformer()

        with pytest.raises(ValueError):
            bk.calculate_average_abspower_beamformer()

    def test_calculate_average_abspower_beamformer(self, beamformerkeeper):
        (_, _, _, _, _, bk) = beamformerkeeper

        apows = [
            np.ones((3, 3)) * 10,
            np.ones((3, 3)) * 20,
            np.ones((3, 3)) * 30,
        ]
        relpows = [
            np.ones((3, 3)) * 100,
            np.ones((3, 3)) * 200,
            np.ones((3, 3)) * 300,
        ]
        i_vals = [1000, 2000, 3000]

        expected_average_abs = np.ones((3, 3)) * 20.

        for apow, relpow, i_val in zip(apows, relpows, i_vals):
            bk.save_beamformers(relpow, apow, i_val)

        bk.calculate_average_abspower_beamformer()

        assert isinstance(bk.average_abspow, np.ndarray)
        assert np.array_equal(bk.average_abspow, expected_average_abs)

    @pytest.mark.xfail
    def test_extract_best_maxima_from_average_relpower(self):
        assert False

    @pytest.mark.xfail
    def test_extract_best_maxima_from_average_abspower(self):
        assert False

    @pytest.mark.xfail
    def test_extract_best_maxima_from_all_relpower(self):
        assert False

    @pytest.mark.xfail
    def test_extract_best_maxima_from_all_abspower(self):
        assert False

    def test_beamforming_keeping_intended_usage(self, beamformerkeeper):
        (_, _, _, _, _, bk) = beamformerkeeper

        apows = [
            np.ones((3, 3)) * 10,
            np.ones((3, 3)) * 20,
            np.ones((3, 3)) * 30,
        ]
        relpows = [
            np.ones((3, 3)) * 100,
            np.ones((3, 3)) * 200,
            np.ones((3, 3)) * 300,
        ]
        i_vals = [1000, 2000, 3000]

        expected_average_abs = np.ones((3, 3)) * 20.
        expected_average_rel = np.ones((3, 3)) * 200.

        def method_to_write_to_bk_from_within(apows, relpows, i_vals, save_callable):
            for apow, relpow, i_val in zip(apows, relpows, i_vals):
                save_callable(relpow, apow, i_val)
            return

        method_to_write_to_bk_from_within(apows, relpows, i_vals, bk.save_beamformers)

        assert len(relpows) == len(bk.rel_pows)
        assert len(apows) == len(bk.abs_pows)

        assert np.array_equal(relpows[0], bk.rel_pows[0])
        assert np.array_equal(relpows[1], bk.rel_pows[1])
        assert np.array_equal(relpows[2], bk.rel_pows[2])
        assert np.array_equal(apows[0], bk.abs_pows[0])
        assert np.array_equal(apows[1], bk.abs_pows[1])
        assert np.array_equal(apows[2], bk.abs_pows[2])

        bk.calculate_average_abspower_beamformer()
        bk.calculate_average_relpower_beamformer()

        assert isinstance(bk.average_abspow, np.ndarray)
        assert np.array_equal(bk.average_abspow, expected_average_abs)

        assert isinstance(bk.average_relpow, np.ndarray)
        assert np.array_equal(bk.average_relpow, expected_average_rel)

    @pytest.mark.xfail
    def test_save_beamformers(self):
        assert False


@pytest.mark.xfail
def test__validate_if_all_beamforming_params_use_same_qcone():
    assert False


@pytest.mark.xfail
def test__validate_if_all_beamforming_params_use_same_qcone_raising():
    assert False


@pytest.mark.xfail
def test__validate_if_all_beamforming_params_use_same_component_codes():
    assert False


@pytest.mark.xfail
def test__validate_if_all_beamforming_params_use_same_component_codes_raising():
    assert False


@pytest.mark.xfail
def test__extract_most_significant_subbeams():
    assert False


@pytest.mark.xfail
def test_select_local_maxima():
    assert False


@pytest.mark.xfail
def test__calculate_slowness():
    assert False


@pytest.mark.xfail
def test__calculate_slowness_raise_when_not_expected_columns():
    assert False


@pytest.mark.xfail
def test__calculate_azimuth_backazimuth():
    assert False


@pytest.mark.xfail
def test__calculate_azimuth_backazimuth_raise_when_not_expected_columns():
    assert False
