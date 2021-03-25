import numpy as np
import pytest
import pytest_check as check

from noiz.processing.beamforming import BeamformerKeeper


class TestBeamformerKeeper:
    def test_get_midtimes(self):
        axis = np.array([1, 2, 3])
        time = np.array([10, 11, 12])
        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=True, save_relpow=True)

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

    def test_save_beamformers_both(self):
        axis = np.array([1, 2, 3])
        time = np.array([10, 11, 12])
        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=True, save_relpow=True)

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
        assert bk.abs_pows == apows
        assert bk.rel_pows == relpows

    def test_save_beamformers_abspows(self):
        axis = np.array([1, 2, 3])
        time = np.array([10, 11, 12])
        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=True, save_relpow=False)

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
        assert bk.abs_pows == apows
        for exp, got in zip(apows, bk.abs_pows):
            assert np.array_equal(got, exp)
        assert bk.rel_pows == []

    def test_save_beamformers_relpows(self):
        axis = np.array([1, 2, 3])
        time = np.array([10, 11, 12])
        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=False, save_relpow=True)

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
        assert bk.rel_pows == relpows
        for exp, got in zip(relpows, bk.rel_pows):
            assert np.array_equal(got, exp)

    def test_calculate_average_relpower_beamformer(self):
        axis = np.array([1, 2, 3])
        time = np.array([10, 11, 12])
        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=False, save_relpow=True)

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

    def test_calculate_average_relpower_beamformer_data_not_saved(self):
        axis = np.array([1, 2, 3])
        time = np.array([10, 11, 12])
        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=False, save_relpow=False)

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

    def test_calculate_average_abspower_beamformer(self):
        axis = np.array([1, 2, 3])
        time = np.array([10, 11, 12])
        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=True, save_relpow=False)

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

    def test_beamforming_keeping_intended_usage(self):
        axis = np.array([1, 2, 3])
        time = np.array([10, 11, 12])
        bk = BeamformerKeeper(xaxis=axis, yaxis=axis, time_vector=time, save_abspow=True, save_relpow=True)

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
