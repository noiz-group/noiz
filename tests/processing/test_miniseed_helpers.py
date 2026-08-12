from pathlib import Path

from noiz.processing import miniseed_helpers


def test_read_single_miniseed_uses_direct_reader_for_mseed(monkeypatch, tmp_path):
    filename = tmp_path / "test.mseed"
    filename.write_bytes(b"dummy")

    called = {}

    def fake_read_mseed(path):
        called["path"] = path
        return "stream"

    def fail_obspy_read(*args, **kwargs):
        raise AssertionError("obspy.read should not be used for explicit MSEED reads")

    monkeypatch.setattr(miniseed_helpers, "_obspy_read_mseed", fake_read_mseed)
    monkeypatch.setattr(miniseed_helpers.obspy, "read", fail_obspy_read)

    result = miniseed_helpers._read_single_miniseed(filename=filename, format="MSEED")

    assert result == "stream"
    assert called == {"path": str(filename)}


def test_read_single_miniseed_uses_obspy_for_other_formats(monkeypatch, tmp_path):
    filename = tmp_path / "test.other"
    filename.write_bytes(b"dummy")

    called = {}

    def fake_obspy_read(path, format):
        called["args"] = (path, format)
        return "stream"

    monkeypatch.setattr(miniseed_helpers.obspy, "read", fake_obspy_read)

    result = miniseed_helpers._read_single_miniseed(filename=filename, format="SAC")

    assert result == "stream"
    assert called == {"args": (str(filename), "SAC")}
