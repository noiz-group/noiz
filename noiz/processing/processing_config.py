from noiz.database import db
from noiz.models import ProcessingConfig


def upsert_default_config():

    default_config = ProcessingConfig(
            use_winter_time=False,
            f_sampling_out=25,
            downsample_filt_order=3,
            window_length_spectrum_sec=100,
            window_spectrum_overlap=0.1,
            window_spectrum_reject_method=None,
            window_spectrum_crit_std=3,
            window_spectrum_crit_quantile=0.1,
            window_spectrum_num_for_stat=10,
            sequence_length_sec=1800,
            sequence_overlap=0,
            sequence_reject_method=None,
            sequence_crit_std=0,
            sequence_crit_quantile=0,
            sequence_num_for_stat=10,
            sequence_reject_tolerance_on_window_spectrum=5,
            f_min_reject_crit=0.01,
            f_max_reject_crit=12,
            filter_type='butterworth',
            taper_time_type='cosine',
            taper_time_width_periods=200,
            taper_time_width_min_samples=25,
            taper_time_width_max_proportion=0.1,
            taper_freq_type='cosine',
            taper_freq_width_proportion=0.1,
            taper_freq_width_min_samples=100,
            taper_freq_width_max_freqs=25,
        )

    current_config = db.session.query(ProcessingConfig).first()

    if  current_config is not None:
        db.session.delete(current_config)

    db.session.add(default_config)
    db.session.commit()

    return