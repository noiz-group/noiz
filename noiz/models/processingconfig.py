from noiz.database import db

Column = db.Column
Boolean = db.Boolean
Float = db.Float
Integer = db.Integer
String = db.String


class ProcessingConfig(db.Model):
    __tablename__ = "processingconfig"
    id = Column('id', Integer, primary_key = True)
    use_winter_time = Column('use_winter_time', Boolean)
    f_sampling_out = Column('f_sampling_out', Integer)
    downsample_filt_order = Column('downsample_filt_order', Integer)
    # default value  chosen so that the window length corresponds to 2^n samples for sampling frequencies Fs=25*2^{m} with m any positive integer
    window_length_spectrum_sec = Column('window_length_spectrum_sec', Float)
    window_spectrum_overlap = Column('window_spectrum_overlap', Float)
    # Method for rejecting the noisiest time windows for spectral estimation. The criterion is defined on the energy of the windows within the frequency band specified by f_min_reject_crit and f_max_reject_crit. The window population on which the criterion is applied is defined by window_spectrum_num_for_stats.
    # possible values:
    # - 'mean-std' : reject windows with energy above mean+(this)*std over the population
    # - 'quantile': reject windows with energy above quantile(this) over the population
    # - None
    window_spectrum_reject_method = Column('window_spectrum_reject_method', String(50))
    # std reject criterion
    # used if window_spectrum_reject_method=mean-std
    window_spectrum_crit_std = Column('window_spectrum_crit_std', Float)
    # quantile reject criterion
    # used if window_spectrum_reject_method=quantile
    window_spectrum_crit_quantile = Column('window_spectrum_crit_quantile', Float)
    # number of adjacent spectral windows to take for computing statistics and defining reject criteria
    window_spectrum_num_for_stat = Column('window_spectrum_num_for_stat', Integer)
    # value in seconds set so that
    # sequence=25*window_spectrum
    sequence_length_sec = Column('sequence_length_sec', Float)
    sequence_overlap = Column('sequence_overlap', Float)
    # Method for rejecting the noisiest sequencies. The criterion is defined on the energy of the sequencies within the frequency band specified by f_min_reject_crit and f_max_reject_crit. The sequence population on which the criterion is applied is defined by sequence_num_for_stats.
    # possible values:
    # - 'mean-std' : reject sequences with energy above mean+(this)*std over the population
    # - 'quantile': reject reject sequences with energy above quantile(this) over the population
    sequence_reject_method = Column('sequence_reject_method', String(50))
    # analog to window_spectrum_crit_std for sequences
    sequence_crit_std = Column('sequence_crit_std', Float)
    # analog to window_spectrum_crit_quantile for sequences
    sequence_crit_quantile = Column('sequence_crit_quantile', Float)
    # number of adjacent sequencies to take for computing statistics and defining reject criteria
    sequence_num_for_stat = Column('sequence_num_for_stat', Integer)
    # which proportion of the spectral estimation windows within a sequence are allowed to be rejected ? (if more that this, the whole sequence is rejected)
    sequence_reject_tolerance_on_window_spectrum = Column('sequence_reject_tolerance_on_window_spectrum', Float)
    f_min_reject_crit = Column('f_min_reject_crit', Float)
    f_max_reject_crit = Column('f_max_reject_crit', Float)
    # filter to be used for all filtering operations when time domain output needed
    # 'fourier_cosine_taper' or 'butterworth'
    filter_type = Column('filter_type', String(50))
    # taper to be used in time domain before fft
    # 'cosine'
    # 'hanning'
    #
    taper_time_type = Column('taper_time_type', String(50))
    # width of the  transition region in cosine taper
    # relative to the maximum period of processing
    taper_time_width_periods = Column('taper_time_width_periods', Float)
    # minimum number of time samples in the transition region of cosine taper
    taper_time_width_min_samples = Column('taper_time_width_min_samples', Integer)
    # maximum length of the transition region in the cosine taper as ratio over the whole length of the processed time series
    taper_time_width_max_proportion = Column('taper_time_width_max_proportion', Float)
    # taper to be used in freq domain before ifft
    # 'cosine'
    # 'hanning'
    taper_freq_type = Column('taper_freq_type', String(50))
    # width of the transition region in cosine taper
    # relative to the width of the queried frequency band
    taper_freq_width_proportion = Column('taper_freq_width_proportion', Float)
    # analog to taper_time_width_min_samples
    taper_freq_width_min_samples = Column('taper_freq_width_min_samples', Integer)
    # maximum width of the  transition region in cosine taper
    # relative to the minimum frequency of processing
    taper_freq_width_max_freqs = Column('taper_freq_width_max_freqs', Float)