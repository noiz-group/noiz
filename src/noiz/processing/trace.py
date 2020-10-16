# from noiz.models import Trace, File
# import obspy
# import logging
#
# # from celery.utils.log import get_task_logger
#
# from noiz.database import db
#
#
# logger = logging.getLogger("processing")
#
#
# def scan_file_for_traces(session, file: File):
#     logger.info(f"FIle {file} received by worker")
#
#     st = obspy.read(file.filepath, format=file.filetype)
#
#     found_traces = []
#     for i, tr in enumerate(st):
#         tr_db = Trace(
#             file=file,
#             trace_number=i,
#             starttime=tr.stats.starttime.datetime,
#             endtime=tr.stats.endtime.datetime,
#             sampling_rate=tr.stats.sampling_rate,
#             npts=tr.stats.npts,
#         )
#         logger.info(f"Found {tr_db}")
#         found_traces.append(tr_db)
#
#     logger.info(f"Finished processing stream, returning lins")
#
#     return found_traces
