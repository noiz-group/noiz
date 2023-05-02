# SPDX-License-Identifier: CECILL-B
# Copyright © 2015-2019 EOST UNISTRA, Storengy SAS, Damian Kula
# Copyright © 2019-2023 Contributors to the Noiz project.

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from pandas.plotting import register_matplotlib_converters

register_matplotlib_converters()


def plot_voltage_temp_raw_soh(df, station):
    fig, ax0 = plt.subplots(figsize=(10, 2), dpi=150)
    ax1 = ax0.twinx()

    ax1.plot(
        df.loc[:, "Temperature(C)"],
        "-",
        C="C1",
        label="Temperature($^\circ$C)",
        alpha=0.5,
    )
    ax0.plot(df.loc[:, "Supply voltage(V)"], ".", C="C0", label="Supply voltage(V)")

    ax0.spines["right"].set_color("C0")
    ax0.yaxis.label.set_color("C0")
    ax0.tick_params(axis="y", colors="C0")

    ax1.spines["right"].set_color("C1")
    ax1.yaxis.label.set_color("C1")
    ax1.tick_params(axis="y", colors="C1")

    ax0.set_ylim((12, 17))
    ax1.set_ylim((-10, 60))

    ax0.set_ylabel("Supply Voltage (V)")
    ax1.set_ylabel("Temperature($^\circ$C)")

    starttime = df.index[0].strftime("%Y.%m.%d %H:%M:%S")
    endtime = df.index[-1].strftime("%Y.%m.%d %H:%M:%S")

    ax0.set_title(
        f"Supply voltage and temperature\n {station} from {starttime} to {endtime}"
    )

    return fig, (ax0, ax1)


def plot_voltage_temp(df, station):
    fig, ax0 = plt.subplots(figsize=(10, 2), dpi=150)
    ax1 = ax0.twinx()

    ax1.plot(
        df.loc[:, "temperature"], "-", C="C1", label="Temperature($^\circ$C)", alpha=0.5
    )
    ax0.plot(df.loc[:, "voltage"], ".", C="C0", label="Supply voltage(V)")

    ax0.spines["right"].set_color("C0")
    ax0.yaxis.label.set_color("C0")
    ax0.tick_params(axis="y", colors="C0")

    ax1.spines["right"].set_color("C1")
    ax1.yaxis.label.set_color("C1")
    ax1.tick_params(axis="y", colors="C1")

    ax0.set_ylim((12, 17))
    ax1.set_ylim((-10, 60))

    ax0.set_ylabel("Supply Voltage (V)")
    ax1.set_ylabel("Temperature($^\circ$C)")

    starttime = df.index[0].strftime("%Y.%m.%d %H:%M:%S")
    endtime = df.index[-1].strftime("%Y.%m.%d %H:%M:%S")

    ax0.set_title(
        f"Supply voltage and temperature\n {station} from {starttime} to {endtime}"
    )

    return fig, (ax0, ax1)


def plot_gpstime(df, station):
    fig, ax0 = plt.subplots(figsize=(15, 2), dpi=150)
    ax1 = ax0.twinx()

    substitutions = {"GPS antenna shorted": 0, "GPS init": 1, "Time OK": 2}

    ax0.plot(
        df.loc[:, "Timing status"].map(substitutions),
        "+",
        C="C0",
        label="Timing status",
    )
    ax1.plot(
        df.loc[:, "Time uncertainty(ms)"],
        ".",
        C="C2",
        label="Time uncertainty(ms)",
        alpha=0.5,
    )
    ax1.plot(df.loc[:, "Time error(ms)"], ".-", C="C1", label="Time error(ms)")

    fig.legend(loc=8, ncol=5, bbox_to_anchor=(0.3, 1.15))

    ax0.spines["right"].set_color("C0")
    ax0.yaxis.label.set_color("C0")
    ax0.tick_params(axis="y", colors="C0")

    ax1.spines["right"].set_color("C1")
    ax1.yaxis.label.set_color("C1")
    ax1.tick_params(axis="y", colors="C1")

    ax0.set_yticks([0, 1, 2])
    ax0.set_yticklabels(["GPS antenna\n shorted", "GPS init", "Time OK"])

    ax0.set_ylabel("Timing Status")
    ax1.set_ylabel("Timing Error(ns)")

    ylim_max = max(np.abs(ax1.get_ylim()))
    ax1.set_ylim(-ylim_max, ylim_max)

    starttime = df.index[0].strftime("%Y.%m.%d %H:%M:%S")
    endtime = df.index[-1].strftime("%Y.%m.%d %H:%M:%S")

    ax0.set_title(
        f"Timing status, error and uncertainty\n"
        f"{station} from {starttime} to {endtime}"
    )

    return fig, (ax0, ax1)
