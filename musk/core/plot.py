import matplotlib.pyplot as plt
import numpy as np
import seaborn
import string
from scipy.optimize import curve_fit


class Plot:
    FIGURE_QUALITY_MAP = {
        1: dict(figsize=(6, 4), dpi=120),
        2: dict(figsize=(12, 9), dpi=150),
        3: dict(figsize=(16, 9), dpi=250),
    }

    def __init__(self, parameters={}):
        self.parameters = parameters
        self._setup_style()
        self._setup_figure()

    def _setup_style(self):
        seaborn.set()
        plt.rc("text", usetex=True)
        plt.rc("font", family="serif")
        plt.rc("xtick", labelsize="x-small")
        plt.rc("ytick", labelsize="x-small")

    def _setup_figure(self):
        figure_parameters = self._get_figure_parameters()
        plt.figure(**figure_parameters)

    def _plot_observations(self, X, Y, label):
        label_observation = f"{label} (Observation)"
        plt.scatter(X, Y, label=label)

    def _get_fit_label(self, base_label, fit_parameters):
        string_reprs = []
        for index, value in enumerate(fit_parameters):
            letter = string.ascii_lowercase[index]
            value_repr = f"{value:.5f}"
            string_reprs.append(f"{letter}={value_repr}")
        string_repr = ", ".join(string_reprs)
        label = f"{base_label} (Fit - {string_repr})"
        return label

    def _plot_fit(self, X, Y, fit_fn, label):
        parameters, _ = curve_fit(fit_fn, X, Y)
        label = self._get_fit_label(label, parameters)
        X_fit = np.linspace(min(X), max(X), 100)
        plt.plot(X_fit, fit_fn(X_fit, *parameters), "--", label=label)

    def _add_labels(self, xlabel, ylabel):
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)

    def _add_legend(self):
        plt.legend()

    def _add_title(self, title):
        plt.title(title.format(**self.parameters))

    def _save_to_file(self, filename):
        plt.savefig(filename)

    def _get_figure_parameters(self):
        if self.figure_quality in self.FIGURE_QUALITY_MAP:
            return self.FIGURE_QUALITY_MAP[self.figure_quality]
        raise ValueError

    def plot(self, X, Y, label, fit_fn=None):
        self._plot_observations(X, Y, label)
        if fit_fn:
            self._plot_fit(X, Y, fit_fn, label)

    def save(self):

        self._add_labels(self.xlabel, self.ylabel)
        self._add_title(self.title)
        self._add_legend()
        self._save_to_file(self.filename)
