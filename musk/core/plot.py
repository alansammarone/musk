import matplotlib.pyplot as plt
import seaborn
import string
from scipy.optimize import curve_fit


class Plot:
    FIGURE_QUALITY_MAP = {
        1: dict(figsize=(6, 4), dpi=120),
        2: dict(figsize=(12, 9), dpi=150),
        3: dict(figsize=(16, 9), dpi=250),
    }

    def __init__(self, X, Y, parameters, fit_fn=None):
        self.X = X
        self.Y = Y
        self.parameters = parameters
        self.fit_fn = fit_fn
        self._setup_style()

    def _setup_style(self):
        seaborn.set()
        plt.rc("text", usetex=True)
        plt.rc("font", family="serif")
        plt.rc("xtick", labelsize="x-small")
        plt.rc("ytick", labelsize="x-small")

    def _plot_observations(self):
        plt.scatter(self.X, self.Y, label="Observation")

    def _get_fit_label(self, fit_parameters):
        string_reprs = []
        for index, value in enumerate(fit_parameters):
            letter = string.ascii_lowercase[index]
            value_repr = f"{value:.3f}"
            string_reprs.append(f"{letter}={value_repr}")
        string_repr = ", ".join(string_reprs)
        label = f"Fit: {string_repr}"
        return label

    def _plot_fit(self, fit_fn):
        parameters, _ = curve_fit(fit_fn, self.X, self.Y)
        label = self._get_fit_label(parameters)
        plt.plot(self.X, self.fit_fn(self.X, *parameters), "--", label=label)

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

        return self.figure_quality

    def save(self):

        plt.figure(**self._get_figure_parameters())
        self._plot_observations()
        if self.fit_fn:
            self._plot_fit(self.fit_fn)

        self._add_labels(self.xlabel, self.ylabel)
        self._add_title(self.title)
        self._add_legend()
        self._save_to_file(self.filename)
