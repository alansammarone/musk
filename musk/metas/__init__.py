import json

from datetime import datetime
from dataclasses import dataclass, field, asdict
from musk.misc import DateTimeEncoder


@dataclass
class BaseMeta:
    def save(self, relative_file_path):
        absolute_filepath = os.path.join(os.getcwd(), relative_file_path)
        with open(absolute_filepath, "wb") as file:
            file.write(
                json.dumps(asdict(self), indent=4, cls=DateTimeEncoder).encode("utf8")
            )

    @classmethod
    def load(cls, relative_file_path):
        absolute_filepath = os.path.join(os.getcwd(), relative_file_path)
        # TODO - load datetimes as datetimes
        with open(absolute_filepath, "rb") as file:
            attr_dict = json.loads(file.read())

        return cls(**attr_dict)


@dataclass
class BaseDefaultsMeta:

    created: datetime = field(default_factory=datetime.utcnow)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    storage_path: str = None


@dataclass
class BaseLatticeMeta(BaseMeta):

    size: int
    name: str


@dataclass
class LatticeMeta(BaseDefaultsMeta, BaseLatticeMeta):
    pass


@dataclass
class BaseSimulationMeta(BaseMeta):

    name: str
    storage_path: str


@dataclass
class BaseSimulationDefaultsMeta(BaseSimulationMeta):
    took_ms: int = None
    lattice_names: list = field(default_factory=lambda: [])


@dataclass
class SimulationMeta(BaseSimulationMeta):
    pass


@dataclass
class BaseSimplePercolationSimulationMeta(SimulationMeta):
    occupation_probability: float
    lattice_size: int


@dataclass
class SimplePercolationSimulationMeta(
    BaseSimulationDefaultsMeta, BaseSimplePercolationSimulationMeta
):
    pass


@dataclass
class BaseExperimentDefaultsMeta(BaseDefaultsMeta):
    took_ms: int = None
    simulation_names: list = field(default_factory=lambda: [])


@dataclass
class BaseExperimentMeta(BaseMeta):

    name: str
    parameter_range: dict
    storage_path: str


@dataclass
class ExperimentMeta(BaseExperimentDefaultsMeta, BaseExperimentMeta):
    pass
