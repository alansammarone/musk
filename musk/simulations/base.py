import os

from ..core import Base


class Simulation(Base):
    LATTICE_NAME_LENGTH = 8
    lattices = []

    @classmethod
    def load(cls, name, storage_path):
        meta_base_path = cls._build_instance_storage_path(storage_path, name)
        meta_file_path = cls._get_meta_file_path(meta_base_path)
        meta_class = self.get_meta_class()
        meta = meta_class.load(meta_file_path)
        return cls(meta, new=False)

    def __init__(self, meta, new=True):

        self.meta = meta
        self._new = new

    def _get_lattice_storage_path(self):
        return os.path.join(self._get_instance_storage_path(), Base.LATTICES_FOLDER)

    def _load_lattices(self):
        lattices = []
        for lattice_name in self.meta.lattice_names:
            lattice_class = self.get_lattice_class()

            print(self._should_save())
            lattice = lattice_class.load(lattice_name, self._get_lattice_storage_path())
            lattices.append(lattice)
        return lattices

    def _initialize_lattices(self):
        if self._is_new():
            lattices = self._build_lattices()
        else:
            lattices = self._load_lattices()

        return lattices

    def get_lattices(self):
        if not self.lattices:
            self.lattices = self._initialize_lattices()

        return self.lattices
