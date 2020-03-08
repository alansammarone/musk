import os
import logging


class Base:

    META_FILENAME = "meta.json"
    SIMULATIONS_FOLDER = "simulations"
    LATTICES_FOLDER = "lattices"

    logger = logging.getLogger("Base")
    _new = None

    @classmethod
    def _get_meta_file_path(cls, meta_base_path):
        return os.path.join(meta_base_path, Base.META_FILENAME)

    @classmethod
    def _get_meta_filename(cls):
        return Experiment.META_FILENAME

    @classmethod
    def _get_random_string(cls, length):
        return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))

    @classmethod
    def _build_instance_storage_path(cls, storage_path, name):
        return os.path.join(storage_path, name)

    def _get_storage_path(self):
        if not self.meta.storage_path:
            raise ParameterException("No storage path")
        return self.meta.storage_path

    def _get_meta_base_path(self, storage_path, name):
        return self._get_instance_storage_path()

    def _get_name(self):
        return self.meta.name

    def _get_instance_storage_path(self):
        return self._build_instance_storage_path(
            self._get_storage_path(), self._get_name()
        )

    def _save_meta(self):
        meta_file_filepath = self._get_meta_file_path(
            self._get_meta_base_path(self._get_storage_path(), self._get_name())
        )
        self._save_meta_to_file_path(meta_file_filepath)

    def _should_save(self):
        return bool(self._get_storage_path())

    def _create_directory_if_not_exists(self, directory):
        if not os.path.exists(directory):
            self.logger.info(f"Creating directory {directory}")
            os.makedirs(directory)

    def _save_meta_to_file_path(self, file_path):
        self._create_directory_if_not_exists(os.path.dirname(file_path))

        self.meta.save(file_path)

    def _is_new(self):
        return bool(self._new)

    def save(self):
        self._save_meta()
