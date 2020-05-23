# import os
# from unittest import (
#     mock,
#     TestCase,
# )
# from musk import Experiment, Simulation


# class DummySimulation(Simulation):
#     def run(self, param1, param2):
#         return {"observable1": [1]}


# class DummyExperiment(Experiment):
#     def analyze(self, groups):
#         pass


# class TestExperiment(TestCase):
#     @classmethod
#     def setUp(cls):
#         cls.parameter_range = {"param1": [1], "param2": [1]}
#         cls.storage_folder = "data/test"

#     def test_experiment_reloads_simulations(self):

#         experiment = DummyExperiment(
#             DummySimulation,
#             parameter_range=self.parameter_range,
#             storage_folder=self.storage_folder,
#             delete_artifacts=False,
#         )
#         experiment.run_simulations()

#         # Create new experiment
#         experiment = DummyExperiment(
#             DummySimulation,
#             parameter_range=self.parameter_range,
#             storage_folder=self.storage_folder,
#             delete_artifacts=False,
#         )

#         expected = [
#             (
#                 {"param2": 1, "param1": 1},
#                 [
#                     {
#                         "observables": {"observable1": [1]},
#                         "parameters": {"param1": 1, "param2": 1},
#                     }
#                 ],
#             )
#         ]
#         compare_groups = lambda actual: self.assertEqual(expected, list(actual))

#         experiment.analyze = compare_groups
#         experiment.run_analysis()

#     def test_experiment_removes_files(self):

#         experiment = DummyExperiment(
#             DummySimulation,
#             parameter_range=self.parameter_range,
#             storage_folder=self.storage_folder,
#             delete_artifacts=True,
#         )
#         experiment.run_simulations()
#         experiment.run_analysis()

#         self.assertEqual(os.listdir(self.storage_folder), [])

#     def test_experiment_reuses_names(self):

#         experiment = DummyExperiment(
#             DummySimulation,
#             parameter_range=self.parameter_range,
#             storage_folder=self.storage_folder,
#             delete_artifacts=False,
#         )

#         simulation_names = [
#             simulation.get_name() for simulation in experiment._get_simulations()
#         ]
#         experiment.run_simulations()
#         experiment.run_analysis()

#         experiment = DummyExperiment(
#             DummySimulation,
#             parameter_range=self.parameter_range,
#             storage_folder=self.storage_folder,
#         )

#         new_simulation_names = [
#             simulation.get_name() for simulation in experiment._get_simulations()
#         ]
#         self.assertEqual(simulation_names, new_simulation_names)

#     @classmethod
#     def tearDown(cls):

#         experiment = DummyExperiment(
#             DummySimulation,
#             parameter_range=cls.parameter_range,
#             storage_folder=cls.storage_folder,
#         )

#         try:
#             experiment.do_delete_arficats()
#         except:
#             pass
