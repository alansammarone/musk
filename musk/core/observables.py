class Observables:

    """ Group together common, stateless operations such as average cluster size, 
        percolation probability and correlation length 
    """

    # @staticmethod
    # def get_group_percolation_probability(group):
    #     simulation_count = 0
    #     percolation_probability = 0
    #     for result in group.simulations_results:
    #         observables = result.observables
    #         percolation_probability += observables["has_percolated"]
    #         simulation_count += 1

    #     return percolation_probability / simulation_count

    @staticmethod
    def get_observable_group_average(group, observable):
        """ Compute the average value of a given observable over the whole group 
        """

        count = 0
        sum_ = 0
        for result in group.simulations_results:
            observable_value = result.observables.get(observable)
            sum_ += observable_value
            count += 1
        return sum_ / count
