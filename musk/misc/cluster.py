class Cluster:

    """ This class contains various utilities related to clusters, 
        such as calcultions of average cluster size and
        whether a particular cluster percolates 
    """

    @staticmethod
    def has_percolated(cluster, boundaries):
        """ 
            Given a cluster and a list of boundaries, 
            check whether there is at least one cluster 
            which contains nodes in more than one boundary 
        """

        if cluster.intersects(*boundaries):
            return True

        #     for boundary in boundaries:
        #         if :  # Intersection
        #             n_boundaries_in_cluster += 1

        #         if n_boundaries_in_cluster >= 2:
        #             return True

        # return True
