from queue import PriorityQueue
from time import time

from graphframes import *
import pandas as pd
from networkx import Graph, connected_components
from pyspark import SparkContext, RDD
from tqdm.autonotebook import tqdm
from ordered_set import OrderedSet

from pyjedai.datamodel import Data, PYJEDAIFeature
from pyjedai.evaluation import Evaluation
from pyjedai.utils import are_matching
from pyjedai.clustering import AbstractClustering, ConnectedComponentsClustering


class ConnectedComponentsClusteringSpark(AbstractClustering):
    """Creates the connected components of the graph. \
        Applied to graph created from entity matching. \
        Input graph consists of the entity ids (nodes) and the similarity scores (edges).
    """

    _method_name: str = "Connected Components Clustering Spark"
    _method_short_name: str = "CCC"
    _method_info: str = "Gets equivalence clusters from the " + \
                        "transitive closure of the similarity graph."

    def __init__(self, spark: SparkContext) -> None:
        super().__init__()
        self.similarity_threshold: float
        self.sc: SparkContext = spark

    def process(self, graph: Graph, data: Data, similarity_threshold: float = None) -> list:
        """NetworkX Connected Components Algorithm in the produced graph.

        Args:
            graph (Graph): Consists of the entity ids (nodes) and the similarity scores (edges).

        Returns:
            list: list of clusters
        """

        def iterate_map_rdd(rdd):
            return rdd.union(rdd.map(lambda x: (x[1], x[0])))

        def count_nb_new_pair(x):
            # global nb_new_pair
            k, values = x
            min_value, value_list = k, []
            for v in values:
                if v < min_value:
                    min_value = v
                value_list.append(v)
            if min_value < k:
                yield (k, min_value)
                for v in value_list:
                    if min_value != v:
                        nb_new_pair.add(1)
                        yield (v, min_value)

        def iterate_reduce_rdd(rdd):
            return rdd.groupByKey().flatMap(count_nb_new_pair).sortByKey()

        start_time = time()
        self.data = data
        self.similarity_threshold: float = similarity_threshold
        # graph_copy = GraphFrame.fromGraphX(graph)
        # graph_copy = Graph()
        similarity_threshold = self.similarity_threshold

        rdd_graph = self.sc.parallelize(graph.edges(data=True))
        nb_new_pair = self.sc.accumulator(0)

        if self.similarity_threshold is not None:
            rdd_graph = rdd_graph.filter(lambda x: x[2]['weight'] >= similarity_threshold)

        rdd = rdd_graph.map(lambda x: (x[0], x[1]))

        # rdd_graph_union = rdd_graph_only_edges.union(rdd_graph_only_edges.map(lambda x: (x[1], x[0])))

        #
        # rdd_reduce_graph = rdd_graph_union.groupByKey() \
        #     .flatMap(count_nb_new_pair).sortByKey()

        nb_iteration = 0

        while True:
            nb_iteration += 1
            start_pair = nb_new_pair.value
            # rdd.repartition(2)
            rdd = iterate_map_rdd(rdd)
            rdd = iterate_reduce_rdd(rdd)
            rdd = rdd.distinct()
            print(f"Number of new pairs for iteration #{nb_iteration}:\t{nb_new_pair.value}")
            if start_pair == nb_new_pair.value:
                rdd.cache()
                break

        def fix_my_ret(x):
            key, my_set = x
            to_ret_set = my_set.copy()
            to_ret_set = my_set
            to_ret_set.add(key)
            return to_ret_set

        clusters = rdd.map(lambda x: (x[1], x[0])).groupByKey().mapValues(set)
        if not data.is_dirty_er:
            resulting_clusters = clusters.filter(lambda x: len(x[1]) == 1).map(
                lambda x: {x[0], list(x[1])[0]}).collect()
        else:
            resulting_clusters = clusters.map(fix_my_ret).collect()
        # print(clusters)
        # print("Number of clusters: ", len(clusters))
        # resulting_clusters = list(filter(lambda x: len(x) == 2, clusters)) \
        #     if not data.is_dirty_er else clusters
        # print("Number of clusters after filtering: ", len(resulting_clusters))
        self.execution_time = time() - start_time
        return resulting_clusters

    def _configuration(self) -> dict:
        return {}


class UniqueMappingClusteringSpark(AbstractClustering):
    """Prunes all edges with a weight lower than t, sorts the remaining ones in
        decreasing weight/similarity and iteratively forms a partition for
        the top-weighted pair as long as none of its entities has already
        been matched to some other.
    """

    _method_name: str = "Unique Mapping Clustering Spark"
    _method_short_name: str = "UMC"
    _method_info: str = "Prunes all edges with a weight lower than t, sorts the remaining ones in" + \
                        "decreasing weight/similarity and iteratively forms a partition for" + \
                        "the top-weighted pair as long as none of its entities has already" + \
                        "been matched to some other."

    def __init__(self, spark: SparkContext) -> None:
        """Unique Mapping Clustering Constructor

        Args:
            similarity_threshold (float, optional): Prunes all edges with a weight
                lower than this. Defaults to 0.1.
            data (Data): Dataset module.
        """
        super().__init__()
        self.similarity_threshold: float
        self.sc: SparkContext = spark

    def process(self, graph: Graph, data: Data, similarity_threshold: float = 0.1) -> list:
        """NetworkX Connected Components Algorithm in the produced graph.

        Args:
            graph (Graph): Consists of the entity ids (nodes) and the similarity scores (edges).

        Returns:
            list: list of clusters
        """
        if data.is_dirty_er:
            raise AttributeError("Unique Mapping Clustering can only be performed in Clean-Clean Entity Resolution.")
        self.similarity_threshold: float = similarity_threshold

        start_time = time()
        self.matched_entities = OrderedSet()
        self.data = data
        maxsize = graph.number_of_edges() * 2

        similarity_threshold = self.similarity_threshold
        rdd_graph = self.sc.parallelize(graph.edges(data=True))
        rdd_graph_filtered = rdd_graph.filter(lambda x: x[2]['weight'] > similarity_threshold)
        rdd_priority_queue = rdd_graph_filtered.map(lambda x: (1 - x[2]['weight'], (x[0], x[1]))).sortBy(lambda x:(x[0],x[1][0], x[1][1])).map(
            lambda x: (x[0], x[1][0], x[1][1]))

        my_priority_queue = rdd_priority_queue.take(maxsize)
        print(len(my_priority_queue))

        self.new_graph = Graph()

        flag = False
        for edge in my_priority_queue:
            sim, entity_1, entity_2 = edge
            if entity_1 in self.matched_entities or entity_2 in self.matched_entities:
                continue
            if entity_1 == 1160: 
                print("2: ", entity_2, sim)
            elif entity_2 == 1160:
                print("1: ", entity_1)
            self.new_graph.add_edge(entity_1, entity_2, weight=sim)
            self.matched_entities.add(entity_1)
            self.matched_entities.add(entity_2)
            if flag:
                flag = False

        # new_graph = Graph()
        # priority_queue = PriorityQueue(maxsize=graph.number_of_edges() * 2)
        # for x in graph.edges(data=True):
        #     if x[2]['weight'] > self.similarity_threshold:
        #         priority_queue.put_nowait((1 - x[2]['weight'], x[0], x[1]))
        #
        # while not priority_queue.empty():
        #     sim, entity_1, entity_2 = priority_queue.get()
        # if entity_1 in matched_entities or entity_2 in matched_entities:
        #     continue
        # new_graph.add_edge(entity_1, entity_2, weight=sim)
        # matched_entities.add(entity_1)
        # matched_entities.add(entity_2)

        clusters = ConnectedComponentsClustering().process(self.new_graph, data, similarity_threshold=None)
        self.execution_time = time() - start_time
        return clusters

    def _configuration(self) -> dict:
        return {}
