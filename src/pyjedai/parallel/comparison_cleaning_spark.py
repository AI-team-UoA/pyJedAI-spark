import sys
import warnings
import pandas as pd
from itertools import chain
from collections import defaultdict
from logging import warning
from math import log10, sqrt
from queue import PriorityQueue
from time import time
from sys import getsizeof

import numpy as np
import math

from pyspark import SparkContext, RDD
from tqdm.autonotebook import tqdm

from pyjedai.evaluation import Evaluation

from pyjedai.datamodel import Data, PYJEDAIFeature
from pyjedai.utils import chi_square, get_sorted_blocks_shuffled_entities, PositionIndex, \
    canonical_swap, sorted_enumerate

from pyjedai.parallel.utils_spark import create_entity_index
from abc import ABC, abstractmethod
from typing import Tuple, List

from abc import ABC, abstractmethod
from pyjedai.comparison_cleaning import AbstractComparisonCleaning, AbstractMetablocking


class AbstractComparisonCleaningSpark(AbstractComparisonCleaning):

    def __init__(self, spark: SparkContext, num_slices: int = 10) -> None:
        super().__init__()
        self.sc = spark
        if num_slices < 10:
            num_slices = 10

        self.num_slices = num_slices

    def process(self,
                blocks: dict,
                data: Data,
                tqdm_disable: bool = False,
                store_weights: bool = False
                ) -> dict:
        start_time = time()
        self.tqdm_disable, self.data, self.store_weights = tqdm_disable, data, store_weights
        self._limit = self.data.num_of_entities \
            if self.data.is_dirty_er or self._node_centric else self.data.dataset_limit
        self._progress_bar = tqdm(
            total=10,
            desc=self._method_name,
            disable=self.tqdm_disable
        )
        num_slices = self.num_slices
        print(num_slices)
        
        
        self._progress_bar.update(1)
        global my_cnt
        global my_thresh
        global _data_is_dirty_er_bc
        global _data_dataset_limit_bc 
        global _entity_index_bc
        global _blocks_bc

        _data_is_dirty_er = self.data.is_dirty_er
        _data_is_dirty_er_bc = self.sc.broadcast(_data_is_dirty_er) 
        
        _data_dataset_limit = self.data.dataset_limit
        _data_dataset_limit_bc = self.sc.broadcast(_data_dataset_limit)

        my_cnt = self.sc.accumulator(0)
        my_thresh = self.sc.accumulator(0)

        self.rdd_blocks = self.sc.parallelize(blocks.items(),numSlices = num_slices)
        self._stored_weights = defaultdict(float) if self.store_weights else None
        is_dirty_er = _data_is_dirty_er
        
        self._rdd_entity_index = create_entity_index(self.rdd_blocks, is_dirty_er)
        _entity_index = self._entity_index = self._rdd_entity_index.collectAsMap()
        
        _entity_index_bc = self.sc.broadcast(_entity_index)
        self._num_of_blocks = len(blocks)
        self._blocks = blocks
        _blocks_bc = self.sc.broadcast(blocks)
        self._blocks = self._apply_main_processing()
        self._progress_bar.update(1)
        self.execution_time = time() - start_time
        self._progress_bar.close()

        return self._blocks

    def get_precalculated_weight(self, rdd: RDD) -> RDD:
        """Returns the precalculated weight for given pair

        Args:
            entity_id (int): Entity ID
            neighbor_id (int): Neighbor ID

        Raises:
            AttributeError: Given pair has no precalculated weigth

        Returns:
            float: Pair weigth
        """
        _stored_weights = self._stored_weights
        _stored_weights_bc = self.sc.broadcast(_stored_weights)
        if (not self.store_weights): raise AttributeError("No precalculated weights.")

        def get_stored_weights(entity):
            e_id, n_id = entity

            def my_canonical_swap(id1, id2):
                if id2 > id1:
                    return (id1, id2)
                else:
                    return (id2, id1)

            return e_id, n_id, _stored_weights_bc.value.get(my_canonical_swap(e_id, n_id),
                                                   KeyError(
                                                       f"Pair [{entity_id},{neighbor_id}] has no precalculated weight"))

        rdd_weight = rdd.map(get_stored_weights)
        return rdd_weight


class AbstractMetablockingSpark(AbstractComparisonCleaningSpark, ABC):
    def __init__(self, spark: SparkContext, num_slices: int = 10) -> None:
        super().__init__(spark, num_slices)
        self._blocks: dict
        self._flags: np.array
        self._counters: np.array
        self._flags: np.array
        self._comparisons_per_entity: np.array
        self._node_centric: bool
        self._threshold: float
        self._distinct_comparisons: int
        self._comparisons_per_entity: np.array
        self._neighbors: set() = set()
        self._retained_neighbors: set() = set()
        self._block_assignments: int = 0
        self.weighting_scheme: str

    def _apply_main_processing(self):
        self._counters = np.empty([self.data.num_of_entities], dtype=float)
        self._flags = np.empty([self.data.num_of_entities], dtype=int)
        
        if (self._comparisons_per_entity_required()):
            self._progress_bar.update(1)
            self._set_statistics()
            
        self._progress_bar.update(1)
        self._set_threshold()
        self._progress_bar.update(1)

        return self._prune_edges()

    def _comparisons_per_entity_required(self):
        return (self.weighting_scheme == 'EJS' or
                self.weighting_scheme == 'CNC' or
                self.weighting_scheme == 'SNC' or
                self.weighting_scheme == 'SND' or
                self.weighting_scheme == 'CND' or
                self.weighting_scheme == 'CNJ' or
                self.weighting_scheme == 'SNJ')

    def _get_weight(self, rdd: RDD) -> RDD:
        ws = self.weighting_scheme
        if (self._comparisons_per_entity_required()):
            _comparisons_per_entity_bc = self._comparisons_per_entity_bc
        if ws == 'CN-CBS' or ws == 'CBS' or ws == 'SN-CBS':
            return rdd
        # CARDINALITY_NORM_COSINE, SIZE_NORM_COSINE
        elif ws == 'CNC' or ws == 'SNC':
            def get_weight(entity):
                e_id, n_id, cnt = entity
                to_ret = cnt / float(
                    sqrt(_comparisons_per_entity_bc.value[e_id] * _comparisons_per_entity_bc.value[n_id]))
                return e_id, n_id, to_ret

            return rdd.map(get_weight)
        # SIZE_NORM_DICE, CARDINALITY_NORM_DICE
        elif ws == 'SND' or ws == 'CND':
            def get_weight(entity):
                e_id, n_id, cnt = entity
                to_ret = 2 * cnt / float(
                    sqrt(_comparisons_per_entity_bc.value[e_id] + _comparisons_per_entity_bc.value[n_id]))
                
                return e_id, n_id, to_ret

            return rdd.map(get_weight)
        # CARDINALITY_NORM_JS, SIZE_NORM_JS
        elif ws == 'CNJ' or ws == 'SNJ':
            def get_weight(entity):
                e_id, n_id, cnt = entity
                to_ret = cnt / float(
                    _comparisons_per_entity_bc.value[e_id] + _comparisons_per_entity_bc.value[n_id] -
                    cnt)
                
                
                return e_id, n_id, to_ret

            return rdd.map(get_weight)
        elif ws == 'COSINE':
            # _entity_index_bc = self._entity_index_bc

            def get_weight(entity):
                global _entity_index_bc
                e_id, n_id, cnt = entity

                to_ret = cnt / float(
                    sqrt(len(_entity_index_bc.value[e_id]) * len(_entity_index_bc.value[n_id])))
                
                
                return e_id, n_id, to_ret

            return rdd.map(get_weight)
        elif ws == 'ECBS':
            # _entity_index_bc = self._entity_index_bc
            _num_of_blocks = self._num_of_blocks

            def get_weight(entity):
                global _entity_index_bc

                e_id, n_id, cnt = entity

                to_ret = float(
                    cnt *
                    log10(float(_num_of_blocks / len(_entity_index_bc.value[e_id]))) *
                    log10(float(_num_of_blocks / len(_entity_index_bc.value[n_id])))
                )
                
                return e_id, n_id, to_ret

            return rdd.map(get_weight)
        elif ws == 'JS':
            # _entity_index_bc = self._entity_index_bc


            def get_weight(entity):

                global _entity_index_bc

                e_id, n_id, cnt = entity

                to_ret = cnt / (len(_entity_index_bc.value[e_id]) + \
                                len(_entity_index_bc.value[n_id]) - cnt)
                
                
                return e_id, n_id, to_ret

            return rdd.map(get_weight)
        elif ws == 'EJS':
            # _entity_index_bc = self._entity_index_bc
            _distinct_comparisons = self._distinct_comparisons
            _comparisons_per_entity_bc = self._comparisons_per_entity_bc

            def get_weight(entity):
                global _entity_index_bc

                e_id, n_id, cnt = entity

                probability = cnt / (len(_entity_index_bc.value[e_id]) + \
                                     len(_entity_index_bc.value[n_id]) - cnt)

                to_ret = float(probability * \
                               log10(_distinct_comparisons / _comparisons_per_entity_bc.value[e_id]) * \
                               log10(_distinct_comparisons / _comparisons_per_entity_bc.value[n_id]))
                
                return e_id, n_id, to_ret

            return rdd.map(get_weight)

        elif ws == 'X2':
            # _entity_index_bc = self._entity_index_bc
            _num_of_blocks = self._num_of_blocks

            def get_weight(entity):
                global _entity_index_bc

                e_id, n_id, cnt = entity
                observed = [int(cnt),
                            int(len(_entity_index_bc.value[e_id]) - cnt)]
                expected = [int(len(_entity_index_bc.value[n_id]) - observed[0]),
                            int(_num_of_blocks - (observed[0] + observed[1] - cnt))]

                to_ret = chi_square(np.array([observed, expected]))
                
                return e_id, n_id, to_ret
            
            return rdd.map(get_weight)
        else:
            raise ValueError("This weighting scheme does not exist")

    # def _get_weight(self, entity_id: int, neighbor_id: int) -> float:
    #     ws = self.weighting_scheme
    #     if ws == 'CN-CBS' or ws == 'CBS' or ws == 'SN-CBS':
    #         return self._counters[neighbor_id]
    #     # CARDINALITY_NORM_COSINE, SIZE_NORM_COSINE
    #     elif ws == 'CNC' or ws == 'SNC':
    #         return self._counters[neighbor_id] / float(
    #             sqrt(self._comparisons_per_entity[entity_id] * self._comparisons_per_entity[neighbor_id]))
    #     # SIZE_NORM_DICE, CARDINALITY_NORM_DICE
    #     elif ws == 'SND' or ws == 'CND':
    #         return 2 * self._counters[neighbor_id] / float(
    #             self._comparisons_per_entity[entity_id] + self._comparisons_per_entity[neighbor_id])
    #     # CARDINALITY_NORM_JS, SIZE_NORM_JS
    #     elif ws == 'CNJ' or ws == 'SNJ':
    #         return self._counters[neighbor_id] / float(
    #             self._comparisons_per_entity[entity_id] + self._comparisons_per_entity[neighbor_id] - self._counters[
    #                 neighbor_id])
    #     elif ws == 'COSINE':
    #         return self._counters[neighbor_id] / float(
    #             sqrt(len(self._entity_index[entity_id]) * len(self._entity_index[neighbor_id])))
    #     elif ws == 'DICE':
    #         return 2 * self._counters[neighbor_id] / float(
    #             len(self._entity_index[entity_id]) + len(self._entity_index[neighbor_id]))
    #     elif ws == 'ECBS':
    #         return float(
    #             self._counters[neighbor_id] *
    #             log10(float(self._num_of_blocks / len(self._entity_index[entity_id]))) *
    #             log10(float(self._num_of_blocks / len(self._entity_index[neighbor_id])))
    #         )
    #     elif ws == 'JS':
    #         return self._counters[neighbor_id] / (len(self._entity_index[entity_id]) + \
    #                                               len(self._entity_index[neighbor_id]) - self._counters[neighbor_id])
    #     elif ws == 'EJS':
    #         probability = self._counters[neighbor_id] / (len(self._entity_index[entity_id]) + \
    #                                                      len(self._entity_index[neighbor_id]) - self._counters[
    #                                                          neighbor_id])
    #         return float(probability * \
    #                      log10(self._distinct_comparisons / self._comparisons_per_entity[entity_id]) * \
    #                      log10(self._distinct_comparisons / self._comparisons_per_entity[neighbor_id]))
    #     elif ws == 'X2':
    #         observed = [int(self._counters[neighbor_id]),
    #                     int(len(self._entity_index[entity_id]) - self._counters[neighbor_id])]
    #         expected = [int(len(self._entity_index[neighbor_id]) - observed[0]),
    #                     int(self._num_of_blocks - (observed[0] + observed[1] - self._counters[neighbor_id]))]
    #         return chi_square(np.array([observed, expected]))
    #     else:
    #         raise ValueError("This weighting scheme does not exist")

    def _normalize_neighbor_entities(self, rdd: RDD) -> RDD:
        # _blocks_bc = self._blocks_bc
        if self.data.is_dirty_er:
            if not self._node_centric:
                def to_token(entity):
                    e_id, block_list = entity
                    for token in block_list:
                        yield (e_id, token)


                rdd = rdd.flatMap(to_token)
                
                def get_neighbors(entity):
                    global _blocks_bc
                    e_id, token = entity
                    for neighbor_id in _blocks_bc.value[token].entities_D1:
                        if neighbor_id < e_id:
                            yield ((e_id, neighbor_id), token)
                
                # def to_dict(entity):
                #     e_id, n_id_with_list = entity
                #     neighbors_dict = defaultdict(list)
                #     for n_id, n_list in n_id_with_list:
                #         neighbors_dict[n_id] = n_list
                #     return e_id, neighbors_dict

                rdd_neighbors = rdd.flatMap(get_neighbors)
                # .groupByKey().mapValues(list).map(lambda x: (x[0][0],x[0][1],x[1]))
                # rdd_to_dict = rdd_neighbors.groupByKey().map(to_dict)

                # def get_neighbors(entity_tuple):
                #     e_id, block_list = entity_tuple
                #     neighbors_dict = defaultdict(list)

                #     for token in block_list:
                #         for neighbor_id in _blocks[token].entities_D1:
                #             if neighbor_id < e_id:
                #                 neighbors_dict[neighbor_id].append(token)

                #     return e_id, neighbors_dict

                return rdd_neighbors
            else:
                def to_token(entity):
                    e_id, block_list = entity
                    for token in block_list:
                        yield (e_id, token)
                    
                rdd = rdd.flatMap(to_token)

                def get_neighbors(entity):
                    global _blocks_bc

                    e_id, token = entity
                    for neighbor_id in _blocks_bc.value[token].entities_D1:
                        if neighbor_id != e_id:
                            yield ((e_id, neighbor_id), token)

                # def to_dict(entity):
                #     e_id, n_id_with_list = entity
                #     neighbors_dict = defaultdict(list)
                #     for n_id, n_list in n_id_with_list:
                #         neighbors_dict[n_id] = n_list
                #     return e_id, neighbors_dict

                rdd_neighbors = rdd.flatMap(get_neighbors)
                # .groupByKey().mapValues(list).map(lambda x: (x[0][0],(x[0][1],x[1])))
                # rdd_to_dict = rdd_neighbors.groupByKey().map(to_dict)



                # # def get_neighbors(entity_tuple):
                # #     e_id, block_list = entity_tuple
                # #     neighbors_dict = defaultdict(list)

                #     for token in block_list:
                #         for neighbor_id in _blocks[token].entities_D1:
                #             if neighbor_id != e_id:
                #                 neighbors_dict[neighbor_id].append(token)

                    # return e_id, neighbors_dict

                return rdd_neighbors
        else:
            # dataset_limit_bc = self._data_dataset_limit_bc


            def to_token(entity):
                    e_id, block_list = entity
                    for token in block_list:
                        yield (e_id, token)

            rdd = rdd.flatMap(to_token)

            def get_neighbors(entity):
                global _blocks_bc

                global _data_dataset_limit_bc
                e_id, token = entity
                
                if e_id < _data_dataset_limit_bc.value:
                    for neighbor_id in _blocks_bc.value[token].entities_D2:
                        yield ((e_id, neighbor_id), token)
                else:
                    for neighbor_id in  _blocks_bc.value[token].entities_D1:
                        yield ((e_id, neighbor_id), token)
    
            # def to_dict(entity):
            #     e_id, n_id_with_list = entity
            #     neighbors_dict = defaultdict(list)
            #     for n_id, n_list in n_id_with_list:
            #         neighbors_dict[n_id] = n_list
            #     return e_id, neighbors_dict

            rdd_neighbors = rdd.flatMap(get_neighbors)
            # .groupByKey().mapValues(list).map(lambda x: (x[0][0],(x[0][1],x[1])))
            # rdd_to_dict = rdd_neighbors.groupByKey().map(to_dict)

            return rdd_neighbors


    # Make it with RDD?
    def _set_statistics(self) -> None:
        self._distinct_comparisons = 0
        # self._comparisons_per_entity = np.empty([self.data.num_of_entities], dtype=float)
        global dist_comp 
        dist_comp = self.sc.accumulator(0)
        
        rdd_get_neighbor_entities = self._get_neighbor_entities()
        # rdd_get_neighbor_entities.cache()
        self._comparisons_per_entity = rdd_get_neighbor_entities.collectAsMap()
        _comparisons_per_entity = self._comparisons_per_entity
        self._comparisons_per_entity_bc = self.sc.broadcast(_comparisons_per_entity)
        self._distinct_comparisons = dist_comp.value
        # print(self._comparisons_per_entity[1474])
        self._distinct_comparisons /= 2

        
        
        # distinct_neighbors = set()
        # for entity_id in range(0, self.data.num_of_entities, 1):
        #     if entity_id in self._entity_index:
        #         associated_blocks = self._entity_index[entity_id]
        #         distinct_neighbors.clear()
        #         # distinct_neighbors = set().union(*[
        #         #     self._get_neighbor_entities(block_id, entity_id) for block_id in associated_blocks
        #         # ])
        #         distinct_neighbors = set(chain.from_iterable(
        #             self._get_neighbor_entities(block_id, entity_id) for block_id in associated_blocks
        #         ))
        #         # for block_id in associated_blocks:
        #         #     distinct_neighbors = set.union(
        #         #         distinct_neighbors,
        #         #         self._get_neighbor_entities(block_id, entity_id)
        #         #     )
        #         self._comparisons_per_entity[entity_id] = len(distinct_neighbors)

        #         if self.data.is_dirty_er:
        #             self._comparisons_per_entity[entity_id] -= 1

        #         self._distinct_comparisons += self._comparisons_per_entity[entity_id]
        # self._distinct_comparisons /= 2


    def _get_neighbor_entities(self) -> RDD:
        # _blocks_bc = self._blocks_bc
        # data_is_dirty_er_bc = self._data_is_dirty_er_bc
        # data_dataset_limit_bc = self._data_dataset_limit_bc

        # def entity_id_with_block(entity):
        #     global _blocks_bc
        #     global _data_is_dirty_er_bc
        #     global _data_dataset_limit_bc
      
        #     entity_id, associated_blocks = entity
      

        #     for block_id in associated_blocks:
        #         if not _data_is_dirty_er_bc.value and entity_id < _data_dataset_limit_bc.value:
        #             yield entity_id, _blocks_bc.value[block_id].entities_D2
        #         else:
        #             yield entity_id, _blocks_bc.value[block_id].entities_D1
      
        # def reduce_neighbor_entities(entity):
        #     global _data_is_dirty_er
        #     global dist_comp
        #     e_id, distinct_neighbors = entity

        #     if data_is_dirty_er_bc.value:
        #         dist_neig = len(distinct_neighbors)-1
        #         dist_comp += dist_neig
        #         return e_id, dist_neig
            
        #     dist_neig = len(distinct_neighbors)-1
        #     dist_comp += dist_neig
        #     return e_id, dist_neig
            
                
    
        def get_neigbor_entities(entity):
            global dist_comp
            global _data_is_dirty_er_bc
            global _data_dataset_limit_bc
            global _blocks_bc


            entity_id, associated_blocks = entity
            # distinct_neighbors = set()
            
            if not _data_is_dirty_er_bc.value and entity_id < _data_dataset_limit_bc.value:
                lists_of_entities = [ _blocks_bc.value[block_id].entities_D2 for block_id in associated_blocks]
            else:
                lists_of_entities = [ _blocks_bc.value[block_id].entities_D1 for block_id in associated_blocks]

            distinct_neighbors = set(itertools.chain(*lists_of_entities))

            # for block_id in associated_blocks: 
            #     if not _data_is_dirty_er_bc.value and entity_id < _data_dataset_limit_bc.value:
            #         distinct_neighbors.update( _blocks_bc.value[block_id].entities_D2)
            #     else:
            #         distinct_neighbors.update( _blocks_bc.value[block_id].entities_D1)
            
            if _data_is_dirty_er_bc.value:
                dist_neig = len(distinct_neighbors)-1
                dist_comp += dist_neig
                return entity_id, dist_neig

            dist_neig = len(distinct_neighbors)
            dist_comp += dist_neig

            return entity_id, dist_neig
        # rdd = self._rdd_entity_index.flatMap(entity_id_with_block).reduceByKey(lambda a,b: a.union(b)).map(reduce_neighbor_entities)
        # rdd = rdd.reduceByKey(reduce_neighbor_entities)

        rdd = self._rdd_entity_index.map(get_neigbor_entities)
        return rdd

    # def _get_neighbor_entities(self, block_id: int, entity_id: int) -> set:
    #     return self._blocks[block_id].entities_D2 \
    #         if (not self.data.is_dirty_er and entity_id < self.data.dataset_limit) else \
    #         self._blocks[block_id].entities_D1

    @abstractmethod
    def _set_threshold(self):
        pass

    @abstractmethod
    def _prune_edges(self) -> dict:
        pass


class WeightedEdgePruningSpark(AbstractMetablockingSpark):
    _method_name = "Weighted Edge Pruning Spark"
    _method_short_name: str = "WEP"
    _method_info = "A Meta-blocking method that retains all comparisons " + \
                   "that have a weight higher than the average edge weight in the blocking graph."

    def __init__(self, spark: SparkContext, num_slices: int = 10, weighting_scheme: str = 'CBS') -> None:
        super().__init__(spark, num_slices)
        self.weighting_scheme = weighting_scheme
        self._node_centric = False
        self._num_of_edges: float

    def _prune_edges(self) -> dict:

        limit = self._limit
        rdd_entity_index = self._rdd_entity_index
        rdd_entity_index_limit = rdd_entity_index.filter(lambda entity: entity[0] < limit)
        rdd_processed_entity = self._process_entity(rdd_entity_index_limit)
        self._verify_valid_entities(rdd_processed_entity)

        return self.blocks

    def _process_entity(self, rdd: RDD) -> RDD:
        

        rdd_neighbors_to_list = self._normalize_neighbor_entities(rdd)
        
        rdd_to_return: RDD

        if self.weighting_scheme == 'CN-CBS' or self.weighting_scheme == 'CNC' or self.weighting_scheme == 'CND' or self.weighting_scheme == 'CNJ':
        
            def counter(entity):
                global _blocks_bc
                global _data_is_dirty_er
                e_id_n_id, b_id = entity
                cnt = 1 / _blocks_bc.value[b_id].get_cardinality(_data_is_dirty_er.value)
                return e_id_n_id, cnt

            rdd_to_return = rdd_neighbors_to_list.map(counter).reduceByKey(lambda a,b: a+b)

        if self.weighting_scheme == 'SN-CBS' or self.weighting_scheme == 'SNC' or self.weighting_scheme == 'SND' or self.weighting_scheme == 'SNJ':

            def counter(entity):
                global _blocks_bc
                e_id_n_id, b_id = entity
                cnt = 1 / _blocks_bc.value[b_id].get_size()
                return e_id_n_id, cnt

            rdd_to_return = rdd_neighbors_to_list.map(counter).reduceByKey(lambda a,b: a+b)

        else:

            def counter(entity):
                e_id_n_id, b_id = entity
                return e_id_n_id, 1

            rdd_to_return = rdd_neighbors_to_list.map(counter).reduceByKey(lambda a,b: a+b)
            
        def faster_count(entity):
            global my_cnt
            my_cnt += 1
            return (entity[0][0],entity[0][1],entity[1])

        return rdd_to_return.map(faster_count)

    def _verify_valid_entities(self, rdd: RDD) -> None:

        _threshold = self._threshold
        rdd_get_weight = self.rdd_weights 

        def check_threshold(entity):
            e_id, n_id, _weight = entity
            return _threshold <= _weight

        rdd_in_threshold = rdd_get_weight.filter(lambda x: check_threshold(x))
        if self.store_weights:
            def my_canonical_swap(entity):
                id1, id2, _weight = entity
                if id2 > id1:
                    return (id1, id2), _weight
                else:
                    return (id2, id1), _weight

            self._stored_weights = rdd_in_threshold.map(my_canonical_swap).collectAsMap()

        rdd_only_pairs = rdd_in_threshold.map(lambda x: (x[0], [x[1]]))
        rdd_blocks = rdd_only_pairs.reduceByKey(lambda a,b: a+b)
        self.blocks = rdd_blocks.collectAsMap()

    def _update_threshold(self, rdd: RDD) -> None:
        
        self._progress_bar.update(1)
        self.rdd_weights = self._get_weight(rdd)

        def set_thresh(entity):
            global my_thresh
            _, _, weight = entity
            my_thresh += weight
        
        self.rdd_weights.foreach(set_thresh)

        self._threshold = my_thresh.value
        self._num_of_edges = my_cnt.value
        self._progress_bar.update(1)

    def _set_threshold(self):
        self._num_of_edges = 0.0
        self._threshold = 0.0
        limit = self._limit

        rdd_entity_index = self._rdd_entity_index
        rdd_entity_index_limit = rdd_entity_index.filter(lambda entity: entity[0] < limit)
        rdd_processed_entity = self._process_entity(rdd_entity_index_limit)
        self._update_threshold(rdd_processed_entity)


        #  ??? Propably it can be deleted  
        rdd = rdd_processed_entity.filter(lambda x: x[0] == 0)
        #  ??? 


        self._threshold /= self._num_of_edges

    def _configuration(self) -> dict:
        return {
            "Node centric": self._node_centric,
            "Weighting scheme": self.weighting_scheme
        }


class WeightedNodePruningSpark(WeightedEdgePruningSpark):
    """A Meta-blocking method that retains for every entity, the comparisons \
        that correspond to edges in the blocking graph that are exceed \
        the average edge weight in the respective node neighborhood.
    """

    _method_name = "Weighted Node Pruning Spark"
    _method_short_name: str = "WNP"
    _method_info = "A Meta-blocking method that retains for every entity, the comparisons \
                    that correspond to edges in the blocking graph that are exceed \
                    the average edge weight in the respective node neighborhood."

    def __init__(self, spark: SparkContext, num_slices: int = 10, weighting_scheme: str = 'CBS') -> None:
        super().__init__(spark, num_slices, weighting_scheme)
        self._average_weight: np.array
        self._node_centric = True

    def _get_valid_weight(self, rdd: RDD) -> RDD:

        # _average_weight = self._average_weight
        rdd_weight = self.rdd_weights

        def valid_weight(entity):
            global _average_weight_bc
            e_id, n_id, weight = entity
            _valid_weight = weight if ((_average_weight_bc.value[e_id] <= weight or \
                                        _average_weight_bc.value[n_id] <= weight) and
                                       e_id < n_id) else 0
            return e_id, n_id, _valid_weight

        rdd_valid_weight = rdd_weight.map(valid_weight)
        return rdd_valid_weight

    def _set_threshold(self):
        self._average_weight = np.zeros(self._limit, dtype=float)

        limit = self._limit

        rdd_entity_index = self._rdd_entity_index
        # rdd_entity_index_limit = rdd_entity_index.filter(lambda entity: entity[0] < limit)
        rdd_processed_entity = self._process_entity(rdd_entity_index)
        self._update_threshold(rdd_processed_entity)
        # for i in range(0, self._limit):
        #     self._process_entity(i)
        #     self._update_threshold(i)

    def _update_threshold(self, rdd: RDD) -> None:

        limit = self._limit
        self.rdd_weights = super()._get_weight(rdd)
        rdd_weights_by_key = self.rdd_weights.map(lambda x: (x[0],[x[2]])).reduceByKey(lambda a,b: a+b)

        def calculate_average_weight(entity):
            e_id, _weight_list = entity
            if e_id > limit: 
                average_weight = 0
            else: 
                average_weight = sum(_weight_list) / len(_weight_list)
            return e_id, average_weight

        rdd_average_weight_fixed = rdd_weights_by_key.map(calculate_average_weight)

        # # Needs improvement probably
        # Which is better probably the for_loop

        # rdd_zeros = self.sc.parallelize(range(limit)).map(lambda x: (x, 0))
        # rdd_average_weight_with_zeros = rdd_average_weight.union(rdd_zeros).reduceByKey(lambda a, b: a + b).sortByKey()
        # rdd_average_weight_fixed = rdd_average_weight_with_zeros.map(lambda x: x[1])
        self._average_weight = rdd_average_weight_fixed.collectAsMap()
        _average_weight = self._average_weight
        global _average_weight_bc
        _average_weight_bc = self.sc.broadcast(_average_weight)
        #
        #
        #
        # average_weight_list = rdd_average_weight.collect()
        #
        # for item in average_weight_list:
        #     index, average_weight = item
        #     self._average_weight[index] = average_weight

    def _verify_valid_entities(self, rdd: RDD) -> None:

        rdd_get_valid_weight = self._get_valid_weight(rdd)
        rdd_in_threshold = rdd_get_valid_weight.filter(lambda x: x[2] != 0)

        if self.store_weights:
            def my_canonical_swap(entity):
                id1, id2, _weight = entity
                if id2 > id1:
                    return (id1, id2), _weight
                else:
                    return (id2, id1), _weight

            self._stored_weights = rdd_in_threshold.map(my_canonical_swap).collectAsMap()

        rdd_only_pairs = rdd_in_threshold.map(lambda x: (x[0], [x[1]]))
        rdd_blocks = rdd_only_pairs.reduceByKey(lambda a, b: a+b)
        self.blocks = rdd_blocks.collectAsMap()


class CardinalityEdgePruningSpark(WeightedEdgePruningSpark):
    """A Meta-blocking method that retains the comparisons \
            that correspond to the top-K weighted edges in the blocking graph.
    """

    _method_name = "Cardinality Edge Pruning Spark"
    _method_short_name: str = "CEP"
    _method_info = "A Meta-blocking method that retains the comparisons " + \
                   "that correspond to the top-K weighted edges in the blocking graph."

    def __init__(self, spark: SparkContext, num_slices: int = 10, weighting_scheme: str = 'JS') -> None:
        super().__init__(spark, num_slices, weighting_scheme)
        self._minimum_weight: float = sys.float_info.min
        self._top_k_edges: RDD

    def _prune_edges(self) -> dict:

        limit = self._limit
        rdd_entity_index = self._rdd_entity_index
        rdd_entity_index_limit = rdd_entity_index.filter(lambda entity: entity[0] < limit)
        rdd_processed_entity = self._process_entity(rdd_entity_index_limit)
        self._verify_valid_entities(rdd_processed_entity)

        if self.store_weights:
            def my_canonical_swap(entity):
                id1, id2, _weight = entity
                if id2 > id1:
                    return (id1, id2), _weight
                else:
                    return (id2, id1), _weight

            self._stored_weights = self._top_k_edges.map(my_canonical_swap).collectAsMap()
        rdd_blocks = self._top_k_edges.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda a,b: a+b)

        self.blocks = rdd_blocks.collectAsMap()

        return self.blocks

    def _set_threshold(self) -> None:

        # _blocks_bc = self._blocks_bc
        
        def summary_block_size(entity):
            global my_thresh
            global _blocks_bc
            token, _ = entity
            my_thresh += _blocks_bc.value[token].get_size()
            
        self.rdd_blocks.foreach(summary_block_size)

        # rdd = self.rdd_blocks.map(summary_block_size)

        block_assignments = my_thresh.value 
        self._threshold = block_assignments / 2

    def _verify_valid_entities(self, rdd: RDD) -> None:
        _minimum_weight = self._minimum_weight
        _threshold = self._threshold

        rdd_get_weight = self._get_weight(rdd).filter(lambda x: x[2] >= _minimum_weight)
            # .map(lambda x: (x[2], (x[0], x[1])))

        rdd_sorted = rdd_get_weight.sortBy(lambda x: (x[2],x[0],x[1]), ascending=False)
        num_slices = self.num_slices
        # self._top_k_edges = self.sc.parallelize(rdd_get_weight.takeOrdered(int(_threshold), key=lambda x: (-x[2],-x[0],-x[1])),numSlices = num_slices)
        rdd_indexed = rdd_sorted.zipWithIndex()
        rdd_in_threshold = rdd_indexed.filter(lambda x: x[1] < _threshold)
        self._top_k_edges = rdd_in_threshold.map(lambda x: (x[0][0], x[0][1], x[0][2]))


class CardinalityNodePruningSpark(CardinalityEdgePruningSpark):
    """A Meta-blocking method that retains for every entity, \
        the comparisons that correspond to its top-k weighted edges in the blocking graph."
    """

    _method_name = "Cardinality Node Pruning Spark"
    _method_short_name: str = "CNP"
    _method_info = "A Meta-blocking method that retains for every entity, " + \
                   "the comparisons that correspond to its top-k weighted edges in the blocking graph."

    def __init__(self, spark: SparkContext,num_slices: int = 10, weighting_scheme: str = 'CBS') -> None:
        super().__init__(spark, num_slices, weighting_scheme)
        self._nearest_entities: dict
        self._rdd_nearest_entities: RDD
        self._node_centric = True
        self._top_k_edges: RDD
        self._number_of_nearest_neighbors: int = None

    def _prune_edges(self) -> dict:

        limit = self._limit
        rdd_entity_index = self._rdd_entity_index
        rdd_entity_index_limit = rdd_entity_index.filter(lambda entity: entity[0] < limit)
        rdd_processed_entity = self._process_entity(rdd_entity_index_limit)
        self._verify_valid_entities(rdd_processed_entity)

        return self._retain_valid_comparisons()

    def _retain_valid_comparisons(self) -> dict:

        rdd_is_valid_comparison = self._is_valid_comparison(self._rdd_nearest_entities)
        rdd_blocks = rdd_is_valid_comparison.reduceByKey(lambda a,b: a.union(b))

        self.blocks = rdd_blocks.collectAsMap()

        return self.blocks

    def _is_valid_comparison(self, rdd: RDD) -> RDD:
        _nearest_entities = dict(self._nearest_entities)

        def valid_comparison(entity):
            e_id = entity[0]
            n_id = list(entity[1])[0]
            if n_id not in _nearest_entities:
                return True
            if e_id in _nearest_entities[n_id]:
                return e_id < n_id
            return True

        return rdd.filter(valid_comparison)

    #     if neighbor_id not in self._nearest_entities:
    #         return True
    #     if entity_id in self._nearest_entities[neighbor_id]:
    #         return entity_id < neighbor_id
    #     return True

    def _set_threshold(self) -> None:
        if (self._number_of_nearest_neighbors is None):
            # _blocks_bc = self._blocks_bc

            def summary_block_size(entity):
                global _blocks_bc
                global my_thresh
                
                token, _ = entity
                my_thresh += _blocks_bc.value[token].get_size()
                

            self.rdd_blocks.foreach(summary_block_size)

            block_assignments = my_thresh.value
            self._threshold = max(1, block_assignments / self.data.num_of_entities)
        else:
            self._threshold = self._number_of_nearest_neighbors

    def _verify_valid_entities(self, rdd: RDD) -> None:
        _minimum_weight = sys.float_info.min
        _threshold = self._threshold

        rdd_get_weight = self._get_weight(rdd).filter(lambda x: x[2] >= _minimum_weight)

        # rdd_sorted = rdd_get_weight.sortBy(lambda x: (x[2],x[0],x[1]), ascending=False)
        rdd_for_grouping_by_entity = rdd_get_weight.map(lambda x: (x[0], [(x[1], x[2])]))
        rdd_grouped = rdd_for_grouping_by_entity.reduceByKey(lambda a,b: a+b)

        def check_in_threshold(entity):
            e_id, weight_list = entity
            minimum_weight = _minimum_weight
            
            
            sorted_weight_list = sorted(weight_list, reverse=True, 
                                key = lambda t: (t[1],t[0]))

            filtered_weights = [(e_id, n_id, _weight) for n_id, _weight in sorted_weight_list[:int(_threshold)]]
            for item in filtered_weights:
                yield item
        

        rdd_in_threshold = rdd_grouped.flatMap(check_in_threshold)

        
        self._top_k_edges = rdd_in_threshold
        if self.store_weights:
            def my_canonical_swap(entity):
                id1, id2, _weight = entity
                if id2 > id1:
                    return (id1, id2), _weight
                else:
                    return (id2, id1), _weight

            rdd_store_weights = self._top_k_edges.map(my_canonical_swap)
            self._stored_weights = rdd_store_weights.collectAsMap()

        self._rdd_nearest_entities = self._top_k_edges.map(lambda x: (x[0], {x[1]}))
        _rdd_nearest_entities_in_set = self._rdd_nearest_entities.reduceByKey(lambda a,b: a.union(b))
        self._nearest_entities = _rdd_nearest_entities_in_set.collectAsMap()


class BLASTSpark(WeightedNodePruningSpark):
    """Meta-blocking method that retains the comparisons \
        that correspond to edges in the blocking graph that are exceed 1/4 of the sum \
        of the maximum edge weights in the two adjacent node neighborhoods.
    """

    _method_name = _method_short_name = "BLAST Spark"
    _method_info = "Meta-blocking method that retains the comparisons " + \
                   "that correspond to edges in the blocking graph that are exceed 1/4 of the sum " + \
                   "of the maximum edge weights in the two adjacent node neighborhoods."

    def __init__(self, spark: SparkContext, num_slices: int = 10, weighting_scheme: str = 'X2') -> None:
        super().__init__(spark, num_slices, weighting_scheme)

    def _get_valid_weight(self, rdd: RDD) -> RDD:
        _average_weight = self._average_weight
        rdd_weight = self.rdd_weights

        def valid_weight(entity):
            e_id, n_id, weight = entity
            edge_threshold = (_average_weight[e_id] + _average_weight[n_id]) / 4
            edge_threshold = (edge_threshold <= weight and e_id < n_id)
            return e_id, n_id, edge_threshold

        rdd_valid_weight = rdd_weight.map(valid_weight)
        return rdd_valid_weight

    def _update_threshold(self, rdd: RDD) -> None:
        limit = self._limit
        self.rdd_weights = self._get_weight(rdd)

        rdd_weights_by_key = self.rdd_weights.map(lambda x: (x[0], [x[2]])).reduceByKey(lambda a,b: a+b)

        def get_max_weight(entity):
            e_id, weight_list = entity
            if e_id > limit:
                return e_id, 0
            else:
                return e_id, max(weight_list)

        rdd_average_weight_fixed = rdd_weights_by_key.map(get_max_weight)

        # rdd_zeros = self.sc.parallelize((range(limit))).map(lambda x: (x, 0))
        # rdd_average_weight_with_zeros = rdd_average_weight.union(rdd_zeros).reduceByKey(lambda a, b: a + b).sortByKey()
        # rdd_average_weight_fixed = rdd_average_weight_with_zeros.map(lambda x: x[1])
        self._average_weight = rdd_average_weight_fixed.collectAsMap()


class ReciprocalCardinalityNodePruningSpark(CardinalityNodePruningSpark):
    """A Meta-blocking method that retains the comparisons \
        that correspond to edges in the blocking graph that are among the top-k weighted  \
        ones for both adjacent entities/nodes.
    """

    _method_name = "Reciprocal Cardinality Node Pruning Spark"
    _method_short_name: str = "RCNP"
    _method_info = "A Meta-blocking method that retains the comparisons " + \
                   "that correspond to edges in the blocking graph that are among " + \
                   "the top-k weighted ones for both adjacent entities/nodes."

    def __init__(self, spark: SparkContext, num_slices: int = 10, weighting_scheme: str = 'CN-CBS') -> None:
        super().__init__(spark, num_slices, weighting_scheme)

    def _is_valid_comparison(self, rdd: RDD) -> RDD:
        # print(len(self._nearest_entities))
        _nearest_entities = self._nearest_entities
        # _nearest_entities_bc = self.sc.broadcast(_nearest_entities)

        def valid_comparison(entity):
            e_id = entity[0]
            n_id = list(entity[1])[0]
            if n_id not in _nearest_entities:
                return False
            if e_id in _nearest_entities[n_id]:
                return e_id < n_id
            return False

        return rdd.filter(valid_comparison)


class ReciprocalWeightedNodePruningSpark(WeightedNodePruningSpark):
    """Meta-blocking method that retains the comparisons\
        that correspond to edges in the blocking graph that are \
        exceed the average edge weight in both adjacent node neighborhoods.
    """

    _method_name = "Reciprocal Weighted Node Pruning Spark"
    _method_short_name: str = "RWNP"
    _method_info = "Meta-blocking method that retains the comparisons " + \
                   "that correspond to edges in the blocking graph that are " + \
                   "exceed the average edge weight in both adjacent node neighborhoods."

    def __init__(self, spark: SparkContext, num_slices: int = 10, weighting_scheme: str = 'CN-CBS') -> None:
        super().__init__(spark, num_slices, weighting_scheme)

    def _get_valid_weight(self, rdd: RDD) -> RDD:
        _average_weight = self._average_weight
        rdd_weight = self.rdd_weights
        _average_weight_bc = self.sc.broadcast(_average_weight)

        def valid_weight(entity):
            e_id, n_id, weight = entity
            _valid_weight = weight if ((_average_weight_bc.value[e_id] <= weight and \
                                        _average_weight_bc.value[n_id] <= weight) and
                                       e_id < n_id) else 0

            return e_id, n_id, _valid_weight

        rdd_valid_weight = rdd_weight.map(valid_weight)
        return rdd_valid_weight


class ProgressiveCardinalityEdgePruningSpark(CardinalityEdgePruningSpark):
    def __init__(self, spark: SparkContext, num_slices: int = 10, weighting_scheme: str = 'JS', budget: int = 0) -> None:
        super().__init__(spark, num_slices, weighting_scheme)
        self._budget = budget

    def _set_threshold(self) -> None:
        self._threshold = self._budget

    def process(self, blocks: dict, data: Data, tqdm_disable: bool = False, store_weights: bool = True,
                cc: AbstractMetablockingSpark = None, emit_all_tps_stop: bool = False) -> dict:

        self._emit_all_tps_stop: bool = emit_all_tps_stop
        self._budget = self._budget if not self._emit_all_tps_stop else float('inf')
        if (cc is None):
            return super().process(blocks, data, tqdm_disable, store_weights)
        else:
            self._threshold = self._budget
            # self._top_k_edges = PriorityQueue(int(2 * self._threshold))
            self._minimum_weight = sys.float_info.min
            self.trimmed_blocks: dict = defaultdict(set)
            num_slices = self.num_slices * 3
            rdd_blocks = self.sc.parallelize(blocks.items(),numSlices = num_slices)

            def func_neighbor(entity):
                e_id, n_list = entity
                to_ret = list()
                for n_id in n_list:
                    yield e_id, n_id


            _minimum_weight = self._minimum_weight
            _threshold = self._threshold

            rdd_neighbors = rdd_blocks.flatMap(func_neighbor)
            rdd_get_precalculated_weight = cc.get_precalculated_weight(rdd_neighbors)

            rdd_get_weight = rdd_get_precalculated_weight.filter(lambda x: x[2] >= _minimum_weight) 
                # .map(lambda x: (x[2], (x[0], x[1])))

            # rdd_sorted = rdd_get_weight.sortByKey(ascending=False)
            # rdd_indexed = rdd_sorted.zipWithIndex()
            # rdd_in_threshold = rdd_indexed.filter(lambda x: x[1] < _threshold)
            self._top_k_edges = self.sc.parallelize(rdd_get_weight.takeOrdered(int(_threshold), key=lambda x: (-x[2],-x[0],-x[1])),numSlices = num_slices)

            if self.store_weights:
                def my_canonical_swap(entity):
                    id1, id2, _weight = entity
                    if id2 > id1:
                        return (id1, id2), _weight
                    else:
                        return (id2, id1), _weight

                self._stored_weights = self._top_k_edges.map(my_canonical_swap).collectAsMap()

            rdd_trimmed_blocks = self._top_k_edges.map(lambda x: (x[0], {x[1]})).reduceByKey(lambda a,b: a.union(b))
            self.trimmed_blocks = rdd_trimmed_blocks.collectAsMap()
            return self.trimmed_blocks
