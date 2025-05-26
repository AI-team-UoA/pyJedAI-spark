import math
import re
import time
import sys
from abc import abstractmethod
from typing import Tuple
import pyspark
from math import ceil

from itertools import chain

import nltk
from pyspark import SparkContext, RDD
from tqdm.auto import tqdm
from ordered_set import OrderedSet


from pyjedai.datamodel import Block, Data

from pyjedai.parallel.utils_spark import (drop_single_entity_blocks, drop_big_blocks_by_size)
from pyjedai.block_building import AbstractBlockProcessing


class AbstractBlockBuildingSpark(AbstractBlockProcessing):
    """Abstract class for the block building method
   """

    def __init__(self, spark: SparkContext):
        super().__init__()
        self.sc = spark
        self.num_slices : int = 0

    def build_blocks(
            self,
            data: Data,
            attributes_1: list = None,
            attributes_2: list = None,
            tqdm_disable: bool = False,
            num_slices: int = 10
    ) -> Tuple[dict, dict]:
        _start_time = time.time()
        self.data, self.attributes_1, self.attributes_2 = data, attributes_1, attributes_2
        self._progress_bar = tqdm(
            total=5, desc=self._method_name, disable=tqdm_disable
        )

        dataframe = data.dataset_1[attributes_1 if attributes_1 else data.attributes_1]
        dataframe['Index'] = dataframe.index

        
        # data.dataset_1['Index'] = data.dataset_1.index
       
        # if attribu/tes_1:
            # attributes_1.insert(0,'Index')
        # else: 
            # data.attributes_1.insert(0,'Index')
        
        

        # Broadcasting variables for better runtime in rdd
        self.entities_d1_counter_bc = self.sc.broadcast(len(data.dataset_1))
        self.is_dirty_er_bc = self.sc.broadcast(data.is_dirty_er)
        self.max_block_size_bc = self.sc.broadcast(self.max_block_size)

        entities_d1_counter = self.entities_d1_counter_bc
        
        if num_slices < 10:
            num_slices = 10


        num_slices = num_slices + (10 - num_slices%10)
        self.num_slices = num_slices

        
        rdd_data = self.sc.parallelize(dataframe.values,
                                       numSlices=num_slices)



        #Propably needs fixing ... (id may not be index)
        
        rdd_data = rdd_data.map(lambda x: (x[-1], x[:-1]))
        
        # atr_len = len( [ attributes_1 if attributes_1 else data.attributes_1 ]  )
        
        # rdd_data = rdd_data.map()
        # rdd_data.persist(pyspark.StorageLevel.MEMORY_ONLY)

        rdd_joined = rdd_data.map(lambda x: (x[0]," ".join(x[1])))
        
        rdd_entities_d1 = self._tokenize_entity(rdd_joined)
        print(rdd_joined.top(10)) 


        # print(rdd_entities_d1.take(2))
        self._progress_bar.update(1)
        # TODO Text process function can be applied in this step (.apply)

        rdd_entities_d2 = None
        num_slices_d2 = 0
        if not data.is_dirty_er:

            index_offset = len(dataframe.index)
            print(index_offset)
            
            dataframe = data.dataset_2[attributes_2 if attributes_2 else data.attributes_2]
            dataframe['Index'] = dataframe.index
            
            memory_usage_bytes = dataframe.memory_usage(deep=True).sum()
            memory_usage_kb = memory_usage_bytes / 1024
            num_slices_d2 = int(
                memory_usage_kb / (3 * 1024))
            if num_slices_d2 < 1:
                num_slices_d2 = 1
            rdd_data2 = self.sc.parallelize(dataframe.values,
                                            numSlices=num_slices_d2)
            rdd_data2 = rdd_data2.map(lambda x: (x[-1], list(x[:-1])))
            rdd_joined2 = rdd_data2.map(lambda x: (x[0]+index_offset," ".join(x[1])))

            rdd_entities_d2 = self._tokenize_entity(rdd_joined2)

        self._progress_bar.update(1)

        def process_entity(item):
            eid, entity = item
            for token in entity:
                yield token, {eid}
            # list_return = [(token, eid) for token in entity]
            # return list_return

        def create_blocks(block):
            counter = entities_d1_counter.value - 1
            token, list_eid = block
            my_block = Block()
            my_block.entities_D1.update({eid for eid in list_eid if eid <= counter})
            if not data.is_dirty_er:
                my_block.entities_D2.update({eid for eid in list_eid if eid > counter})
            return (token, my_block)

        def union_sets(my_set):
            block, sets = my_set
            return block, sets[0].union(sets[1])


        rdd_entities_d1_with_index = rdd_entities_d1
        # .zipWithIndex()

        rdd_entities_d1_to_clean = rdd_entities_d1_with_index
        #  \
            # .flatMap(process_entity)
    
        rdd_reduced = rdd_entities_d1_to_clean.reduceByKey(lambda a,b: a.union(b))

        # rdd_reduced = rdd_entities_d1_to_clean.aggregateByKey(set(), lambda a, b: a.union({b}),
                                                            #   lambda a, b: a.union(b))

        if not data.is_dirty_er:
            rdd_entities_d2_with_index = rdd_entities_d2
            
            #.zipWithIndex().map(lambda x: (x[0], x[1] + entities_d1_counter.value))
            
            rdd_entities_d2_to_clean = rdd_entities_d2_with_index
            # .flatMap(process_entity)
            rdd_reduced_d2 = rdd_entities_d2_to_clean.reduceByKey(lambda a,b: a.union(b))
            
            # rdd_reduced_d2 = rdd_entities_d2_to_clean.aggregateByKey(set(), lambda a, b: a.union({b}),
                                                                    #  lambda a, b: a.union(b))
            rdd_reduced = rdd_reduced.join(rdd_reduced_d2).map(union_sets)
            
            # .map(lambda x: (x[0], set(x[1][0]).union(set(x[1][1]))))
            # print(rdd_reduced.take(10))


            # rdd_blocks_to_clean = rdd_entities_d1_to_clean.join(rdd_entities_d2_to_clean).map(
            #     lambda x: (x[0], set(x[1][0]).union(set(x[1][1]))))

        # else:
        # rdd_blocks_to_clean = rdd_entities_d1_to_clean.mapValues(set)

        # rdd_blocks_to_clean = rdd_entities_d2_to_clean

        # num_slices_d1 = int((len(data.dataset_2) + len(data.dataset_1)) / 20000)
        # print(num_slices_d1)
        # if num_slices_d1 < 1:
        #     num_slices_d1 = 1
        #
        # rdd_entities_d1 = rdd_entities_d1.union(rdd_entities_d2)

        # rdd_entities_d1.cache()
        # rdd_entities_d1 = rdd_entities_d1.repartition(num_slices_d1)
        # else:
        #     rdd_entities_union = rdd_entities_d1
        # rdd_entities_d1 = rdd_entities_d1.partitionBy(num_slices_d1, lambda x: hash(x))
        # rdd_entities_d1 = rdd_entities_d1.repartition(num_slices_d1)

        # rdd_blocks_to_clean = rdd_entities_d1.zipWithIndex() \
        #     .map(process_entity) \
        #     .flatMap(lambda x: x).partitionBy(num_slices_d1, lambda x: hash(x)) \
        #     .groupByKey() \
        #     .mapValues(set)

        self._progress_bar.update(1)

        rdd_cleaned = self._clean_blocks(rdd_reduced)

        # if rdd_cleaned.getNumPartitions() > num_cores:
        #     rdd_cleaned = rdd_cleaned.coalesce(num_cores)

        rdd_final_blocks = rdd_cleaned.map(create_blocks)
        self._progress_bar.update(1)

        self.blocks = rdd_final_blocks.collectAsMap()
        # rdd_final_blocks.cache()

        self._progress_bar.update(1)

        # self.original_num_of_blocks = len(blocks)
        # self.blocks = self._clean_blocks(blocks)
        # self.num_of_blocks_dropped = len(blocks) - len(self.blocks)
        self.execution_time = time.time() - _start_time
        self._progress_bar.close()

        return self.blocks

    @abstractmethod
    def _clean_blocks(self, rdd: RDD) -> RDD:
        pass
    # @abstractmethod
    # def _clean_blocks(self, blocks: dict) -> dict:
    #     pass


def get_attr_index(attributes, attributes_original):
    if attributes:
        return [attributes_original.index(atr) for atr in attributes]

    return [i + 1 for i in range(len(attributes_original))]


class StandardBlockingSpark(AbstractBlockBuildingSpark):
    """ Creates one block for every token in \
        the attribute values of at least two entities.
    """

    _method_name = "Standard Blocking Spark"
    _method_short_name: str = "SB"
    _method_info = "Creates one block for every token in " + \
                   "the attribute values of at least two entities."

    def __init__(self, spark: SparkContext) -> any:
        super().__init__(spark)

    def _tokenize_entity(self, rdd: RDD) -> RDD:
        """Produces a list of words of a given string

        Args:
            entity (str): String representation  of an entity

        Returns:
            list: List of words
        """
        def tokenize(entity):
            e_id, tokens = entity
            for token in list(set(filter(None, re.split('[\\W_]', tokens.lower())))):
                yield token, {e_id}

        return rdd.flatMap(tokenize)

    def _clean_blocks(self, rdd: RDD) -> RDD:
        """No cleaning"""
        return drop_single_entity_blocks(rdd, self.is_dirty_er_bc, self.entities_d1_counter_bc)

    def _configuration(self) -> dict:
        """No configuration"""
        return {}


class QGramsBlockingSpark(StandardBlockingSpark):
    """ Creates one block for every q-gram that is extracted \
        from any token in the attribute values of any entity. \
            The q-gram must be shared by at least two entities.
    """

    _method_name = "Q-Grams Blocking"
    _method_short_name: str = "QGB"
    _method_info = "Creates one block for every q-gram that is extracted " + \
                   "from any token in the attribute values of any entity. " + \
                   "The q-gram must be shared by at least two entities."

    def __init__(
            self, spark: SparkContext, qgrams: int = 6
    ) -> any:
        super().__init__(spark)
        self.qgrams = qgrams

    def _tokenize_entity(block, rdd: RDD) -> RDD:
        rdd_tokens = super()._tokenize_entity(rdd)

        # Broadcast the value of block.qgrams
        qgrams_broadcast = block.sc.broadcast(block.qgrams)

        def key_function(entity):
            qgrams = qgrams_broadcast.value
            token, e_id_set = entity
            e_id = list(e_id_set)[0]
            if len(token) < qgrams:
                yield token, {e_id}
            else:
                for token_1 in [''.join(qg) for qg in nltk.ngrams(token, n=qgrams)]:
                    yield token_1, {e_id}


        keys_rdd = rdd_tokens.flatMap(key_function)
        return keys_rdd

    def _clean_blocks(self, rdd: RDD) -> RDD:
        return drop_single_entity_blocks(rdd, self.is_dirty_er_bc, self.entities_d1_counter_bc)

    # def _clean_blocks(self, blocks: dict) -> dict:
    #     return drop_single_entity_blocks(blocks, self.data.is_dirty_er)

    def _configuration(self) -> dict:
        return {
            "Q-Gramms": self.qgrams
        }


class SuffixArraysBlockingSpark(StandardBlockingSpark):
    """ It creates one block for every suffix that appears \
        in the attribute value tokens of at least two entities.
    """

    _method_name = "Suffix Arrays Blocking"
    _method_short_name: str = "SAB"
    _method_info = "Creates one block for every suffix that appears in the " + \
                   "attribute value tokens of at least two entities."

    def __init__(
            self,
            spark: SparkContext,
            suffix_length: int = 6,
            max_block_size: int = 53
    ) -> any:
        super().__init__(spark)
        self.suffix_length, self.max_block_size = suffix_length, max_block_size

    def _tokenize_entity(block, rdd: RDD) -> RDD:
        rdd_tokens = super()._tokenize_entity(rdd)

        # Broadcast the value of block.suffix_length
        suffix_length_broadcast = block.sc.broadcast(block.suffix_length)

        def key_function(entity):
            suffix_length = suffix_length_broadcast.value
            token, e_id_set = entity
            e_id = list(e_id_set)[0]
            if len(token) < suffix_length:
                yield token, {e_id}
            else:
                for length in range(0, len(token) - suffix_length + 1):
                    yield token[length:], {e_id}


        keys_rdd = rdd_tokens.flatMap(key_function)

        return keys_rdd

    def _clean_blocks(self, rdd: RDD) -> RDD:
        return drop_big_blocks_by_size(rdd, self.max_block_size_bc, self.is_dirty_er_bc, self.entities_d1_counter_bc)

    def _configuration(self) -> dict:
        return {
            "Suffix length": self.suffix_length,
            "Maximum Block Size": self.max_block_size
        }


class ExtendedSuffixArraysBlockingSpark(StandardBlockingSpark):
    """ It creates one block for every substring \
        (not just suffix) that appears in the tokens of at least two entities.
    """

    _method_name = "Extended Suffix Arrays Blocking"
    _method_short_name: str = "ESAB"
    _method_info = "Creates one block for every substring (not just suffix) " + \
                   "that appears in the tokens of at least two entities."

    def __init__(
            self,
            spark: SparkContext,
            suffix_length: int = 6,
            max_block_size: int = 39
    ) -> any:
        super().__init__(spark)
        self.suffix_length, self.max_block_size = suffix_length, max_block_size

    def _tokenize_entity(block, rdd: RDD) -> RDD:
        rdd_tokens = super()._tokenize_entity(rdd)

        # Broadcast the value of block.suffix_length
        suffix_length_broadcast = block.sc.broadcast(block.suffix_length)

        def key_function(entity):
            suffix_length = suffix_length_broadcast.value
            token, e_id_set = entity
            e_id = list(e_id_set)[0]
            yield token, {e_id}
            if len(token) > suffix_length:
                for current_size in range(suffix_length, len(token)):
                    for letters in list(nltk.ngrams(token, n=current_size)):
                        yield "".join(letters), {e_id}

        keys_rdd = rdd_tokens.flatMap(key_function)

        return keys_rdd

    def _clean_blocks(self, rdd: RDD) -> RDD:
        return drop_big_blocks_by_size(rdd, self.max_block_size_bc, self.is_dirty_er_bc, self.entities_d1_counter_bc)

    def _configuration(self) -> dict:
        return {
            "Suffix length": self.suffix_length,
            "Maximum Block Size": self.max_block_size
        }


class ExtendedQGramsBlockingSpark(StandardBlockingSpark):
    """It creates one block for every combination of q-grams that represents at least two entities.
    The q-grams are extracted from any token in the attribute values of any entity.
    """

    _method_name = "Extended QGramsBlocking"
    _method_short_name: str = "EQGB"
    _method_info = "Creates one block for every substring (not just suffix) " + \
                   "that appears in the tokens of at least two entities."

    def __init__(
            self,
            spark: SparkContext,
            qgrams: int = 6,
            threshold: float = 0.95
    ) -> any:
        super().__init__(spark)
        self.threshold: float = threshold
        self.MAX_QGRAMS: int = 15
        self.qgrams = qgrams

    def _tokenize_entity(block, rdd: RDD) -> RDD:

        def _qgrams_combinations(sublists: list, sublist_length: int) -> list:
            if sublist_length == 0 or len(sublists) < sublist_length:
                return []

            remaining_elements = sublists.copy()
            last_sublist = remaining_elements.pop(len(sublists) - 1)

            combinations_exclusive_x = _qgrams_combinations(remaining_elements, sublist_length)
            combinations_inclusive_x = _qgrams_combinations(remaining_elements, sublist_length - 1)

            resulting_combinations = combinations_exclusive_x.copy() if combinations_exclusive_x else []

            if not combinations_inclusive_x:  # is empty
                resulting_combinations.append(last_sublist)
            else:
                for combination in combinations_inclusive_x:
                    resulting_combinations.append(combination + last_sublist)

            return resulting_combinations

        rdd_tokens = super()._tokenize_entity(rdd)

        # Broadcast the value of block.qgrams
        qgrams_broadcast = block.sc.broadcast(block.qgrams)
        MAX_QGRAMS_broadcast = block.sc.broadcast(block.MAX_QGRAMS)
        threshold_broadcast = block.sc.broadcast(block.threshold)

        def key_function(entity):
            qgrams = qgrams_broadcast.value
            MAX_QGRAMS = MAX_QGRAMS_broadcast.value
            threshold = threshold_broadcast.value


            token, e_id_set = entity
            e_id = list(e_id_set)[0]
            # keys_ret = []
            # e_id, tokens_list = entity

            if len(token) < qgrams:
                yield token, {e_id}
            # for token in tokens_list:
                
            else:
                qgrams_list = [''.join(qgram) for qgram in nltk.ngrams(token, n=qgrams)]
                if len(qgrams_list) == 1:
                    for t in qgrams_list:
                        yield t, {e_id}
                else:
                    if len(qgrams_list) > MAX_QGRAMS:
                        qgrams_list = qgrams_list[:MAX_QGRAMS]
                    minimum_length = max(1, math.floor(len(qgrams_list) * threshold))
                    for i in range(minimum_length, len(qgrams_list) + 1):
                        for t in _qgrams_combinations(qgrams_list, i):
                            yield t, {e_id}


        keys_rdd = rdd_tokens.flatMap(key_function)

        return keys_rdd

    def _clean_blocks(self, rdd: RDD) -> RDD:
        return drop_single_entity_blocks(rdd, self.is_dirty_er_bc, self.entities_d1_counter_bc)

    def _configuration(self) -> dict:
        return {
            "Q-Gramms": self.qgrams,
            "Threshold": self.threshold
        }
