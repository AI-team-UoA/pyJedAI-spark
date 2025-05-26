"""Block Cleaning Module
Contains:
 - BlockFiltering
 - BlockPurging
"""
from abc import ABC
from collections import defaultdict
from time import time
from typing import Tuple
from sys import getsizeof


import numpy as np
from pyspark import SparkContext, RDD
from tqdm.autonotebook import tqdm

from pyjedai.block_cleaning import AbstractBlockCleaning
from pyjedai.block_building import AbstractBlockProcessing
from pyjedai.datamodel import Block, Data
from pyjedai.utils import create_entity_index, java_math_round
from pyjedai.parallel.utils_spark import create_entity_index, drop_single_entity_blocks


# from pyjedai.parallel.accumulators import ListAccumulator


#
# class AbstractBlockCleaningSpark(AbstractBlockProcessing):
#
#     def __init__(self, spark: SparkContext) -> None:
#         super().__init__()
#         self.sc = spark


class BlockFilteringSpark(AbstractBlockCleaning):
    """Retains every entity in a subset of its smallest blocks.

        Filtering consists of 3 steps:
        - Blocks sort in ascending cardinality
        - Creation of Entity Index: inversed block dictionary
        - Retain every entity in ratio % of its smallest blocks
        - Blocks reconstruction
    """

    _method_name = "Block Filtering Spark"
    _method_short_name: str = "BF"
    _method_info = "Retains every entity in a subset of its smallest blocks."

    def __init__(self, spark: SparkContext, ratio: float = 0.8) -> None:
        super().__init__()
        self.sc = spark
        if ratio > 1.0 or ratio < 0.0:
            raise AttributeError("Ratio is a number between 0.0 and 1.0")
        else:
            self.ratio = ratio
        self.entity_index: dict

    def __str__(self) -> str:
        print(self._method_name + self._method_info)
        print("Ratio: ", self.ratio)
        return super().__str__()

    def process(
            self,
            blocks: dict,
            data: Data,
            tqdm_disable: bool = False
    ) -> Tuple[dict, dict]:
        """Main method of Block Filtering.

        Args:
            blocks (dict): dict of keys to Blocks.
            data (Data): input dataset.
            tqdm_disable (bool, optional): disable progress bars. Defaults to False.

        Returns:
            Tuple[dict, dict]: dict of keys to Blocks, entity index (reversed blocks)
        """
        start_time, self.tqdm_disable, self.data = time(), tqdm_disable, data
        self._progress_bar = tqdm(
            total=3,
            desc=self._method_name,
            disable=self.tqdm_disable
        )

        blocks_length = len(blocks)
        memory_usage_bytes = getsizeof(blocks)
        memory_usage_kb =  memory_usage_bytes / 1024
        num_slices = int(
            memory_usage_kb / (3 * 1024))
        print(num_slices)
        if num_slices < 1:
            num_slices = 1

        rdd_blocks = self.sc.parallelize(list(blocks.items()), numSlices=num_slices)
        rdd_sorted_blocks = _sort_blocks_cardinality(rdd_blocks, self.data.is_dirty_er)
        self._progress_bar.update(1)

        rdd_entity_index = create_entity_index(rdd_sorted_blocks, self.data.is_dirty_er)
        self.entity_index = dict(rdd_entity_index.collect())
        self._progress_bar.update(1)

        ratio = self.ratio
        dataset_limit = self.data.dataset_limit

        is_dirty_er = self.data.is_dirty_er
        dirty_bc = self.sc.broadcast(is_dirty_er)
        limit_bc = self.sc.broadcast(dataset_limit)


        def process_entity_index(entity_index):
            entity_id, block_keys = entity_index
            return [(key, entity_id) for key in block_keys[:java_math_round(ratio * float(len(block_keys)))]]

        def filter_blocks(block):
            key, list_eid = block
            filtered_block = Block()
            filtered_block.entities_D1.update({eid for eid in list_eid if eid < dataset_limit})
            filtered_block.entities_D2.update({eid for eid in list_eid if eid >= dataset_limit})
            return (key, filtered_block)

        rdd_filtered_blocks = rdd_entity_index.map(process_entity_index).flatMap(lambda x: x) \
            .groupByKey() \
            .mapValues(list) \
            .map(filter_blocks)

        # self.filtered_blocks = rdd_filtered_blocks.collect()

        self._progress_bar.update(1)
        new_blocks = dict(drop_single_entity_blocks(rdd_filtered_blocks, dirty_bc, limit_bc).collect())
        self._progress_bar.close()
        self.num_of_blocks_dropped = len(blocks) - len(new_blocks)
        self.execution_time = time() - start_time
        self.blocks = new_blocks

        return self.blocks

    def _configuration(self) -> dict:
        return {
            "Ratio": self.ratio
        }


class BlockPurgingSpark(AbstractBlockCleaning):
    """Discards the blocks exceeding a certain number of comparisons.
    """

    _method_name = "Block Purging"
    _method_short_name: str = "BP"
    _method_info = "Discards the blocks exceeding a certain number of comparisons."

    def __init__(self, spark: SparkContext, smoothing_factor: float = 1.025) -> any:
        super().__init__()
        self.sc = spark
        self.smoothing_factor: float = smoothing_factor
        self.max_comparisons_per_block: float

    def process(
            self,
            blocks: dict,
            data: Data,
            tqdm_disable: bool = False
    ) -> dict:
        """Main method of Block Purging.

        Args:
            blocks (dict): Blocks of entities.
            data (Data): Data module. Contains all the information about the dataset.
            tqdm_disable (bool, optional): Disable progress bar. Defaults to False.

        Returns:
            dict: Purged blocks.
        """

        self.tqdm_disable, self.data, start_time = tqdm_disable, data, time()
        if not blocks:
            raise AttributeError("Empty dict of blocks was given as input!")
        else:
            
            # memory_usage_bytes = blocks.memory_usage(deep=True).sum()
            # memory_usage_kb = memory_usage_bytes / 1024
            # num_slices = int(
            # memory_usage_kb / (3 * 1024))
            # print(num_slices)
            # if num_slices < 1:
            #     num_slices = 1
            num_slices = 40
            # if num_slices < 1:
            #     num_slices = 1
            print(num_slices)

            new_blocks = blocks
            rdd_new_blocks = self.sc.parallelize(list(blocks.items()),numSlices=num_slices)
        self._progress_bar = tqdm(total=2 * len(new_blocks), desc=self._method_name, disable=self.tqdm_disable)
        self._set_threshold(rdd_new_blocks)
        is_dirty_er = self.data.is_dirty_er
        max_comparisons_per_block = self.max_comparisons_per_block

        def _cardinality_threshold(id_block_tuple) -> bool:
            return id_block_tuple[1].get_cardinality(is_dirty_er) <= max_comparisons_per_block

        rdd_threshold_blocks = rdd_new_blocks.filter(_cardinality_threshold)

        # new_blocks = dict(filter(self._cardinality_threshold, blocks.items()))
        self._progress_bar.close()
        self.execution_time = time() - start_time
        self.blocks = dict(rdd_threshold_blocks.collect())
        new_blocks = self.blocks

        return new_blocks

    # Not Easily Parallelizable needs to get prev state stuff....
    # Is there any way??

    def _set_threshold(self, rdd_blocks: RDD) -> None:
        is_dirty_er = self.data.is_dirty_er

        blocks_comparisons_and_sizes = rdd_blocks.map(
            lambda block: (block[1].get_cardinality(is_dirty_er), block[1].get_size()))
        block_comparisons_and_sizes_per_comparison_level = blocks_comparisons_and_sizes.map(lambda x: (x[0], x))
        total_number_of_comparisons_and_size_per_comparison_level = block_comparisons_and_sizes_per_comparison_level \
            .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
        total_number_of_comparisons_and_size_per_comparison_level_sorted = \
            total_number_of_comparisons_and_size_per_comparison_level.sortBy(lambda x: x[0]).collect()

        def sum_precedent_levels(b):
            for i in range(0, len(b) - 1):
                b[i + 1] = (b[i + 1][0], (b[i][1][0] + b[i + 1][1][0], b[i][1][1] + b[i + 1][1][1]))
            return b

        input_d = sum_precedent_levels(
            total_number_of_comparisons_and_size_per_comparison_level_sorted)

        current_bc = current_cc = current_size = previous_size = 0

        for i in range(len(input_d) - 1, -1, -1):
            previous_size = current_size
            previous_bc = current_bc
            previous_cc = current_cc
            current_size = input_d[i][0]
            current_bc = input_d[i][1][1]
            current_cc = input_d[i][1][0]

            if current_bc * previous_cc < self.smoothing_factor * current_cc * previous_bc:
                break

        self.max_comparisons_per_block = previous_size

    # def _set_threshold(self, blocks: dict) -> None:
    #     """Calculates the maximum number of comparisons per block, so in the next step to be purged.
    #
    #     Args:
    #         blocks (dict): _description_
    #     """
    #     sorted_blocks = _sort_blocks_cardinality(blocks, self.data.is_dirty_er)
    #     distinct_comparisons_level = set(b.get_cardinality(self.data.is_dirty_er) \
    #                                      for _, b in sorted_blocks.items())
    #     block_assignments = np.empty([len(distinct_comparisons_level)])
    #     comparisons_level = np.empty([len(distinct_comparisons_level)])
    #     total_comparisons_per_level = np.empty([len(distinct_comparisons_level)])
    #     index = -1
    #     for _, block in sorted_blocks.items():
    #         if index == -1:
    #             index += 1
    #             comparisons_level[index] = block.get_cardinality(self.data.is_dirty_er)
    #             block_assignments[index] = 0
    #             total_comparisons_per_level[index] = 0
    #         elif block.get_cardinality(self.data.is_dirty_er) != comparisons_level[index]:
    #             index += 1
    #             comparisons_level[index] = block.get_cardinality(self.data.is_dirty_er)
    #             block_assignments[index] = block_assignments[index - 1]
    #             total_comparisons_per_level[index] = total_comparisons_per_level[index - 1]
    #
    #         block_assignments[index] += block.get_size()
    #         total_comparisons_per_level[index] += block.get_cardinality(self.data.is_dirty_er)
    #         self._progress_bar.update(1)
    #
    #     current_bc = current_cc = current_size = \
    #         previous_bc = previous_cc = previous_size = 0
    #     for i in range(len(block_assignments) - 1, 0, -1):
    #         previous_size = current_size
    #         previous_bc = current_bc
    #         previous_cc = current_cc
    #         current_size = comparisons_level[i]
    #         current_bc = block_assignments[i]
    #         current_cc = total_comparisons_per_level[i]
    #         if current_bc * previous_cc < self.smoothing_factor * current_cc * previous_bc:
    #             break
    #     self.max_comparisons_per_block = previous_size

    def _satisfies_threshold(self, block: Block) -> bool:
        return block.get_cardinality(self.data.is_dirty_er) <= self.max_comparisons_per_block

    def _configuration(self) -> dict:
        return {
            "Smoothing factor": self.smoothing_factor,
            "Max Comparisons per Block": self.max_comparisons_per_block
        }


def _sort_blocks_cardinality(blocks: dict, is_dirty_er: bool) -> dict:
    # return blocks.sortBy(lambda b: b[1].get_cardinality(is_dirty_er))
    return dict(sorted(blocks.items(), key=lambda x: x[1].get_cardinality(is_dirty_er)))


def _sort_blocks_cardinality(rdd_blocks: RDD, is_dirty_er: bool) -> RDD:
    return rdd_blocks.sortBy(lambda x: x[1].get_cardinality(is_dirty_er))
