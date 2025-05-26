from pyspark import RDD


# from pyjedai.utils import block_with_one_entity


def drop_single_entity_blocks(rdd: RDD, is_dirty_er, d1_counter) -> RDD:
    return rdd.filter(lambda entity: not block_with_one_entity(entity[1], is_dirty_er.value, d1_counter.value))


def drop_big_blocks_by_size(rdd: RDD, max_block_size, is_dirty_er, d1_counter) -> RDD:
    return rdd.filter(
        lambda e: not block_with_one_entity(e[1], is_dirty_er.value, d1_counter.value)
                  and len(e[1]) <= max_block_size.value
    )


def create_entity_index(rdd_blocks: RDD, is_dirty_er: bool) -> RDD:
    """Creates a rdd of entity ids to block keys
        Example:
            e_id -> ['block_key_1', ..]
            ...  -> [ ... ]
    """

    def process_block(block_tuple):
        key, block = block_tuple
        for e_id in block.entities_D1:
            yield (e_id, [key])
        # list_to_return = [(e_id, key) for e_id in block.entities_D1]
        if not is_dirty_er:
            for e_id in block.entities_D2:
                yield (e_id, [key])

            # list_to_return += [(e_id, key) for e_id in block.entities_D2]
        # return list_to_return

    rdd_entity_index = rdd_blocks.flatMap(process_block).reduceByKey(lambda a,b: a+b)

    return rdd_entity_index


def block_with_one_entity(block: list, is_dirty_er: bool, d1_counter: int) -> bool:
    return True if ((is_dirty_er and len(block) == 1) or \
                    (not is_dirty_er and (not contains_d1(block, d1_counter) or not contains_d2(block, d1_counter)))) \
        else False


def contains_d1(block: list, d1_counter: int) -> bool:
    block_min = min(block)
    return True if block_min <= d1_counter - 1 \
        else False


def contains_d2(block: list, d1_counter: int) -> bool:
    block_max = max(block)
    return True if block_max > d1_counter - 1 else False


# def predict(self, rdd: RDD) -> RDD:
#     """Returns the predicted similarity score for the given entities
#     Args:
#         id1 (int): id of an entity of the 1nd dataset within experiment context (not necessarily preloaded matrix)
#         id2 (int): id of an entity of the 2nd dataset within experiment context (not necessarily preloaded matrix)
#     Returns:
#         float: Similarity score of entities with specified IDs
#     """
#     # candidates = np.vstack((self.corpus_as_matrix[id1], self.corpus_as_matrix[id2]))
#     # distances = pairwise_distances(candidates, metric=self.metric)
#     # return 1.0 - distances[0][1]
#     if (self.indexing == self.distance_matrix_indexing):
#         return self.distance_matrix[id1][id2]
#     # _id1 = (id1 + self._entities_d2_num) if (self.indexing == "inorder") else (id1 + self._entities_d1_num)
#     # _id2 = (id2 - self._entities_d1_num) if (self.indexing == "inorder") else (id2 - self._entities_d2_num)
#     _id1 = (id1 + self._entities_d2_num)
#     _id2 = (id2 - self._entities_d1_num)
#
#     return self.distance_matrix[_id1][_id2]
