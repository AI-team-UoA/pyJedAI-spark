
from sys import getsizeof

from py_stringmatching.similarity_measure.cosine import Cosine
from py_stringmatching.similarity_measure.dice import Dice
from py_stringmatching.similarity_measure.generalized_jaccard import \
    GeneralizedJaccard
from py_stringmatching.similarity_measure.jaccard import Jaccard
from py_stringmatching.similarity_measure.jaro import Jaro
from py_stringmatching.similarity_measure.levenshtein import Levenshtein
from py_stringmatching.similarity_measure.overlap_coefficient import \
    OverlapCoefficient
from pyspark import SparkContext, RDD

from pyjedai.matching import EntityMatching
import warnings


class EntityMatchingSpark(EntityMatching):
    """Calculates similarity from 0.0 to 1.0 for all blocks
    """

    _method_name: str = "Entity Matching Spark"
    _method_info: str = "Calculates similarity from 0. to 1. for all blocks"

    def __init__(
            self,
            spark: SparkContext,
            num_slices: int = 10,
            metric: str = 'dice',
            tokenizer: str = 'white_space_tokenizer',
            vectorizer: str = None,
            qgram: int = 1,
            similarity_threshold: float = 0.5,
            tokenizer_return_unique_values=False,  # unique values or not,
            attributes: any = None,
    ) -> None:
        super().__init__(metric, tokenizer, vectorizer, qgram, similarity_threshold, tokenizer_return_unique_values,
                         attributes)
        
        self.sc = spark
        if num_slices < 10:
            num_slices = 10
        self.num_slices =  num_slices
        print("RUNNING NEW SPARK")
        

    def _predict_raw_blocks(self, blocks: dict) -> None:
        
        
        num_slices  = self.num_slices
        rdd_blocks = self.sc.parallelize(blocks.items(),numSlices=num_slices)

        if self.data.is_dirty_er:
            rdd_entities = rdd_blocks.map(lambda x: list(x[1].entities_D1))



            def _entities_first_pair_list(entity):
                for index_1 in range(0, len(entity), 1):
                    yield (entity[index_1], entity[index_1+1:])
                    
                
            def _to_entities_array(entity):
                first_pair, entity_list = entity
                for index_1 in range(0, len(entity_list), 1):
                    # for index_2 in range(index_1 + 1, len(entity), 1):
                    yield (first_pair, entity_list[index_1])

            rdd_entities_get_first_pair_list = rdd_entities.flatMap(_entities_first_pair_list)
            rdd_entities_array = rdd_entities_get_first_pair_list.flatMap(_to_entities_array)
            # rdd_entities_array = rdd_entities.flatMap(_to_entities_array)


            rdd_similarity = self._similarity(rdd_entities_array)
            if self.similarity_threshold is not None:
                similarity_threshold = self.similarity_threshold
                rdd_similarity = rdd_similarity.filter(lambda x: x[2]['weight'] > similarity_threshold)

            self.pairs.add_edges_from(rdd_similarity.collect())
        else:
            def _to_entities_array(entity):
                _, block = entity
                to_ret = []
                for entity_id1 in block.entities_D1:
                    for entity_id2 in block.entities_D2:
                        yield (entity_id1, entity_id2)

            rdd_entities_array = rdd_blocks.flatMap(_to_entities_array)
            rdd_similarity = self._similarity(rdd_entities_array)
            if self.similarity_threshold is not None:
                similarity_threshold = self.similarity_threshold
                rdd_similarity = rdd_similarity.filter(lambda x: x[2]['weight'] > similarity_threshold)

            self.pairs.add_edges_from(rdd_similarity.collect())


    def _similarity(self, rdd: RDD) -> RDD:



        metrics_mapping = {
            'edit_distance': Levenshtein(),
            'cosine': Cosine(),
            'jaro': Jaro(),
            'jaccard': Jaccard(),
            'generalized_jaccard': GeneralizedJaccard(),
            'dice': Dice(),
            'overlap_coefficient': OverlapCoefficient(),
        }

        set_metrics = [
            'cosine', 'dice', 'generalized_jaccard', 'jaccard', 'overlap_coefficient'
        ]
        set_metrics_bc = self.sc.broadcast(set_metrics)
        metrics_mapping_bc = self.sc.broadcast(metrics_mapping)
        if self.vectorizer is not None:

            global frequency_evaluator_bc
            
            frequency_evaluator = self.frequency_evaluator
            frequency_evaluator_bc = self.sc.broadcast(frequency_evaluator)

            def _similarity_fun(entity):
                similarity: float = 0.0
                entity_id1, entity_id2 = entity
                similarity = frequency_evaluator_bc.value.predict(entity_id1, entity_id2)
                # _insert_to_graph(entity_id1, entity_id2, similarity)
                return entity_id1, entity_id2, {'weight': similarity }

            return rdd.map(_similarity_fun)
        elif isinstance(self.attributes, dict):
            data_entities = self.data.entities.iloc
            data_entities_bc = self.sc.broadcast(data_entities)
            
            attributes_items = self.attributes.items()
            attributes_items_bc = self.sc.broadcast(attributes_items)
            
            _metric = self._metric
            _metric_bc = self.sc.broadcast(_metric)
            

            _tokenizer = self._tokenizer
            _tokenizer_bc = self.sc.broadcast(_tokenizer)

            def _similarity_fun(entity):
                similarity: float = 0.0
                entity_id1, entity_id2 = entity
                for attribute, weight in attributes_items_bc.value:
                    e1 = data_entities_bc.value[entity_id1][attribute].lower()
                    e2 = data_entities_bc.value[entity_id2][attribute].lower()

                    similarity += weight * metrics_mapping_bc.value[_metric_bc.value].get_sim_score(
                        _tokenizer_bc.value.tokenize(e1) if _metric_bc.value in set_metrics_bc.value else e1,
                        _tokenizer_bc.value.tokenize(e2) if _metric_bc.value in set_metrics.value else e2
                    )
                # _insert_to_graph(entity_id1, entity_id2, similarity)
                return entity_id1, entity_id2, {'weight': similarity }

            return rdd.map(_similarity_fun)

        if isinstance(self.attributes, list):
            data_entities = self.data.entities.iloc
            data_entities_bc = self.sc.broadcast(data_entities)
            
            attributes = self.attributes
            attributes_bc = self.sc.broadcast(attributes)
            
            _metric = self._metric
            _metric_bc = self.sc.broadcast(_metric)
            

            _tokenizer = self._tokenizer
            _tokenizer_bc = self.sc.broadcast(_tokenizer)


            def _similarity_fun(entity):
                similarity: float = 0.0
                entity_id1, entity_id2 = entity
                for attribute in attributes_bc.value:
                    e1 = data_entities_bc.value[entity_id1][attribute].lower()
                    e2 = data_entities_bc.value[entity_id2][attribute].lower()

                    similarity += metrics_mapping_bc.value[_metric_bc.value].get_sim_score(
                        _tokenizer_bc.value.tokenize(e1) if _metric_bc.value in set_metrics_bc.value else e1,
                        _tokenizer_bc.value.tokenize(e2) if _metric_bc.value in set_metrics_bc.value else e2
                    )
                    similarity /= len(attributes_bc.value)
                # _insert_to_graph(entity_id1, entity_id2, similarity)
                return entity_id1, entity_id2, {'weight': similarity }

            return rdd.map(_similarity_fun)
        else:
            data_entities = self.data.entities.iloc
            data_entities_bc = self.sc.broadcast(data_entities)
            
            attributes = self.attributes
            attributes_bc = self.sc.broadcast(attributes)
            
            _metric = self._metric
            _metric_bc = self.sc.broadcast(_metric)
            

            _tokenizer = self._tokenizer
            _tokenizer_bc = self.sc.broadcast(_tokenizer)

            # concatenated row string
            def _similarity_fun(entity):
                entity_id1, entity_id2 = entity
                similarity: float = 0.0
                e1 = data_entities_bc.value[entity_id1].str.cat(sep=' ').lower()
                e2 = data_entities_bc.value[entity_id2].str.cat(sep=' ').lower()
                te1 = _tokenizer_bc.value.tokenize(e1) if _metric_bc.value in set_metrics_bc.value else e1
                te2 = _tokenizer_bc.value.tokenize(e2) if _metric_bc.value in set_metrics_bc.value else e2
                similarity = metrics_mapping_bc.value[_metric_bc.value].get_sim_score(te1, te2)
                # _insert_to_graph(entity_id1, entity_id2, similarity)
                return entity_id1, entity_id2, {'weight': similarity }

            return rdd.map(_similarity_fun)

    #
    # def _insert_to_graph(self, rdd : RDD) -> RDD:
    #     if self.similarity_threshold is None or \
    #         (self.similarity_threshold is not None and similarity > self.similarity_threshold):
    #         self.pairs.add_edge(entity_id1, entity_id2, weight=similarity)
    #
    #     return rdd