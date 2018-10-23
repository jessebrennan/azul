#!/usr/bin/python

import json
import difflib
import logging.config
import unittest
from urllib.parse import urlparse, parse_qs
from service import WebServiceTestCase
from azul.service.responseobjects.elastic_request_builder import ElasticTransformDump as EsTd
from azul import config

logger = logging.getLogger(__name__)


class TestRequestBuilder(WebServiceTestCase):
    request_config = {
        "translation": {
            "entity_id": "entity_id",
            "entity_version": "entity_version",
            "projectId": "contents.projects.document_id",
            "libraryConstructionApproach": "contents.processes.library_construction_approach",
            "disease": "contents.specimens.disease",
            "donorId": "contents.specimens.donor_biomaterial_id",
            "genusSpecies": "contents.specimens.genus_species"
        },
        "autocomplete-translation": {
            "files": {
                "entity_id": "entity_id",
                "entity_version": "entity_version"
            },
            "donor": {
                "donor": "donor_uuid"
            }
        },
        "manifest": [
            "File ID:Version",
            "Assay Id",
            "Analysis Id",
            "Project Id"
        ],
        "facets": [
        ]
    }

    @staticmethod
    def compare_dicts(actual_output, expected_output):
        """"Print the two outputs along with a diff of the two"""
        print("Comparing the two dictionaries built.")
        print('{}... => {}...'.format(actual_output[:20], expected_output[:20]))
        for i, s in enumerate(difflib.ndiff(actual_output, expected_output)):
            if s[0] == ' ':
                continue
            elif s[0] == '-':
                print(u'Delete "{}" from position {}'.format(s[-1], i))
            elif s[0] == '+':
                print(u'Add "{}" to position {}'.format(s[-1], i))

    def test_create_request(self):
        """
        Tests creation of a simple request
        :return: True or False depending on the assertion
        """
        # Load files required for this test
        expected_output = {
            "post_filter": {
                "bool": {
                    "must": [
                        {
                            "constant_score": {
                                "filter": {
                                    "terms": {
                                        "entity_id.keyword": [
                                            "cbb998ce-ddaf-34fa-e163-d14b399c6b34"
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "query": {
                "match_all": {}
            }
        }
        # Create a simple filter to test on
        sample_filter = {"entity_id": {"is": ["cbb998ce-ddaf-34fa-e163-d14b399c6b34"]}}
        # Need to work on a couple cases:
        # - The empty case
        # - The 1 filter case
        # - The complex multiple filters case

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            self.request_config,
            post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            es_search.to_dict(),
            sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_empty(self):
        """
        Tests creation of an empty request. That is, no filter
        :return: True or false depending on the test
        """
        # Testing with default (that is, no) filter
        # Load files required for this test
        expected_output = {
            "query": {
                "bool": {}
            }
        }

        # Create empty filter
        # TODO: Need some form of handler for the query language
        sample_filter = {}
        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            self.request_config)
        # Convert objects to be compared to strings
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(es_search.to_dict(), sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_complex(self):
        """
        Tests creation of a complex request.
        :return: True or false depending on the test
        """
        # Testing with default (that is, no) filter
        # Load files required for this test
        expected_output = {
            "post_filter": {
                "bool": {
                    "must": [
                        {
                            "constant_score": {
                                "filter": {
                                    "terms": {
                                        "entity_id.keyword": [
                                            "cbb998ce-ddaf-34fa-e163-d14b399c6b34"
                                        ]
                                    }
                                }
                            }
                        },
                        {
                            "constant_score": {
                                "filter": {
                                    "terms": {
                                        "entity_version.keyword": [
                                            "1993-07-19T23:50:09"
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "query": {
                "match_all": {}
            }
        }

        # Create sample filter
        sample_filter = {
            "entity_id":
                {
                    "is": ["cbb998ce-ddaf-34fa-e163-d14b399c6b34"]
                },
            "entity_version":
                {
                    "is": ["1993-07-19T23:50:09"]
                }
        }

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            self.request_config,
            post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(es_search.to_dict(), sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_missing_values(self):
        """
        Tests creation of a request for facets that do not have a value
        """
        # Load files required for this test
        request_config = self.request_config
        expected_output = {
            "post_filter": {
                "bool": {
                    "must": [
                        {
                            "constant_score": {
                                "filter": {
                                    "bool": {
                                        "must_not": [
                                            {
                                                "exists": {
                                                    "field": "entity_id.keyword"
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "query": {
                "match_all": {}
            }
        }

        # Create a filter for missing values
        sample_filter = {"entity_id": {"is": None}}

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            request_config,
            post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            es_search.to_dict(),
            sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_terms_and_missing_values(self):
        """
        Tests creation of a request for a combination of facets that do and do not have a value
        """
        # Load files required for this test
        expected_output = {
            "post_filter": {
                "bool": {
                    "must": [
                        {
                            "constant_score": {
                                "filter": {
                                    "bool": {
                                        "must_not": [
                                            {
                                                "exists": {
                                                    "field": "term1.keyword"
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        },
                        {
                            "constant_score": {
                                "filter": {
                                    "terms": {
                                        "term2.keyword": [
                                            "test"
                                        ]
                                    }
                                }
                            }
                        },
                        {
                            "constant_score": {
                                "filter": {
                                    "bool": {
                                        "must_not": [
                                            {
                                                "exists": {
                                                    "field": "term3.keyword"
                                                }
                                            }
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "query": {
                "match_all": {}
            }
        }

        # Create a filter for missing values
        sample_filter = {
            "term1": {"is": None},
            "term2": {"is": ["test"]},
            "term3": {"is": None},
        }

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            self.request_config,
            post_filter=True)
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            es_search.to_dict(),
            sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_aggregate(self):
        """
        Tests creation of an ES aggregate
        """
        expected_output = {
            "filter": {
                "bool": {}
            },
            "aggs": {
                "myTerms": {
                    "terms": {
                        "field": "facet1.translation.keyword",
                        "size": 99999
                    }
                },
                "untagged": {
                    "missing": {
                        "field": "facet1.translation.keyword"
                    }
                }
            }
        }

        sample_filter = {}

        # Create a request object
        agg_field = 'facet1'
        aggregation = EsTd.create_aggregate(
            sample_filter,
            facet_config={agg_field: f'{agg_field}.translation'},
            agg=agg_field
        )
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            aggregation.to_dict(),
            sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        # Testing first case with 1 filter
        self.assertEqual(actual_output, expected_output)

    def test_create_request_projects(self):
        """
        Test creation of a projects index request
        Request should have _project aggregations containing project_id buckets at the top level
        and sub-aggregations within each project bucket
        """
        # Load files required for this test
        expected_output = {
            "post_filter": {
                "bool": {
                    "must": [
                        {
                            "constant_score": {
                                "filter": {
                                    "terms": {
                                        "entity_id.keyword": [
                                            "cbb998ce-ddaf-34fa-e163-d14b399c6b34"
                                        ]
                                    }
                                }
                            }
                        }
                    ]
                }
            },
            "query": {
                "match_all": {}
            },
            "aggs": {
                "_project_agg": {
                    "terms": {
                        "field": "contents.projects.document_id.keyword",
                        "size": 99999
                    },
                    "aggs": {
                        "donor_count": {
                            "cardinality": {
                                "field": "contents.specimens.donor_document_id.keyword",
                                "precision_threshold": "40000"
                            }
                        },
                        "species": {
                            "terms": {
                                "field": "contents.specimens.genus_species.keyword"
                            }
                        },
                        "libraryConstructionApproach": {
                            "terms": {
                                "field": "contents.processes.library_construction_approach.keyword"
                            }
                        },
                        "disease": {
                            "terms": {
                                "field": "contents.specimens.disease.keyword"
                            }
                        }
                    }
                }
            }
        }

        sample_filter = {"entity_id": {"is": ["cbb998ce-ddaf-34fa-e163-d14b399c6b34"]}}

        # Create ElasticTransformDump instance
        es_ts_instance = EsTd()
        # Create a request object
        es_search = EsTd.create_request(
            sample_filter,
            es_ts_instance.es_client,
            self.request_config,
            post_filter=True,
            entity_type='projects')
        # Convert objects to be compared to strings
        expected_output = json.dumps(
            expected_output,
            sort_keys=True)
        actual_output = json.dumps(
            es_search.to_dict(),
            sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        self.assertEqual(actual_output, expected_output)

    def test_project_summaries(self):
        """
        Test creation of project summary
        Summary should be added to dict of corresponding project id in hits.
        """
        hits = [{'entryId': 'a'}, {'entryId': 'b'}]
        es_response = {
            "hits": {
                "hits": [
                    {
                        "_id": "a",
                        "_source": {
                            "entity_id": "a",
                            "contents": {
                                "specimens": [],
                                "files": [],
                                "processes": [],
                                "project": {
                                    "document_id": "a"
                                }
                            },
                            "bundles": [
                                {}
                            ]
                        }
                    },
                    {
                        "_id": "b",
                        "_source": {
                            "entity_id": "b",
                            "contents": {
                                "specimens": [
                                    {
                                        "biomaterial_id": [
                                            "specimen1"
                                        ],
                                        "disease": [
                                            "disease1"
                                        ],
                                        "organ": [
                                            "organ1"
                                        ],
                                        "total_estimated_cells": 2,
                                        "donor_biomaterial_id": [
                                            "donor1"
                                        ],
                                        "genus_species": [
                                            "species1"
                                        ]
                                    }
                                ],
                                "files": [],
                                "processes": [],
                                "project": {
                                    "document_id": "b"
                                }
                            },
                            "bundles": [
                                {}
                            ]
                        }
                    }
                ]
            },
            "aggregations": {
                "_project_agg": {
                    "doc_count_error_upper_bound": 0,
                    "sum_other_doc_count": 0,
                    "buckets": [
                        {
                            "key": "a",
                            "libraryConstructionApproach": {
                                "buckets": []
                            },
                            "disease": {
                                "buckets": []
                            },
                            "donor_count": {
                                "value": 0
                            },
                            "species": {
                                "buckets": []
                            }
                        },
                        {
                            "key": "b",
                            "libraryConstructionApproach": {
                                "buckets": []
                            },
                            "disease": {
                                "buckets": [
                                    {
                                        "key": "disease1",
                                        "doc_count": 1
                                    }
                                ]
                            },
                            "donor_count": {
                                "value": 1
                            },
                            "species": {
                                "buckets": [
                                    {
                                        "key": "species1",
                                        "doc_count": 1
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
        EsTd().add_project_summaries(hits, es_response)

        expected_output = [
            {
                "entryId": "a",
                "projectSummary": {
                    "donorCount": 0,
                    "totalCellCount": 0.0,
                    "organSummaries": [],
                    "genusSpecies": [],
                    "libraryConstructionApproach": [],
                    "disease": []
                }
            },
            {
                "entryId": "b",
                "projectSummary": {
                    "donorCount": 1,
                    "totalCellCount": 2.0,
                    "organSummaries": [
                        {
                            "organType": "organ1",
                            "countOfDocsWithOrganType": 1,
                            "totalCellCountByOrgan": 2.0
                        }
                    ],
                    "genusSpecies": [
                        "species1"
                    ],
                    "libraryConstructionApproach": [],
                    "disease": [
                        "disease1"
                    ]
                }
            }
        ]

        expected_output = json.dumps(expected_output, sort_keys=True)
        actual_output = json.dumps(hits, sort_keys=True)

        self.compare_dicts(actual_output, expected_output)

        self.assertEqual(actual_output, expected_output)

    def test_transform_request_with_file_url(self):
        response_json = EsTd().transform_request(filters={"file": {}},
                                                 pagination={'order': 'desc',
                                                             'size': 10,
                                                             'sort': 'specimenId'},
                                                 post_filter=True,
                                                 include_file_urls=True,
                                                 entity_type='files')
        bundle_files = [file_data for hit in response_json['hits'] for file_data in hit['files']]
        for file_data in bundle_files:
            self.assertIn('url', file_data.keys())
            actual_url = urlparse(file_data['url'])
            actual_query_vars = parse_qs(actual_url.query)
            expected_base_url = urlparse(config.dss_endpoint)
            self.assertEquals(expected_base_url.netloc, actual_url.netloc)
            self.assertEquals(expected_base_url.scheme, actual_url.scheme)
            self.assertIsNotNone(actual_url.path)
            self.assertEquals('aws', actual_query_vars['replica'][0])
            self.assertIsNotNone(actual_query_vars['version'][0])


if __name__ == '__main__':
    unittest.main()
