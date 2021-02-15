# irods_capability_policy_composed_indexing

iRODS Rule Engine Plugin Policy Engines designed to work with the [Event Handler Framework](https://github.com/jasoncoposky/irods_rule_engine_plugins_policy) for full text and metadata indexing with Elastic Search.

## Rule Engine Plugin Configuration
### Metadata Indexing
```
    {
        {
            "instance_name": "irods_rule_engine_plugin-event_handler-metadata_modified-instance",
            "plugin_name": "irods_rule_engine_plugin-event_handler-metadata_modified",
            'plugin_specific_configuration': {
                "policies_to_invoke" : [
                {
                    "active_policy_clauses" : ["post"],
                    "events" : ["metadata"],
                    "conditional" : {
                        "metadata_exists" : {
                            "recursive" : "true",
                            "attribute" : "irods::indexing::index",
                            "entity_type" : "data_object"
                        },
                    },
                    "policy_to_invoke"    : "irods_policy_indexing_metadata_index_elasticsearch",
                    "configuration" : {
                        "hosts" : ["http://localhost:9200/"],
                        "bulk_count" : 100,
                        "read_size" : 4194304
                    }

                },
                {
                    "active_policy_clauses" : ["post"],
                    "events" : ["metadata"],
                    "conditional" : {
                        "metadata_exists" : {
                            "recursive" : "true",
                            "attribute" : "irods::indexing::index",
                            "entity_type" : "collection"
                        },
                    },
                    "policy_to_invoke"    : "irods_policy_indexing_metadata_index_elasticsearch",
                    "configuration" : {
                        "hosts" : ["http://localhost:9200/"],
                        "bulk_count" : 100,
                        "read_size" : 4194304
                    }

                },
                {
                    "active_policy_clauses" : ["post"],
                    "events" : ["metadata"],
                    "conditional" : {
                        "metadata_applied" : {
                            "attribute" : "irods::indexing::index",
                            "entity_type" : "collection",
                            "operation" : ["set", "add"]
                        }
                    },
                    "policy_to_invoke" : "irods_policy_query_processor",
                    "parameters" : {
                        "query_string" : "SELECT USER_NAME, COLL_NAME, DATA_NAME, RESC_NAME WHERE COLL_NAME = 'IRODS_TOKEN_COLLECTION_NAME_END_TOKEN'",
                        "query_limit" : 0,
                        "query_type" : "general",
                        "number_of_threads" : 1,
                        "policy_to_invoke" : "irods_policy_indexing_metadata_index_elasticsearch",
                        "configuration" : {
                            "hosts" : ["http://localhost:9200/"],
                            "bulk_count" : 100,
                            "read_size" : 4194304
                        }
                    }
                }
                ]
            }
        },
        {
            "instance_name": "irods_rule_engine_plugin-policy_engine-query_processor-instance",
            "plugin_name": "irods_rule_engine_plugin-policy_engine-query_processor",
            "plugin_specific_configuration": {
                "log_errors" : "true"
            }
        },
        {
            "instance_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_index_metadata-instance",
            "plugin_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_index_metadata",
            "plugin_specific_configuration": {
                "log_errors" : "true"
            }
        }
    }
```

### Full Text Indexing

```
    {
        {
            "instance_name": "irods_rule_engine_plugin-event_handler-metadata_modified-instance",
            "plugin_name": "irods_rule_engine_plugin-event_handler-metadata_modified",
            'plugin_specific_configuration': {
                "policies_to_invoke" : [
                {
                    "active_policy_clauses" : ["post"],
                    "events" : ["metadata"],
                    "conditional" : {
                        "metadata_applied" : {
                            "attribute"   : "irods::indexing::index",
                            "entity_type" : "collection",
                            "operation"   : ["rm"]
                        }
                    },
                    "policy_to_invoke" : "irods_policy_query_processor",
                    "parameters" : {
                        "query_string" : "SELECT USER_NAME, COLL_NAME, DATA_NAME, RESC_NAME WHERE COLL_NAME = 'IRODS_TOKEN_COLLECTION_NAME_END_TOKEN'",
                        "query_limit" : 0,
                        "query_type" : "general",
                        "number_of_threads" : 1,
                        "policy_to_invoke" : "irods_policy_indexing_full_text_purge_elasticsearch",
                        "configuration" : {
                            "hosts" : ["http://localhost:9200/"],
                            "bulk_count" : 100,
                            "read_size" : 1024
                        }
                    }
                },
                {
                    "active_policy_clauses" : ["post"],
                    "events" : ["metadata"],
                    "conditional" : {
                        "metadata_applied" : {
                            "attribute"   : "irods::indexing::index",
                            "entity_type" : "collection",
                            "operation"   : ["add", "set"]
                        }
                    },
                    "policy_to_invoke" : "irods_policy_query_processor",
                    "parameters" : {
                        "query_string" : "SELECT USER_NAME, COLL_NAME, DATA_NAME, RESC_NAME WHERE COLL_NAME = 'IRODS_TOKEN_COLLECTION_NAME_END_TOKEN'",
                        "query_limit" : 0,
                        "query_type" : "general",
                        "number_of_threads" : 1,
                        "policy_to_invoke" : "irods_policy_indexing_full_text_index_elasticsearch",
                        "configuration" : {
                            "hosts" : ["http://localhost:9200/"],
                            "bulk_count" : 100,
                            "read_size" : 1024
                        }
                    }
                }

                ]
            }
        },
        {
            "instance_name": "irods_rule_engine_plugin-policy_engine-query_processor-instance",
            "plugin_name": "irods_rule_engine_plugin-policy_engine-query_processor",
            "plugin_specific_configuration": {
                "log_errors" : "true"
            }
        },
        {
            "instance_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_index_fulltext-instance",
            "plugin_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_index_fulltext",
            "plugin_specific_configuration": {
                "log_errors" : "true"
            }
        },
        {
            "instance_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_purge_fulltext-instance",
            "plugin_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_purge_fulltext",
            "plugin_specific_configuration": {
                "log_errors" : "true"
            }
        }
    }
```
