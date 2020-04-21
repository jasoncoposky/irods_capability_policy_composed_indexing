# irods_capability_policy_composed_indexing
Application of policy composition to the iRODS Indexing Capability

```
            {
                "instance_name": "irods_rule_engine_plugin-event_handler-metadata_modified-instance",
                "plugin_name": "irods_rule_engine_plugin-event_handler-metadata_modified",
                'plugin_specific_configuration': {
                    "policies_to_invoke" : [
                       {
                             "active_policy_clauses" : ["post"],
                             "events" : ["metadata"],
                             "policy"    : "irods_policy_event_delegate_collection_metadata",
                             "configuration" : {
                                 "policies_to_invoke" : [
                                     {
                                         "conditional" : {
                                             "metadata" : {
                                                 "attribute"   : "irods::indexing::index",
                                                 "entity_type" : "data_object",
                                                 "operation"   : ["set", "add"]
                                             },
                                         },
                                         "policy"    : "irods_policy_indexing_metadata_index_elasticsearch",
                                         "configuration" : {
                                             "hosts" : ["http://localhost:9200/"],
                                             "bulk_count" : 100,
                                             "read_size" : 4194304
                                         }
                                     }
                                 ]
                             }
                         },
                         {
                             "active_policy_clauses" : ["post"],
                             "events" : ["metadata"],
                             "conditional" : {
                                 "metadata" : {
                                     "attribute"   : "irods::indexing::index",
                                     "entity_type" : "collection",
                                     "operation"   : ["set", "add"]
                                 }
                             },
                             "policy" : "irods_policy_query_processor",
                             "parameters" : {
                                   "query_string" : "SELECT USER_NAME, COLL_NAME, DATA_NAME, RESC_NAME WHERE COLL_NAME = 'IRODS_TOKEN_COLLECTION_NAME'",
                                   "query_limit" : 1,
                                   "query_type" : "general",
                                   "number_of_threads" : 1,
                                   "policy_to_invoke" : "irods_policy_event_generator_object_metadata",
                                   "configuration" : {
                                       "policies_to_invoke" : [
                                           {
                                               "policy" : "irods_policy_indexing_metadata_index_elasticsearch",
                                               "configuration" : {
                                                        "hosts" : ["http://localhost:9200/"],
                                                        "bulk_count" : 100,
                                                        "read_size" : 4194304
                                               }
                                           }
                                       ]
                                   }
                             }
                         },
                         {
                             "active_policy_clauses" : ["post"],
                             "events" : ["metadata"],
                             "policy"    : "irods_policy_event_delegate_collection_metadata",
                             "configuration" : {
                                 "policies_to_invoke" : [
                                     {
                                         "conditional" : {
                                             "metadata" : {
                                                 "attribute"   : "irods::indexing::index",
                                                 "entity_type" : "data_object",
                                                 "operation"   : ["rm"]
                                             },
                                         },
                                         "policy"    : "irods_policy_indexing_metadata_purge_elasticsearch",
                                         "configuration" : {
                                             "hosts" : ["http://localhost:9200/"],
                                             "bulk_count" : 100,
                                             "read_size" : 4194304
                                         }
                                     }
                                 ]
                             }
                         },
                         {
                             "active_policy_clauses" : ["post"],
                             "events" : ["metadata"],
                             "conditional" : {
                                 "metadata" : {
                                     "attribute"   : "irods::indexing::index",
                                     "entity_type" : "collection",
                                     "operation"   : ["rm"]
                                 }
                             },
                             "policy" : "irods_policy_query_processor",
                             "parameters" : {
                                   "query_string" : "SELECT USER_NAME, COLL_NAME, DATA_NAME, RESC_NAME WHERE COLL_NAME = 'IRODS_TOKEN_COLLECTION_NAME'",
                                   "query_limit" : 1,
                                   "query_type" : "general",
                                   "number_of_threads" : 1,
                                   "policy_to_invoke" : "irods_policy_event_generator_object_metadata",
                                   "configuration" : {
                                       "policies_to_invoke" : [
                                           {
                                               "policy" : "irods_policy_indexing_metadata_purge_elasticsearch",
                                               "configuration" : {
                                                        "hosts" : ["http://localhost:9200/"],
                                                        "bulk_count" : 100,
                                                        "read_size" : 4194304
                                               }
                                           }
                                       ]
                                   }
                             }
                         }

                     ]
                }
            },
           {
                "instance_name": "irods_rule_engine_plugin-event_generator-object_metadata-instance",
                "plugin_name": "irods_rule_engine_plugin-event_generator-object_metadata",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
           },
           {
                "instance_name": "irods_rule_engine_plugin-event_delegate-collection_metadata-instance",
                "plugin_name": "irods_rule_engine_plugin-event_delegate-collection_metadata",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
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
           },
           {
                "instance_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_purge_metadata-instance",
                "plugin_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_purge_metadata",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
           }
```
