import os
import sys
import shutil
import contextlib
import tempfile
import json
import os.path
import pycurl

from time import sleep

if sys.version_info >= (2, 7):
    import unittest
else:
    import unittest2 as unittest

from ..configuration import IrodsConfig
from ..controller import IrodsController
from .resource_suite import ResourceBase
from ..test.command import assert_command
from . import session
from .. import test
from .. import paths
from .. import lib
import ustrings

@contextlib.contextmanager
def metadata_event_handler_configured(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
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
                                         "match" : {
                                             "metadata" : {
                                                 "attribute" : "irods::indexing::index",
                                                 "entity_type" : "data_object"
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
                         }
                         ,
                         {
                             "active_policy_clauses" : ["post"],
                             "events" : ["metadata"],
                             "match" : {
                                 "metadata" : {
                                     "attribute" : "irods::indexing::index",
                                     "entity_type" : "collection"
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
                         }
                     ]
                }
            }
        )

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
           {
                "instance_name": "irods_rule_engine_plugin-event_generator-object_metadata-instance",
                "plugin_name": "irods_rule_engine_plugin-event_generator-object_metadata",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
           }
        )

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
           {
                "instance_name": "irods_rule_engine_plugin-event_delegate-collection_metadata-instance",
                "plugin_name": "irods_rule_engine_plugin-event_delegate-collection_metadata",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
           }
        )


        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
           {
                "instance_name": "irods_rule_engine_plugin-policy_engine-query_processor-instance",
                "plugin_name": "irods_rule_engine_plugin-policy_engine-query_processor",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
           }
        )


        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
           {
                "instance_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_index_metadata-instance",
                "plugin_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_index_metadata",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
           }
        )

        irods_config.commit(irods_config.server_config, irods_config.server_config_path)

        IrodsController().restart()

        try:
            yield
        finally:
            pass

curl_get = """
curl -X GET -H'Content-Type: application/json' HTTP://localhost:9200/metadata_index/text/_search?pretty=true -d '
{
   "from": 0, "size" : 500,
   "_source" : ["logical_path", "attribute", "value", "units"],
   "query" : {
       "term" : {"attribute" : "a0"}
   }
}'
"""

curl_get_wildcard = """
curl -X GET -H'Content-Type: application/json' HTTP://localhost:9200/metadata_index/text/_search?pretty=true -d '
{
"from": 0, "size" : 500,
"_source" : ["logical_path", "attribute", "value", "units"],
    "query" : {
        "wildcard": {
            "attribute": {
                "value": "a*",
                "boost": 1.0,
                "rewrite": "constant_score"
            }
        }
    }
}'
"""

curl_delete = """
curl -X DELETE -H'Content-Type: application/json' HTTP://localhost:9200/metadata_index/
"""

curl_create = """
curl -X PUT -H'Content-Type: application/json' http://localhost:9200/metadata_index
"""

curl_schema = """
curl -X PUT -H'Content-Type: application/json' http://localhost:9200/metadata_index/_mapping/text -d '{ "properties" : { "logical_path" : { "type" : "text" }, "attribute" : { "type" : "text" }, "value" : { "type" : "text" }, "unit" : { "type" : "text" } } }'
"""

def assert_index_content(expected_output):
    max_iter = 10
    counter = 0
    done = False
    while done == False:
        out, _ = lib.execute_command(curl_get_wildcard)
        if -1 == out.find(expected_output):
            counter = counter + 1
            if(counter > max_iter):
                assert(False)
            sleep(1)
            continue
        else:
            print(out)
            done = True

class TestElasticSearchIndexingMetadata(ResourceBase, unittest.TestCase):
    def repave_index(self):
        output, _ = lib.execute_command(curl_delete)
        output, _ = lib.execute_command(curl_create)
        output, _ = lib.execute_command(curl_schema)

    def setUp(self):
        super(TestElasticSearchIndexingMetadata, self).setUp()

    def tearDown(self):
        super(TestElasticSearchIndexingMetadata, self).tearDown()

    def test_indexing_add_metadata(self):
        self.repave_index()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('imeta set -C /tempZone/home irods::indexing::index metadata_index::metadata elasticsearch')
            filename = 'test_put_file'
            lib.create_local_testfile(filename)
            admin_session.assert_icommand('iput ' + filename)

            try:
                with metadata_event_handler_configured():
                    admin_session.assert_icommand('imeta set -d ' + filename + ' a0 v0 u0')

                assert_index_content('"attribute" : "a0"')
                assert_index_content('"value" : "v0"')
                assert_index_content('"units" : "u0"')

            finally:
                admin_session.assert_icommand('imeta rm -C /tempZone/home irods::indexing::index metadata_index::metadata elasticsearch')
                admin_session.assert_icommand('irm -f ' + filename)
                admin_session.assert_icommand('iadmin rum')

    def test_indexing_full_collection(self):
        self.repave_index()
        with session.make_session_for_existing_admin() as admin_session:
            try:
                base_name = 'test_indexing_full_collection'
                local_dir = os.path.join('/tmp/test_elastic_search_indexing_metadata', base_name)
                dir1 = 'dir1'
                dir1path = os.path.join(local_dir, dir1)
                lib.make_dir_p(local_dir)
                lib.create_directory_of_small_files(dir1path,2)
                admin_session.assert_icommand('iput -fr ' + dir1path, 'STDOUT_SINGLELINE', 'Running recursive pre-scan')
                admin_session.assert_icommand('ils -l ' + dir1, 'STDOUT_SINGLELINE', 'demoResc')
                admin_session.assert_icommand('imeta set -d ' + dir1+'/0' + ' a0 v0 u0')
                with metadata_event_handler_configured():
                    admin_session.assert_icommand('imeta set -C /tempZone/home/rods/dir1 irods::indexing::index metadata_index::metadata elasticsearch')
                assert_index_content('"attribute" : "a0"')
                assert_index_content('"value" : "v0"')
                assert_index_content('"units" : "u0"')

            finally:
                admin_session.assert_icommand('irm -rf ' + dir1)
                admin_session.assert_icommand('iadmin rum')



