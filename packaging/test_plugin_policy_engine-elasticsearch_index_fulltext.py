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
def index_event_handler_configured(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-event_handler-data_object_modified-instance",
                "plugin_name": "irods_rule_engine_plugin-event_handler-data_object_modified",
                'plugin_specific_configuration': {
                    "policies_to_invoke" : [
                        {
                            "active_policy_clauses" : ["post"],
                            "events" : ["put", "write"],
                            "conditional" : {
                                "metadata_exists" : {
                                    "recursive" : "true",
                                    "attribute" : "irods::indexing::index",
                                    "entity_type" : "collection"
                                }
                            },
                            "policy_to_invoke" : "irods_policy_indexing_full_text_index_elasticsearch",
                            "configuration" : {
                                    "hosts" : ["http://localhost:9200/"],
                                    "bulk_count" : 100,
                                    "read_size" : 1024
                            }
                        }
                    ]
                }
            }
        )

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
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
                                     "operation"   : ["set", "add"]
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
                "instance_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_index_fulltext-instance",
                "plugin_name": "irods_rule_engine_plugin-policy_engine-elasticsearch_index_fulltext",
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

curl_get_wildcard = """
curl -X GET -H'Content-Type: application/json' HTTP://localhost:9200/full_text_index/text/_search?pretty=true -d '
{
"from": 0, "size" : 500,
"_source" : ["logical_path", "data"],
    "query" : {
        "wildcard": {
            "logical_path": {
                "value": "*",
                "boost": 1.0,
                "rewrite": "constant_score"
            }
        }
    }
}'
"""

curl_delete = """
curl -X DELETE -H'Content-Type: application/json' HTTP://localhost:9200/full_text_index
"""

curl_create = """
curl -X PUT -H'Content-Type: application/json' http://localhost:9200/full_text_index
"""

curl_schema = """
curl -X PUT -H'Content-Type: application/json' http://localhost:9200/full_text_index/_mapping/text -d '{ "properties" : { "logical_path" : { "type" : "text" }, "data" : { "type" : "text" } } }'
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
            #print(out)
            sleep(1)
            continue
        else:
            #print(out)
            done = True

class TestElasticSearchIndexingFullText(ResourceBase, unittest.TestCase):
    def repave_index(self):
        output, _ = lib.execute_command(curl_delete)
        output, _ = lib.execute_command(curl_create)
        output, _ = lib.execute_command(curl_schema)

    def setUp(self):
        super(TestElasticSearchIndexingFullText, self).setUp()

    def tearDown(self):
        super(TestElasticSearchIndexingFullText, self).tearDown()

    def test_indexing_put_file(self):
        self.repave_index()
        with session.make_session_for_existing_admin() as admin_session:
            physical_path = '/var/lib/irods/scripts/irods/test/full_text_index_test_file.txt'
            logical_path  = '/tempZone/home/rods/full_text_index_test_file.txt'
            admin_session.assert_icommand('imeta set -C /tempZone/home irods::indexing::index full_text_index::full_text elasticsearch')

            try:
                with index_event_handler_configured():
                    admin_session.assert_icommand('iput -f ' + physical_path)

                assert_index_content('"logical_path" : "'+logical_path+'"')

            finally:
                admin_session.assert_icommand('imeta rm -C /tempZone/home irods::indexing::index full_text_index::full_text elasticsearch')
                admin_session.assert_icommand('irm -f ' + logical_path)
                admin_session.assert_icommand('iadmin rum')

    def test_indexing_full_collection(self):
        self.repave_index()
        with session.make_session_for_existing_admin() as admin_session:
            physical_path = '/var/lib/irods/scripts/irods/test/full_text_index_test_file.txt'
            logical_path0 = '/tempZone/home/rods/index_dir/file0'
            logical_path1 = '/tempZone/home/rods/index_dir/file1'
            logical_path2 = '/tempZone/home/rods/index_dir/file2'
            try:
                admin_session.assert_icommand('imkdir index_dir')
                admin_session.assert_icommand('iput -f ' + physical_path + ' index_dir/file0')
                admin_session.assert_icommand('iput -f ' + physical_path + ' index_dir/file1')
                admin_session.assert_icommand('iput -f ' + physical_path + ' index_dir/file2')

                admin_session.assert_icommand('ils -l index_dir', 'STDOUT_SINGLELINE', 'demoResc')
                with index_event_handler_configured():
                    admin_session.assert_icommand('imeta set -C /tempZone/home/rods/index_dir irods::indexing::index full_text_index::full_text elasticsearch')

                assert_index_content('"logical_path" : "'+logical_path0+'"')
                assert_index_content('"logical_path" : "'+logical_path1+'"')
                assert_index_content('"logical_path" : "'+logical_path2+'"')

            finally:
                admin_session.assert_icommand('imeta rm -C /tempZone/home/rods/index_dir irods::indexing::index full_text_index::full_text elasticsearch')
                admin_session.assert_icommand('irm -rf index_dir')
                admin_session.assert_icommand('iadmin rum')



