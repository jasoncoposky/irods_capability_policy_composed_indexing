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
                            "match_metadata" : {
                                "attribute" : "irods::indexing::index"
                            },
                            "policy" : "irods_policy_query_processor",
                            "parameters" : {
                                  "query_string" : "SELECT USER_NAME, COLL_NAME, DATA_NAME, RESC_NAME WHERE COLL_NAME = 'IRODS_TOKEN_COLLECTION_NAME' AND DATA_NAME = 'IRODS_TOKEN_DATA_NAME'",
                                  "query_limit" : 1,
                                  "query_type" : "general",
                                  "number_of_threads" : 1,
                                  "policy_to_invoke" : "irods_policy_indexing_metadata_index_elasticsearch",
                                  "configuration" : {
                                      "source_resource" : "demoResc"
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
                                        "match_metadata" : {
                                            "attribute" : "irods::indexing::index"
                                        },
                                        "policy"    : "irods_policy_indexing_metadata_index_elasticsearch",
                                        "configuration" : {
                                            "hosts" : ["http://localhost:9200/"]
                                        }

                                    }
                                ]
                            }
                        }
                    ]
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

#  "{ \"object_path\":\"%s\", \"attribute\":\"%s\", \"value\":\"%s\", \"units\":\"%s\" }"
curl_command = """
curl -XPOST -H'Content-Type: applicatin/json'  HTTP://localhost:9200/metadata_index/text/ -d '{ "object_path" : "/tempZone/home/rods/test_put_file", "attribute" : "purge_attribute0", "value" : "purge_value0", "units" : "purge_units0" }'
"""


class TestElasticSearchIndexingMetadata(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestElasticSearchIndexingMetadata, self).setUp()

    def tearDown(self):
        super(TestElasticSearchIndexingMetadata, self).tearDown()

    def test_event_handler_metadata(self):
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('imeta set -C /tempZone/home irods::indexing::index metadata_index::metadata elasticsearch')
            filename = 'test_put_file'
            lib.create_local_testfile(filename)
            admin_session.assert_icommand('iput ' + filename)

            with metadata_event_handler_configured():
                try:
                    admin_session.assert_icommand('imeta set -d ' + filename + ' a0 v0 u0')
                    admin_session.assert_icommand('imeta set -d ' + filename + ' a1 v1 u1')
                    output, _ = lib.execute_command(curl_command)
                    assert(-1 != output.find('"attribute" : "a0"'))
                    assert(-1 != output.find('"value" : "v0"'))
                    assert(-1 != output.find('"units" : "u0"'))
                finally:
                    admin_session.assert_icommand('imeta rm -C /tempZone/home irods::indexing::index metadata_index::metadata elasticsearch')
                    admin_session.assert_icommand('irm -f ' + filename)
                    admin_session.assert_icommand('iadmin rum')



