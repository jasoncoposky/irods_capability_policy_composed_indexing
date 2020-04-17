
#include "utilities.hpp"

#include "policy_engine_configuration_manager.hpp"

#include "cpr/response.h"
#include "elasticlient/client.h"
#include "elasticlient/bulk.h"
#include "elasticlient/logging.h"

namespace {
    namespace pe   = irods::policy_engine;
    namespace idx  = irods::indexing;
    namespace fs   = irods::experimental::filesystem;
    namespace fsvr = irods::experimental::filesystem::server;

    irods::error purge_metadata(
          ruleExecInfo_t*       rei
        , elasticlient::Client& client
        , const std::string&    object_path
        , const std::string&    index_name
        , const std::string&    attribute
        , const std::string&    value
        , const std::string&    units) {

        const std::string md_index_id{
                              idx::get_metadata_index_id(
                                  idx::get_object_index_id(
                                      rei,
                                      object_path),
                                  attribute,
                                  value,
                                  units)};
        const cpr::Response response = client.remove(index_name, "text", md_index_id);
        if(response.status_code != 200 && response.status_code != 201) {
            return ERROR(
                SYS_INTERNAL_ERR,
                boost::format("failed to purge metadata [%s] [%s] [%s] for [%s] code [%d] message [%s]")
                % attribute
                % value
                % units
                % object_path
                % response.status_code
                % response.text);
        }

        return SUCCESS();

    } // purge_metadata

    irods::error purge_metadata_for_object(
          ruleExecInfo_t*       rei
        , elasticlient::Client& client
        , const std::string&    object_path
        , const std::string&    index_name) {

        irods::error last_error{};
        for(auto&& avu : fsvr::get_metadata(*rei->rsComm, object_path)) {
            auto err = purge_metadata(
                             rei
                           , client
                           , object_path
                           , index_name
                           , avu.attribute
                           , avu.value
                           , avu.units);
            if(!err.ok()) {
                last_error = err;
            }
        } // for avu

        return last_error;

    } // purge_metadata_for_object

    irods::error metadata_purge_elasticsearch(const pe::context& ctx)
    {
        auto [err, index_name] = idx::get_index_name(ctx.parameters);
        if(!err.ok()) {
            return err;
        }

        pe::configuration_manager cfg_mgr{ctx.instance_name, ctx.configuration};

        std::vector<std::string> hosts{};
        std::tie(err, hosts) = cfg_mgr.get_value("hosts", hosts);
        elasticlient::Client client{hosts};

        std::string user_name{}, object_path{}, source_resource{}, destination_resource{};
        std::tie(user_name, object_path, source_resource, destination_resource) =
            irods::capture_parameters(ctx.parameters, irods::tag_first_resc);

        const std::string event{ctx.parameters["event"]};

        if("METADATA" == event) {
            const std::string attribute{ctx.parameters["attribute"]}
                            , value{ctx.parameters["value"]}
                            , units{ctx.parameters["units"]}
                            , operation{ctx.parameters["operation"]};
            if("rm" != operation) {
                return SUCCESS();
            }

            return purge_metadata(
                         ctx.rei
                       , client
                       , object_path
                       , index_name
                       , attribute
                       , value
                       , units);
        }
        else {
            return purge_metadata_for_object(
                         ctx.rei
                       , client
                       , object_path
                       , index_name);
        }

        return SUCCESS();

    } // metadata_purge_elasticsearch
} // namespace

const char usage[] = R"(
{
    "id": "file:///var/lib/irods/configuration_schemas/v3/policy_engine_usage.json",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "type": "object",
    "properties": {
        "description": {"type": "string"},
        "input_interfaces": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "enum": [
                        "event_handler-collection_modified",
                        "event_handler-data_object_modified",
                        "event_handler-metadata_modified",
                        "event_handler-user_modified",
                        "event_handler-resource_modified",
                        "direct_invocation",
                        "query_results"
                    ]},
                    "description": {"type": "string"},
                    "json_schema": {"type": "string"}
                },
                "required": ["name","description","json_schema"]
            }
        },
        "output_json_for_validation": {"type": "string"}
    },
    "required": [
        "description",
        "input_interfaces",
        "output_json_for_validation"
    ]
}
)";

extern "C"
pe::plugin_pointer_type plugin_factory(
      const std::string& _plugin_name
    , const std::string&) {

    return pe::make(
                 _plugin_name
               , "irods_policy_indexing_metadata_purge_elasticsearch"
               , usage
               , metadata_purge_elasticsearch);

} // plugin_factory
