
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
        , const std::string&    units
        , const bool            log_verbose) {

        try {
            const std::string md_index_id{
                                  idx::get_metadata_index_id(
                                      idx::get_object_index_id(
                                          rei,
                                          object_path),
                                      attribute,
                                      value,
                                      units)};

            const cpr::Response response = client.remove(index_name, "text", md_index_id);

                if(log_verbose) {
                    rodsLog(
                        LOG_NOTICE
                      , "response code [%d] response text [%s]"
                      , response.status_code
                      , response.text.c_str());
                }

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
        }
        catch(const irods::exception& e) {
            return ERROR(e.code(), e.what());
        }

    } // purge_metadata

    irods::error purge_metadata_for_object(
          ruleExecInfo_t*       rei
        , elasticlient::Client& client
        , const std::string&    object_path
        , const std::string&    index_name
        , const bool            log_verbose) {

        irods::error last_error{};
        for(auto&& avu : fsvr::get_metadata(*rei->rsComm, object_path)) {
            auto err = purge_metadata(
                             rei
                           , client
                           , object_path
                           , index_name
                           , avu.attribute
                           , avu.value
                           , avu.units
                           , log_verbose);
            if(!err.ok()) {
                last_error = err;
            }
        } // for avu

        return last_error;

    } // purge_metadata_for_object

    namespace idx = irods::indexing;

    irods::error metadata_purge_elasticsearch(const pe::context& ctx)
    {
        auto [err, index_name] = idx::get_index_name(ctx.parameters);
        if(!err.ok()) {
            return err;
        }

        pe::configuration_manager cfg_mgr{ctx.instance_name, ctx.configuration};

        std::vector<std::string> hosts{};
        std::tie(err, hosts) = cfg_mgr.get_value("hosts", hosts);

        std::string log_verbose_param{};
        std::tie(err, log_verbose_param) = cfg_mgr.get_value("log_errors", "false");
        const bool log_verbose{"true" == log_verbose_param};

        elasticlient::Client client{hosts};

        std::string user_name{}, object_path{}, source_resource{}, destination_resource{};
        std::tie(user_name, object_path, source_resource, destination_resource) =
            irods::capture_parameters(ctx.parameters, irods::tag_first_resc);

        if(!ctx.parameters.contains("event")) {
            return ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       "parameters do not include an event");
        }

        const std::string event{ctx.parameters.at("event")};

        if("METADATA" == event) {
            const auto [err, operation, attribute, value, units] =
                idx::extract_metadata_parameters(ctx.parameters);
            if(!err.ok()) {
                return err;
            }

            if(!operation.empty() && "rm" != operation) {
                return SUCCESS();
            }

            return purge_metadata(
                         ctx.rei
                       , client
                       , object_path
                       , index_name
                       , attribute
                       , value
                       , units
                       , log_verbose);
        }
        else {
            return purge_metadata_for_object(
                         ctx.rei
                       , client
                       , object_path
                       , index_name
                       , log_verbose);
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
