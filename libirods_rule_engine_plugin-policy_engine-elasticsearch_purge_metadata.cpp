
#include "utilities.hpp"

#include "policy_composition_framework_configuration_manager.hpp"
#include "policy_composition_framework_parameter_capture.hpp"

#include "cpr/response.h"
#include "elasticlient/client.h"
#include "elasticlient/bulk.h"
#include "elasticlient/logging.h"

namespace {
    namespace pe   = irods::policy_composition::policy_engine;
    namespace idx  = irods::indexing;
    namespace fs   = irods::experimental::filesystem;
    namespace fsvr = irods::experimental::filesystem::server;

    irods::error purge_metadata(
          rsComm_t*             comm
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
                                      idx::get_id_for_logical_path(
                                          comm,
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
          rsComm_t*             comm
        , elasticlient::Client& client
        , const std::string&    object_path
        , const std::string&    index_name
        , const bool            log_verbose) {

        irods::error last_error = SUCCESS();

        for(auto&& avu : fsvr::get_metadata(*comm, object_path)) {
            auto err = purge_metadata(
                             comm
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

    irods::error metadata_purge_elasticsearch(const pe::context& ctx, pe::arg_type out)
    {
        idx::throw_if_metadata_is_missing(ctx.parameters);

        idx::throw_if_conditional_metadata_is_missing(ctx.parameters);

        if(idx::event_is_invalid(ctx.parameters, {"METADATA"})) {
            return SUCCESS();
        }

        // clang-format off
        const auto cfg   = pe::configuration_manager{ctx.instance_name, ctx.configuration};
        const auto hosts = cfg.get("hosts", std::vector<std::string>{});
        const auto verb  = std::string{"true"} == cfg.get(std::string{"log_errors"}, std::string{"false"});
        // clang-format on

        auto [u, logical_path, sr, dr] =
            capture_parameters(ctx.parameters, tag_first_resc);

        const auto index_name = idx::get_index_name(ctx.parameters);

        const auto [attribute, value, units, operation, entity, entity_type] =
            idx::extract_all(ctx.parameters.at("metadata"));

        elasticlient::Client client{hosts};

        if("data_object" == entity_type) {
            if(!operation.empty() && "rm" != operation) {
                return SUCCESS();
            }

            return purge_metadata(
                         ctx.rei->rsComm
                       , client
                       , logical_path
                       , index_name
                       , attribute
                       , value
                       , units
                       , verb);
        }
        else if("collection" == entity_type) {
            return purge_metadata_for_object(
                         ctx.rei->rsComm
                       , client
                       , logical_path
                       , index_name
                       , verb);
        }
        else {
            return ERROR(
                       SYS_NOT_SUPPORTED,
                       fmt::format("%s - entity_type is not supported [%s]"
                       , __FUNCTION__
                       , entity_type));
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
