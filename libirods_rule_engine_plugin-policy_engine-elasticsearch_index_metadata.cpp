
#include "utilities.hpp"

#include "policy_composition_framework_configuration_manager.hpp"
#include "policy_composition_framework_parameter_capture.hpp"

#include "cpr/response.h"
#include "elasticlient/client.h"
#include "elasticlient/bulk.h"
#include "elasticlient/logging.h"

#include "fmt/format.h"

namespace {
    namespace pe   = irods::policy_composition::policy_engine;
    namespace idx  = irods::indexing;
    namespace fs   = irods::experimental::filesystem;
    namespace fsvr = irods::experimental::filesystem::server;

    irods::error index_metadata(
          rsComm_t*             comm
        , elasticlient::Client& client
        , const std::string&    logical_path
        , const std::string&    index_name
        , const std::string&    attribute
        , const std::string&    value
        , const std::string&    units
        , const bool            log_verbose) {

        if(log_verbose) {
            rodsLog(
                LOG_NOTICE
              , "indexing metadata [%s] [%s] [%s] in [%s] for path [%s]"
              , attribute.c_str()
              , value.c_str()
              , units.c_str()
              , index_name.c_str()
              , logical_path.c_str());
        }

        try {
            const std::string md_index_id{
                                  idx::get_metadata_index_id(
                                      idx::get_id_for_logical_path(
                                          comm,
                                          logical_path),
                                      attribute,
                                      value,
                                      units)};
            std::string payload{
                            fmt::format(
                            "{{ \"logical_path\":\"{}\", \"attribute\":\"{}\", \"value\":\"{}\", \"units\":\"{}\" }}"
                            , logical_path
                            , attribute
                            , value
                            , units)} ;

            const cpr::Response response = client.index(index_name, "text", md_index_id, payload);

            if(log_verbose) {
                rodsLog(
                    LOG_NOTICE
                  , "response code [%d] response text [%s]"
                  , response.status_code
                  , response.text.c_str());
            }

            if(response.status_code != 200 && response.status_code != 201) {
                return ERROR( SYS_INTERNAL_ERR,
                    fmt::format(
                        "failed to index metadata [{}] [{}] [{}] for [{}] code [{}] message [{}]"
                        , attribute
                        , value
                        , units
                        , logical_path
                        , response.status_code
                        , response.text));
            }

            return SUCCESS();
        }
        catch(const irods::exception& e) {
            rodsLog(
                LOG_ERROR,
                "%s exception caught [%d] [%s]",
                __FUNCTION__,
                e.code(),
                e.what());
            return ERROR(e.code(), e.what());
        }

    } // index_metadata

    irods::error index_metadata_for_object(
          rsComm_t*              comm
        , elasticlient::Client&  client
        , const std::string&     logical_path
        , const std::string&     index_name
        , const bool             log_verbose) {

        auto last_error = SUCCESS();

        for(auto&& avu : fsvr::get_metadata(*comm, logical_path)) {
            auto err = index_metadata(
                             comm
                           , client
                           , logical_path
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

    } // index_metadata_for_object

    irods::error metadata_index_elasticsearch(const pe::context& ctx, pe::arg_type out)
    {
        idx::throw_if_metadata_is_missing(ctx.parameters);

        idx::throw_if_conditional_metadata_is_missing(ctx.parameters);

        if(idx::event_is_invalid(ctx.parameters, {"METADATA"})) {
            return SUCCESS();
        }

        // clang-format off
        const auto cfg_mgr     = pe::configuration_manager{ctx.instance_name, ctx.configuration};
        const auto hosts       = cfg_mgr.get("hosts", std::vector<std::string>{});
        const auto log_verbose = std::string{"true"} == cfg_mgr.get(std::string{"log_errors"}, std::string{"false"});
        const auto is_idx_md   = idx::metadata_is_indexing(ctx.parameters.at("metadata"));
        const auto index_name  = idx::get_index_name(ctx.parameters);
        // clang-format on

        elasticlient::Client client{hosts};

        auto [u, logical_path, sr, dr] =
            capture_parameters(ctx.parameters, tag_first_resc);

        const auto [attribute, value, units, operation, entity, entity_type] =
            idx::extract_all(ctx.parameters.at("metadata"));

        if("set" != operation && "add" != operation) {
            return SUCCESS();
        }

        if("data_object" == entity_type
           || ("collection" == entity_type && !is_idx_md)) {
            // adding an individual avu to an object or collection
            return index_metadata(
                         ctx.rei->rsComm
                       , client
                       , logical_path
                       , index_name
                       , attribute
                       , value
                       , units
                       , log_verbose);
        }
        else if("collection" == entity_type && is_idx_md) {
            // annotated a collection to be indexed
            return index_metadata_for_object(
                         ctx.rei->rsComm
                       , client
                       , logical_path
                       , index_name
                       , log_verbose);
        }

        return SUCCESS();

    } // metadata_index_elasticsearch

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
               , "irods_policy_indexing_metadata_index_elasticsearch"
               , usage
               , metadata_index_elasticsearch);

} // plugin_factory
