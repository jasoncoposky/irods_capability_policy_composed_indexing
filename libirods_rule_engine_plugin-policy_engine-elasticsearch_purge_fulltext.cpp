
#define IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API
#define IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API

#include "utilities.hpp"

#include "policy_composition_framework_configuration_manager.hpp"
#include "policy_composition_framework_parameter_capture.hpp"
#include "policy_composition_framework_keywords.hpp"

#include "transport/default_transport.hpp"
#include "dstream.hpp"

#include "cpr/response.h"
#include "elasticlient/client.h"
#include "elasticlient/logging.h"

#include "fmt/format.h"

namespace {
    namespace pe   = irods::policy_composition::policy_engine;
    namespace kw   = irods::policy_composition::keywords;
    namespace idx  = irods::indexing;
    namespace fs   = irods::experimental::filesystem;
    namespace fsvr = irods::experimental::filesystem::server;

    irods::error purge_fulltext(
          rsComm_t*                             comm
        , std::shared_ptr<elasticlient::Client> client
        , const std::string&                    logical_path
        , const std::string&                    index_name
        , const bool                            log_verbose) {

        if(log_verbose) {
            rodsLog(
                LOG_NOTICE
              , "purging full text in [%s] for path [%s]"
              , index_name.c_str()
              , logical_path.c_str());
        }

        uint64_t chunk_counter{};

        const std::string object_id{idx::get_id_for_logical_path(comm, logical_path)};

        bool done{false};
        while(!done) {
            std::string index_id{
                            fmt::format(
                            "{}{}{}"
                            , object_id
                            , idx::indexer_separator
                            , chunk_counter)};

            ++chunk_counter;

            const cpr::Response response = client->remove(index_name, "text", index_id);
            if(response.status_code != 200) {
                done = true;
            }

        } // while

        return SUCCESS();
    } // purge_fulltext

    void log_fcn(elasticlient::LogLevel lvl, const std::string& msg) {
        if(lvl == elasticlient::LogLevel::ERROR) {
            rodsLog(LOG_ERROR, "ELASTICLIENT :: [%s]", msg.c_str());
        }
    } // log_fcn

    irods::error full_text_purge_elasticsearch(const pe::context& ctx, pe::arg_type out)
    {
        if(idx::event_is_invalid(ctx.parameters, {"unlink", "unregister", "metadata"})) {
            return SUCCESS();
        }

        // clang-format off
        const auto cfg_mgr     = pe::configuration_manager{ctx.instance_name, ctx.configuration};
        const auto event       = std::string{ctx.parameters.at(kw::event)};
        const auto hosts       = cfg_mgr.get("hosts", std::vector<std::string>{});
        const auto log_verbose = std::string{"true"} == cfg_mgr.get(kw::log_errors, std::string{"false"});
        const auto index_name  = idx::get_index_name(ctx.parameters);
        // clang-format on

        auto [un, logical_path, sr, dr] =
            capture_parameters(ctx.parameters, tag_first_resc);

        std::shared_ptr<elasticlient::Client> client =
            std::make_shared<elasticlient::Client>(hosts);

        return purge_fulltext(
                     ctx.rei->rsComm
                   , client
                   , logical_path
                   , index_name
                   , log_verbose);

        return SUCCESS();

    } // full_text_purge_elasticsearch
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
               , "irods_policy_indexing_full_text_purge_elasticsearch"
               , usage
               , full_text_purge_elasticsearch);

} // plugin_factory
