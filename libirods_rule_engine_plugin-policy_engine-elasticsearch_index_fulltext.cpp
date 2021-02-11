
#define IRODS_IO_TRANSPORT_ENABLE_SERVER_SIDE_API
#define IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API

#include "utilities.hpp"

#include "policy_composition_framework_configuration_manager.hpp"
#include "policy_composition_framework_parameter_capture.hpp"

#include "transport/default_transport.hpp"
#include "dstream.hpp"

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

    irods::error index_fulltext(
          rsComm_t*                             comm
        , std::shared_ptr<elasticlient::Client> client
        , const uint64_t                        read_size
        , const uint32_t                        bulk_count
        , const std::string&                    logical_path
        , const std::string&                    index_name
        , const bool                            log_verbose) {

        if(log_verbose) {
            rodsLog(
                LOG_NOTICE
              , "indexing full text in [%s] for path [%s]"
              , index_name.c_str()
              , logical_path.c_str());
        }

        const std::string object_id{idx::get_id_for_logical_path(comm, logical_path)};

        elasticlient::Bulk bulkIndexer(client);
        elasticlient::SameIndexBulkData bulk(index_name, bulk_count);

        auto file_size = fsvr::data_object_size(*comm, logical_path);

        // TODO:: compute bulk count vs read size
        auto foo_size{file_size / bulk_count};

        char read_buff[foo_size];
        irods::experimental::io::server::basic_transport<char> xport(*comm);
        irods::experimental::io::idstream ds{xport, logical_path};

        int chunk_counter{0};
        bool need_final_perform{false};
        while(ds) {
            ds.read(read_buff, foo_size);
            std::string data{read_buff};

            data.erase(
                std::remove_if(
                    data.begin(),
                    data.end(),
                [](wchar_t c) { return (std::iscntrl(c) ||
                                        c == '"'        ||
                                        c == '\''       ||
                                        c == '\\');}),
                    data.end());

            std::string cleaned{idx::correct_non_utf_8(&data)};

            std::string index_id{
                            fmt::format(
                            "{}{}{}"
                            , object_id
                            , idx::indexer_separator
                            , chunk_counter)};
            ++chunk_counter;

            std::string payload{
                            fmt::format(
                            "{{ \"logical_path\" : \"{}\", \"data\" : \"{}\" }}"
                            , logical_path
                            , cleaned)};

            need_final_perform = true;
            bool done = bulk.indexDocument("text", index_id, payload.data());
            if(done) {
                need_final_perform = false;
                // have reached bulk_count chunks
                auto error_count = bulkIndexer.perform(bulk);
                bulk.clear();
                if(error_count > 0) {
                    return ERROR(
                               SYS_INTERNAL_ERR,
                               fmt::format("Encountered {} errors when indexing [{}]"
                                , error_count
                                , logical_path));
                }
            }
        } // while

        if(need_final_perform) {
            auto error_count = bulkIndexer.perform(bulk);
            bulk.clear();
            if(error_count > 0) {
                return ERROR(
                           SYS_INTERNAL_ERR,
                           fmt::format("Encountered {} errors when indexing [{}]"
                            , error_count
                            , logical_path));
            }
        }

        return SUCCESS();
    } // index_fulltext

    void log_fcn(elasticlient::LogLevel lvl, const std::string& msg) {
        if(lvl == elasticlient::LogLevel::ERROR) {
            rodsLog(LOG_ERROR, "ELASTICLIENT :: [%s]", msg.c_str());
        }
    } // log_fcn

    irods::error full_text_index_elasticsearch(const pe::context& ctx, pe::arg_type out)
    {
        if(idx::event_is_invalid(ctx.parameters, {"put", "write", "metadata"})) {
            return SUCCESS();
        }

        // clang-format off
        const auto cfg_mgr     = pe::configuration_manager{ctx.instance_name, ctx.configuration};
        const auto event       = std::string{ctx.parameters.at("event")};
        const auto hosts       = cfg_mgr.get("hosts", std::vector<std::string>{});
        const auto log_verbose = std::string{"true"} == cfg_mgr.get("log_errors", std::string{"false"});
        const auto read_size   = cfg_mgr.get("read_size", uint64_t{4194304});
        const auto bulk_count  = cfg_mgr.get("bulk_count", uint32_t{100});
        const auto index_name  = idx::get_index_name(ctx.parameters);
        // clang-format on

        auto [un, logical_path, sr, dr] =
            capture_parameters(ctx.parameters, tag_first_resc);

        elasticlient::setLogFunction(log_fcn);

        std::shared_ptr<elasticlient::Client> client =
            std::make_shared<elasticlient::Client>(hosts);

        return index_fulltext(
                     ctx.rei->rsComm
                   , client
                   , read_size
                   , bulk_count
                   , logical_path
                   , index_name
                   , log_verbose);

        return SUCCESS();

    } // full_text_index_elasticsearch
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
               , "irods_policy_indexing_full_text_index_elasticsearch"
               , usage
               , full_text_index_elasticsearch);

} // plugin_factory
