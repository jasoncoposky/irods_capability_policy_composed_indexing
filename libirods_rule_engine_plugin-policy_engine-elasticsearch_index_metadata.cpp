
#include "policy_engine.hpp"
#include "policy_engine_configuration_manager.hpp"

#include "irods_hasher_factory.hpp"
#include "MD5Strategy.hpp"

#include "cpr/response.h"
#include "elasticlient/client.h"
#include "elasticlient/bulk.h"
#include "elasticlient/logging.h"

namespace {
    const std::string indexer_separator{"::"};

    namespace pe = irods::policy_engine;
    namespace fs = irods::experimental::filesystem;

    using json = nlohmann::json;

    std::string get_object_index_id(
        ruleExecInfo_t*    _rei,
        const std::string& _object_path) {


        fs::path p{_object_path};
        auto coll_name = p.parent_path().string();
        auto data_name = p.object_name().string();
        auto qstr {
            boost::str(
                boost::format("SELECT DATA_ID WHERE DATA_NAME = '%s' AND COLL_NAME = '%s'")
                    % data_name
                    % coll_name) };

        try {
            irods::query<rsComm_t> qobj{_rei->rsComm, qstr, 1};
            if(qobj.size() > 0) {
                return qobj.front()[0];
            }
            THROW(
                CAT_NO_ROWS_FOUND,
                boost::format("failed to get object id for [%s]")
                % _object_path);
        }
        catch(const irods::exception& _e) {
            THROW(
                CAT_NO_ROWS_FOUND,
                boost::format("failed to get object id for [%s]")
                % _object_path);
        }

    } // get_object_index_id


    std::string get_metadata_index_id(
        const std::string& _index_id,
        const std::string& _attribute,
        const std::string& _value,
        const std::string& _units) {

        std::string str = _attribute +
                          _value +
                          _units;
        irods::Hasher hasher;
        irods::getHasher( irods::MD5_NAME, hasher );
        hasher.update(str);

        std::string digest;
        hasher.digest(digest);

        return _index_id + indexer_separator + digest;

    } // get_metadata_index_id

    std::tuple<std::string, std::string>
    parse_indexer_string(
        const std::string& _indexer_string) {

        const auto pos = _indexer_string.find_last_of(indexer_separator);
        if(std::string::npos == pos) {
            THROW(
               SYS_INVALID_INPUT_PARAM,
               boost::format("[%s] does not include an index separator for collection")
               % _indexer_string);
        }
        const auto index_name = _indexer_string.substr(0, pos-(indexer_separator.size()-1));
        const auto index_type = _indexer_string.substr(pos+1);
        return std::make_tuple(index_name, index_type);
    } // parse_indexer_string

    auto get_index_name(json _params) {
        std::string attribute{}, value{}, units{};

        auto md = _params["match_metadata"];
        attribute = md["attribute"];
        value     = md["value"];
        units     = md["units"];

        if("irods::indexing::index" != attribute ||
           "elasticsearch"         != units) {
            return std::make_tuple(
                    ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       boost::format("indexing invoked with incorrect metadata a [%s] u [%s]")
                       % attribute
                       % units),
                    std::string{});
        }

        auto [index_name, index_type] = parse_indexer_string(value);

        if("metadata" != index_type) {
            return std::make_tuple(
                    ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       boost::format("indexing invoked with incorrect index type [%s]")
                       % index_type),
                    std::string{});
        }

        return std::make_tuple(SUCCESS(), index_name);

    } // get_index_name

    irods::error metadata_index_elasticsearch(const pe::context& ctx)
    {
        auto [err, index_name] = get_index_name(ctx.parameters);
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

        std::string attribute{ctx.parameters["attribute"]}
                  , value{ctx.parameters["value"]}
                  , units{ctx.parameters["units"]};

        const std::string md_index_id{
                              get_metadata_index_id(
                                  get_object_index_id(
                                      ctx.rei,
                                      object_path),
                                  attribute,
                                  value,
                                  units)};
        std::string payload{
                        boost::str(
                        boost::format(
                        "{ \"object_path\":\"%s\", \"attribute\":\"%s\", \"value\":\"%s\", \"units\":\"%s\" }")
                        % object_path
                        % attribute
                        % value
                        % units)} ;

        const cpr::Response response = client.index(index_name, "text", md_index_id, payload);
        if(response.status_code != 200 && response.status_code != 201) {
            return ERROR(
                SYS_INTERNAL_ERR,
                boost::format(
                    "failed to index metadata [%s] [%s] [%s] for [%s] code [%d] message [%s]")
                    % attribute
                    % value
                    % units
                    % object_path
                    % response.status_code
                    % response.text);
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
