
#include "policy_engine.hpp"

#include "irods_hasher_factory.hpp"
#include "MD5Strategy.hpp"

namespace irods::indexing {
    const std::string indexer_separator{"::"};

    namespace pe = irods::policy_engine;
    namespace fs = irods::experimental::filesystem;

    using json = nlohmann::json;

    std::string get_object_index_id(
        ruleExecInfo_t*    _rei,
        const std::string& _logical_path) {

        fs::path p{_logical_path};
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
                % _logical_path);
        }
        catch(const irods::exception& _e) {
            THROW(
                CAT_NO_ROWS_FOUND,
                boost::format("failed to get object id for [%s]")
                % _logical_path);
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

        try {
            if(!_params.contains("conditional")) {
                return std::make_tuple(
                        ERROR(
                           SYS_INVALID_INPUT_PARAM,
                           "indexing invoked with without conditional metadata"),
                        std::string{});
            }
            if(!_params.at("conditional").contains("metadata")) {
                return std::make_tuple(
                        ERROR(
                           SYS_INVALID_INPUT_PARAM,
                           "indexing invoked with without conditional metadata"),
                        std::string{});
            }

            auto md{_params.at("conditional").at("metadata")};
            attribute = md["attribute"];
            value     = md["value"];
            units     = md["units"];
        }
        catch(...) {
            return std::make_tuple(
                    ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       "indexing invoked with without index metadata"),
                    std::string{});
        }

        if("irods::indexing::index" != attribute ||
           "elasticsearch"          != units) {
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

} // namespace irods::indexing


