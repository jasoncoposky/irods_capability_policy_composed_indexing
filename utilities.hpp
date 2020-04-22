
#include "policy_engine.hpp"

#include "irods_hasher_factory.hpp"
#include "MD5Strategy.hpp"

namespace irods::indexing {
    const std::string indexer_separator{"::"};

    namespace pe = irods::policy_engine;
    namespace fs = irods::experimental::filesystem;

    using json = nlohmann::json;

    auto extract_metadata_parameters(const json& params)
    {
        auto err = SUCCESS();
        std::string operation{}, attribute{}, value{}, units{};

        if(!params.contains("metadata")) {
            err = ERROR(
                SYS_INVALID_INPUT_PARAM,
                "parameters do not include a metadata object");
            return std::make_tuple(err, operation, attribute, value, units);
        }

        auto md = params.at("metadata");

        if(!md.contains("operation")) {
            err = ERROR(
                SYS_INVALID_INPUT_PARAM,
                "metadata object does not include an operation parameter");
            return std::make_tuple(err, operation, attribute, value, units);
        }
        else {
            operation = md.at("operation");
        }

        if(!md.contains("attribute")) {
            err = ERROR(
                SYS_INVALID_INPUT_PARAM,
                "metadata object does not include an attribute parameter");
            return std::make_tuple(err, operation, attribute, value, units);
        }
        else {
            attribute = md.at("attribute");
        }

        if(!md.contains("value")) {
            err = ERROR(
                SYS_INVALID_INPUT_PARAM,
                "metadata object does not include an value parameter");
            return std::make_tuple(err, operation, attribute, value, units);
        }
        else {
            value = md.at("value");
        }

        if(md.contains("units")) { units = md.at("units"); }

        return std::make_tuple(err, operation, attribute, value, units);

    } // extract_metadata_parameters

    std::string get_object_index_id(
        rsComm_t*          _comm,
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
            irods::query<rsComm_t> qobj{_comm, qstr, 1};
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
                           "indexing invoked with without conditional parameter"),
                        std::string{});
            }
            if(!_params.at("conditional").contains("metadata")) {
                return std::make_tuple(
                        ERROR(
                           SYS_INVALID_INPUT_PARAM,
                           "indexing invoked with without conditional metadata parameter"),
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

        if("metadata" != index_type && "full_text" != index_type) {
            return std::make_tuple(
                    ERROR(
                       SYS_INVALID_INPUT_PARAM,
                       boost::format("indexing invoked with incorrect index type [%s]")
                       % index_type),
                    std::string{});
        }

        return std::make_tuple(SUCCESS(), index_name);

    } // get_index_name

	std::string correct_non_utf_8(std::string *str)
	{
		int i,f_size=str->size();
		unsigned char c,c2,c3,c4;
		std::string to;
		to.reserve(f_size);

		for(i=0 ; i<f_size ; i++){
			c=(unsigned char)(*str)[i];
			if(c<32){//control char
				if(c==9 || c==10 || c==13){//allow only \t \n \r
					to.append(1,c);
				}
				continue;
			}else if(c<127){//normal ASCII
				to.append(1,c);
				continue;
			}else if(c<160){//control char (nothing should be defined here either ASCI, ISO_8859-1 or UTF8, so skipping)
				if(c2==128){//fix microsoft mess, add euro
					to.append(1,226);
					to.append(1,130);
					to.append(1,172);
				}
				if(c2==133){//fix IBM mess, add NEL = \n\r
					to.append(1,10);
					to.append(1,13);
				}
				continue;
			}else if(c<192){//invalid for UTF8, converting ASCII
				to.append(1,(unsigned char)194);
				to.append(1,c);
				continue;
			}else if(c<194){//invalid for UTF8, converting ASCII
				to.append(1,(unsigned char)195);
				to.append(1,c-64);
				continue;
			}else if(c<224 && i+1<f_size){//possibly 2byte UTF8
				c2=(unsigned char)(*str)[i+1];
				if(c2>127 && c2<192){//valid 2byte UTF8
					if(c==194 && c2<160){//control char, skipping
						;
					}else{
						to.append(1,c);
						to.append(1,c2);
					}
					i++;
					continue;
				}
			}else if(c<240 && i+2<f_size){//possibly 3byte UTF8
				c2=(unsigned char)(*str)[i+1];
				c3=(unsigned char)(*str)[i+2];
				if(c2>127 && c2<192 && c3>127 && c3<192){//valid 3byte UTF8
					to.append(1,c);
					to.append(1,c2);
					to.append(1,c3);
					i+=2;
					continue;
				}
			}else if(c<245 && i+3<f_size){//possibly 4byte UTF8
				c2=(unsigned char)(*str)[i+1];
				c3=(unsigned char)(*str)[i+2];
				c4=(unsigned char)(*str)[i+3];
				if(c2>127 && c2<192 && c3>127 && c3<192 && c4>127 && c4<192){//valid 4byte UTF8
					to.append(1,c);
					to.append(1,c2);
					to.append(1,c3);
					to.append(1,c4);
					i+=3;
					continue;
				}
			}
			//invalid UTF8, converting ASCII (c>245 || string too short for multi-byte))
			to.append(1,(unsigned char)195);
			to.append(1,c-64);
		}
		return to;
	}

} // namespace irods::indexing


