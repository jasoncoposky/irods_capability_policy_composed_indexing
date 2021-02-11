
#include "policy_composition_framework_policy_engine.hpp"

#define IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#include "filesystem.hpp"

#include "irods_hasher_factory.hpp"
#include "MD5Strategy.hpp"

namespace irods::indexing {

    namespace keywords {
        const std::string attribute{"attribute"};
        const std::string value{"value"};
        const std::string units{"units"};
        const std::string operation{"operation"};
        const std::string entity_type{"entity_type"};
        const std::string entity{"entity"};
    }

    namespace index_type {
        const std::string metadata{"metadata"};
        const std::string full_text{"full_text"};
    }

    // clang-format off
    namespace pe   = irods::policy_composition::policy_engine;
    namespace fs   = irods::experimental::filesystem;
    namespace fsvr = irods::experimental::filesystem::server;
    namespace kw   = keywords;
    namespace it   = index_type;
    using     json = nlohmann::json;
    // clang-format on

    const std::string indexer_separator{"::"};
    const std::string indexing_attribute{"irods::indexing::index"};
    const std::string elasticsearch_units{"elasticsearch"};

    namespace {

        auto event_is_invalid(const json& p, const std::vector<std::string>& events)
        {
            if(!p.contains("event")) {
                return true;
            }

            const auto event = p.at("event").get<std::string>();

            for(const auto e : events) {
                std::string upper{e};
                std::transform(upper.begin(),
                               upper.end(),
                               upper.begin(),
                               ::toupper);
                if(upper == event) {
                    return false;
                }

            } // for e

            return true;

        } // event_is_invalid

        void throw_if_not_elasticsearch(const json& md)
        {
            if(elasticsearch_units != md.at(kw::units)) {
                THROW(
                    SYS_INVALID_INPUT_PARAM,
                    fmt::format("metadata did not include elasticsearch configuration [{}]",
                    md.dump(4).c_str()));
            }
        }

        void throw_if_type_is_invalid(const std::string& t)
        {
            if(it::metadata != t
               && it::full_text != t) {
                THROW(
                    SYS_INVALID_INPUT_PARAM,
                    fmt::format("indexing invoked with invalid type [{}]", t));
            }
        }

        void throw_if_metadata_is_missing(const json& p)
        {
            if(!p.contains("metadata")) {
                rodsLog(LOG_ERROR, "metadata is missing %s",
                        p.dump(4).c_str());
                THROW(
                    SYS_INVALID_INPUT_PARAM,
                    "metadata object is missing [{}]");
            }
        }

        void throw_if_conditional_metadata_is_missing(const json& p)
        {
            if(!p.contains("conditional_metadata")) {
                rodsLog(
                    LOG_ERROR,
                    "configuration_metadata is missing %s",
                    p.dump(4).c_str());
                THROW(
                    SYS_INVALID_INPUT_PARAM,
                    "configuration_metadata object is missing [{}]");
            }
        }

        void throw_if_metadata_is_not_complete(const json& md)
        {
            if(!md.contains(kw::attribute)
               || !md.contains(kw::value)
               || !md.contains(kw::units)) {
                THROW(SYS_INVALID_INPUT_PARAM,
                    fmt::format("metadata is not complete [{}]",
                    md.dump(4).c_str()));
            }
        }

        auto extract_avu(const json& md)
        {
            auto a = md.at(kw::attribute).get<std::string>();
            auto v = md.at(kw::value).get<std::string>();
            auto u = md.at(kw::units).get<std::string>();

            return std::make_tuple(a, v, u);
        }

        auto metadata_is_indexing(const json& md)
        {
            return (!md.empty()
                    && md.contains(kw::attribute)
                    && md.at(kw::attribute) == indexing_attribute);
        }

        auto get_indexing_metadata(const json& params)
        {
            // conditional metadata
            auto cmd = params.at("conditional_metadata");

            // applied metadata
            auto amd = params.contains("metadata")
                       ? params.at("metadata")
                       : json{};

            // metadata applied use case
            if(metadata_is_indexing(amd)) {
                return amd;
            }

            // metadata exists use case
            if(metadata_is_indexing(cmd)) {
                return cmd;
            }

            THROW(SYS_INVALID_INPUT_PARAM,
                  "metadata did not include indexing configuration");

        } // get_indexing_metadata

        auto extract_name_and_type(const std::string& str)
        {
            const auto pos = str.find_last_of(indexer_separator);

            if(std::string::npos == pos) {
                THROW(
                   SYS_INVALID_INPUT_PARAM,
                   boost::format("[%s] does not include an index separator for collection")
                   % str);
            }

            const auto name = str.substr(0, pos-(indexer_separator.size()-1));
            const auto type = str.substr(pos+1);

            return std::make_tuple(name, type);

        } // extract_name_and_type

    } // namespace

    auto extract_all(const json& m)
    {
        // clang-format off
        auto a = m.at(kw::attribute).get<std::string>();
        auto v = m.at(kw::value).get<std::string>();
        auto u = m.contains(kw::units)       ? m.at(kw::units).get<std::string>()       : std::string{};
        auto o = m.contains(kw::operation)   ? m.at(kw::operation).get<std::string>()   : std::string{};
        auto e = m.contains(kw::entity)      ? m.at(kw::entity).get<std::string>()      : std::string{};
        auto t = m.contains(kw::entity_type) ? m.at(kw::entity_type).get<std::string>() : std::string{};
        // clang-format on

        return std::make_tuple(a, v, u, o, e, t);

    } // extract_all

    auto get_id_for_logical_path(
        rsComm_t*          _comm,
        const std::string& _logical_path)
    {

        fs::path p{_logical_path};

        auto qstr = std::string{};

        if(fsvr::is_collection(*_comm, _logical_path)) {
            qstr = fmt::format("SELECT COLL_ID WHERE COLL_NAME = '{}'"
                        , _logical_path);
        }
        else {
            auto coll_name = p.parent_path().string();
            auto data_name = p.object_name().string();

            qstr = fmt::format("SELECT DATA_ID WHERE DATA_NAME = '{}' AND COLL_NAME = '{}'"
                        , data_name
                        , coll_name);
        }
        try {
            irods::query<rsComm_t> qobj{_comm, qstr, 1};
            if(qobj.size() > 0) {
                return qobj.front()[0];
            }
            THROW(
                CAT_NO_ROWS_FOUND,
                boost::format("failed to get id for [%s]")
                % _logical_path);
        }
        catch(const irods::exception& _e) {
            THROW(
                CAT_NO_ROWS_FOUND,
                boost::format("failed to get id for [%s]")
                % _logical_path);
        }

    } // get_id_for_logical_path

    auto get_metadata_index_id(
        const std::string& _index_id,
        const std::string& _attribute,
        const std::string& _value,
        const std::string& _units)
    {
        std::string str = _attribute
                          + _value
                          + _units;

        irods::Hasher hasher;
        irods::getHasher( irods::MD5_NAME, hasher );
        hasher.update(str);

        std::string digest;
        hasher.digest(digest);

        return _index_id + indexer_separator + digest;

    } // get_metadata_index_id

    auto get_index_name(const json& params)
    {
        auto md = get_indexing_metadata(params);

        throw_if_metadata_is_not_complete(md);

        throw_if_not_elasticsearch(md);

        auto [a, v, u] = extract_avu(md);
        auto [n, t]    = extract_name_and_type(v);

        throw_if_type_is_invalid(t);

        return n;

    } // get_index_name

	auto correct_non_utf_8(std::string *str)
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

	} // correct_non_utf_8

} // namespace irods::indexing


