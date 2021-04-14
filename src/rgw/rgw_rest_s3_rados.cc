// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <errno.h>
#include <array>
#include <string.h>
#include <string_view>

#include "common/ceph_crypto.h"
#include "common/split.h"
#include "common/Formatter.h"
#include "common/utf8.h"
#include "common/ceph_json.h"
#include "common/safe_io.h"
#include "common/errno.h"
#include "auth/Crypto.h"
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/tokenizer.hpp>

#include <liboath/oath.h>

#include "rgw_rest_s3.h"
#include "rgw_rest_s3_rados.h"
#include "rgw_rest_pubsub.h"
#include "rgw_auth_s3.h"
#include "rgw_acl.h"
#include "rgw_user.h"
#include "rgw_cors.h"
#include "rgw_cors_s3.h"
#include "rgw_tag_s3.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

bool is_valid_status(const string& s) {
  return (s == "Enabled" ||
          s == "Disabled");
}

static string enabled_group_id = "s3-bucket-replication:enabled";
static string disabled_group_id = "s3-bucket-replication:disabled";

struct ReplicationConfiguration {
  string role;

  struct Rule {
    struct DeleteMarkerReplication {
      string status;

      void decode_xml(XMLObj *obj) {
        RGWXMLDecoder::decode_xml("Status", status, obj);
      }

      void dump_xml(Formatter *f) const {
        encode_xml("Status", status, f);
      }

      bool is_valid(CephContext *cct) const {
        bool result = is_valid_status(status);
        if (!result) {
          ldout(cct, 5) << "NOTICE: bad status provided in DeleteMarkerReplication element (status=" << status << ")" << dendl;
        }
        return result;
      }
    };

    struct Source { /* rgw extension */
      std::vector<string> zone_names;

      void decode_xml(XMLObj *obj) {
        RGWXMLDecoder::decode_xml("Zone", zone_names, obj);
      }

      void dump_xml(Formatter *f) const {
        encode_xml("Zone", zone_names, f);
      }
    };

    struct Destination {
      struct AccessControlTranslation {
        string owner;

        void decode_xml(XMLObj *obj) {
          RGWXMLDecoder::decode_xml("Owner", owner, obj);
        }
        void dump_xml(Formatter *f) const {
          encode_xml("Owner", owner, f);
        }
      };

      std::optional<AccessControlTranslation> acl_translation;
      std::optional<string> account;
      string bucket;
      std::optional<string> storage_class;
      std::vector<string> zone_names;

      void decode_xml(XMLObj *obj) {
        RGWXMLDecoder::decode_xml("AccessControlTranslation", acl_translation, obj);
        RGWXMLDecoder::decode_xml("Account", account, obj);
        if (account && account->empty()) {
          account.reset();
        }
        RGWXMLDecoder::decode_xml("Bucket", bucket, obj);
        RGWXMLDecoder::decode_xml("StorageClass", storage_class, obj);
        if (storage_class && storage_class->empty()) {
          storage_class.reset();
        }
        RGWXMLDecoder::decode_xml("Zone", zone_names, obj); /* rgw extension */
      }

      void dump_xml(Formatter *f) const {
        encode_xml("AccessControlTranslation", acl_translation, f);
        encode_xml("Account", account, f);
        encode_xml("Bucket", bucket, f);
        encode_xml("StorageClass", storage_class, f);
        encode_xml("Zone", zone_names, f);
      }
    };

    struct Filter {
      struct Tag {
        string key;
        string value;

        bool empty() const {
          return key.empty() && value.empty();
        }

        void decode_xml(XMLObj *obj) {
          RGWXMLDecoder::decode_xml("Key", key, obj);
          RGWXMLDecoder::decode_xml("Value", value, obj);
        };

        void dump_xml(Formatter *f) const {
          encode_xml("Key", key, f);
          encode_xml("Value", value, f);
        }
      };

      struct AndElements {
        std::optional<string> prefix;
        std::vector<Tag> tags;

        bool empty() const {
          return !prefix &&
            (tags.size() == 0);
        }

        void decode_xml(XMLObj *obj) {
          std::vector<Tag> _tags;
          RGWXMLDecoder::decode_xml("Prefix", prefix, obj);
          if (prefix && prefix->empty()) {
            prefix.reset();
          }
          RGWXMLDecoder::decode_xml("Tag", _tags, obj);
          for (auto& t : _tags) {
            if (!t.empty()) {
              tags.push_back(std::move(t));
            }
          }
        };

        void dump_xml(Formatter *f) const {
          encode_xml("Prefix", prefix, f);
          encode_xml("Tag", tags, f);
        }
      };

      std::optional<string> prefix;
      std::optional<Tag> tag;
      std::optional<AndElements> and_elements;

      bool empty() const {
        return (!prefix && !tag && !and_elements);
      }

      void decode_xml(XMLObj *obj) {
        RGWXMLDecoder::decode_xml("Prefix", prefix, obj);
        if (prefix && prefix->empty()) {
          prefix.reset();
        }
        RGWXMLDecoder::decode_xml("Tag", tag, obj);
        if (tag && tag->empty()) {
          tag.reset();
        }
        RGWXMLDecoder::decode_xml("And", and_elements, obj);
        if (and_elements && and_elements->empty()) {
          and_elements.reset();
        }
      };

      void dump_xml(Formatter *f) const {
        encode_xml("Prefix", prefix, f);
        encode_xml("Tag", tag, f);
        encode_xml("And", and_elements, f);
      }

      bool is_valid(CephContext *cct) const {
        if (tag && prefix) {
          ldout(cct, 5) << "NOTICE: both tag and prefix were provided in replication filter rule" << dendl;
          return false;
        }

        if (and_elements) {
          if (prefix && and_elements->prefix) {
            ldout(cct, 5) << "NOTICE: too many prefixes were provided in re" << dendl;
            return false;
          }
        }
        return true;
      };

      int to_sync_pipe_filter(CephContext *cct,
                              rgw_sync_pipe_filter *f) const {
        if (!is_valid(cct)) {
          return -EINVAL;
        }
        if (prefix) {
          f->prefix = *prefix;
        }
        if (tag) {
          f->tags.insert(rgw_sync_pipe_filter_tag(tag->key, tag->value));
        }

        if (and_elements) {
          if (and_elements->prefix) {
            f->prefix = *and_elements->prefix;
          }
          for (auto& t : and_elements->tags) {
            f->tags.insert(rgw_sync_pipe_filter_tag(t.key, t.value));
          }
        }
        return 0;
      }

      void from_sync_pipe_filter(const rgw_sync_pipe_filter& f) {
        if (f.prefix && f.tags.empty()) {
          prefix = f.prefix;
          return;
        }
        if (f.prefix) {
          and_elements.emplace();
          and_elements->prefix = f.prefix;
        } else if (f.tags.size() == 1) {
          auto iter = f.tags.begin();
          if (iter == f.tags.end()) {
            /* should never happen */
            return;
          }
          auto& t = *iter;
          tag.emplace();
          tag->key = t.key;
          tag->value = t.value;
          return;
        }

        if (f.tags.empty()) {
          return;
        }

        if (!and_elements) {
          and_elements.emplace();
        }

        for (auto& t : f.tags) {
          auto& tag = and_elements->tags.emplace_back();
          tag.key = t.key;
          tag.value = t.value;
        }
      }
    };

    set<rgw_zone_id> get_zone_ids_from_names(rgw::sal::Store* store,
                                             const vector<string>& zone_names) const {
      set<rgw_zone_id> ids;

      for (auto& name : zone_names) {
	std::unique_ptr<rgw::sal::Zone> zone;
	int r = store->get_zone_by_name(name, &zone);
	if (r >= 0) {
	  ids.insert(zone->get_id());
	}
      }

      return ids;
    }

    vector<string> get_zone_names_from_ids(rgw::sal::Store* store,
                                           const set<rgw_zone_id>& zone_ids) const {
      vector<string> names;

      for (auto& id : zone_ids) {
	std::unique_ptr<rgw::sal::Zone> zone;
	int r = store->get_zone_by_id(id, &zone);
	if (r >= 0) {
          names.emplace_back(zone->get_name());
        }
      }

      return names;
    }

    std::optional<DeleteMarkerReplication> delete_marker_replication;
    std::optional<Source> source;
    Destination destination;
    std::optional<Filter> filter;
    string id;
    int32_t priority;
    string status;

    void decode_xml(XMLObj *obj) {
      RGWXMLDecoder::decode_xml("DeleteMarkerReplication", delete_marker_replication, obj);
      RGWXMLDecoder::decode_xml("Source", source, obj);
      RGWXMLDecoder::decode_xml("Destination", destination, obj);
      RGWXMLDecoder::decode_xml("ID", id, obj);

      std::optional<string> prefix;
      RGWXMLDecoder::decode_xml("Prefix", prefix, obj);
      if (prefix) {
        filter.emplace();
        filter->prefix = prefix;
      }

      if (!filter) {
        RGWXMLDecoder::decode_xml("Filter", filter, obj);
      } else {
        /* don't want to have filter reset because it might have been initialized
         * when decoding prefix
         */
        RGWXMLDecoder::decode_xml("Filter", *filter, obj);
      }

      RGWXMLDecoder::decode_xml("Priority", priority, obj);
      RGWXMLDecoder::decode_xml("Status", status, obj);
    }

    void dump_xml(Formatter *f) const {
      encode_xml("DeleteMarkerReplication", delete_marker_replication, f);
      encode_xml("Source", source, f);
      encode_xml("Destination", destination, f);
      encode_xml("Filter", filter, f);
      encode_xml("ID", id, f);
      encode_xml("Priority", priority, f);
      encode_xml("Status", status, f);
    }

    bool is_valid(CephContext *cct) const {
      if (!is_valid_status(status)) {
        ldout(cct, 5) << "NOTICE: bad status provided in rule (status=" << status << ")" << dendl;
        return false;
      }
      if ((filter && !filter->is_valid(cct)) ||
          (delete_marker_replication && !delete_marker_replication->is_valid(cct))) {
        return false;
      }
      return true;
    }

    int to_sync_policy_pipe(req_state *s, rgw::sal::Store* store,
                            rgw_sync_bucket_pipes *pipe,
                            bool *enabled) const {
      if (!is_valid(s->cct)) {
        return -EINVAL;
      }

      pipe->id = id;
      pipe->params.priority = priority;

      const auto& user_id = s->user->get_id();

      rgw_bucket_key dest_bk(user_id.tenant,
                             destination.bucket);

      if (source && !source->zone_names.empty()) {
        pipe->source.zones = get_zone_ids_from_names(store, source->zone_names);
      } else {
        pipe->source.set_all_zones(true);
      }
      if (!destination.zone_names.empty()) {
        pipe->dest.zones = get_zone_ids_from_names(store, destination.zone_names);
      } else {
        pipe->dest.set_all_zones(true);
      }
      pipe->dest.bucket.emplace(dest_bk);

      if (filter) {
        int r = filter->to_sync_pipe_filter(s->cct, &pipe->params.source.filter);
        if (r < 0) {
          return r;
        }
      }
      if (destination.acl_translation) {
        rgw_user u;
        u.tenant = user_id.tenant;
        u.from_str(destination.acl_translation->owner); /* explicit tenant will override tenant,
                                                           otherwise will inherit it from s->user */
        pipe->params.dest.acl_translation.emplace();
        pipe->params.dest.acl_translation->owner = u;
      }
      pipe->params.dest.storage_class = destination.storage_class;

      *enabled = (status == "Enabled");

      pipe->params.mode = rgw_sync_pipe_params::Mode::MODE_USER;
      pipe->params.user = user_id.to_str();

      return 0;
    }

    void from_sync_policy_pipe(rgw::sal::Store* store,
                              const rgw_sync_bucket_pipes& pipe,
                              bool enabled) {
      id = pipe.id;
      status = (enabled ? "Enabled" : "Disabled");
      priority = pipe.params.priority;

      if (pipe.source.all_zones) {
        source.reset();
      } else if (pipe.source.zones) {
        source.emplace();
        source->zone_names = get_zone_names_from_ids(store, *pipe.source.zones);
      }

      if (!pipe.dest.all_zones &&
          pipe.dest.zones) {
        destination.zone_names = get_zone_names_from_ids(store, *pipe.dest.zones);
      }

      if (pipe.params.dest.acl_translation) {
        destination.acl_translation.emplace();
        destination.acl_translation->owner = pipe.params.dest.acl_translation->owner.to_str();
      }

      if (pipe.params.dest.storage_class) {
        destination.storage_class = *pipe.params.dest.storage_class;
      }

      if (pipe.dest.bucket) {
        destination.bucket = pipe.dest.bucket->get_key();
      }

      filter.emplace();
      filter->from_sync_pipe_filter(pipe.params.source.filter);

      if (filter->empty()) {
        filter.reset();
      }
    }
  };

  std::vector<Rule> rules;

  void decode_xml(XMLObj *obj) {
    RGWXMLDecoder::decode_xml("Role", role, obj);
    RGWXMLDecoder::decode_xml("Rule", rules, obj);
  }

  void dump_xml(Formatter *f) const {
    encode_xml("Role", role, f);
    encode_xml("Rule", rules, f);
  }

  int to_sync_policy_groups(req_state *s, rgw::sal::Store* store,
                            vector<rgw_sync_policy_group> *result) const {
    result->resize(2);

    rgw_sync_policy_group& enabled_group = (*result)[0];
    rgw_sync_policy_group& disabled_group = (*result)[1];

    enabled_group.id = enabled_group_id;
    enabled_group.status = rgw_sync_policy_group::Status::ENABLED;
    disabled_group.id = disabled_group_id;
    disabled_group.status = rgw_sync_policy_group::Status::ALLOWED; /* not enabled, not forbidden */

    for (auto& rule : rules) {
      rgw_sync_bucket_pipes pipe;
      bool enabled;
      int r = rule.to_sync_policy_pipe(s, store, &pipe, &enabled);
      if (r < 0) {
        ldpp_dout(s, 5) << "NOTICE: failed to convert replication configuration into sync policy pipe (rule.id=" << rule.id << "): " << cpp_strerror(-r) << dendl;
        return r;
      }

      if (enabled) {
        enabled_group.pipes.emplace_back(std::move(pipe));
      } else {
        disabled_group.pipes.emplace_back(std::move(pipe));
      }
    }
    return 0;
  }

  void from_sync_policy_group(rgw::sal::Store* store,
                              const rgw_sync_policy_group& group) {

    bool enabled = (group.status == rgw_sync_policy_group::Status::ENABLED);

    for (auto& pipe : group.pipes) {
      auto& rule = rules.emplace_back();
      rule.from_sync_policy_pipe(store, pipe, enabled);
    }
  }
};

void RGWGetBucketReplication_ObjStore_S3::send_response_data()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);

  ReplicationConfiguration conf;

  if (s->bucket->get_info().sync_policy) {
    auto policy = s->bucket->get_info().sync_policy;

    auto iter = policy->groups.find(enabled_group_id);
    if (iter != policy->groups.end()) {
      conf.from_sync_policy_group(store, iter->second);
    }
    iter = policy->groups.find(disabled_group_id);
    if (iter != policy->groups.end()) {
      conf.from_sync_policy_group(store, iter->second);
    }
  }

  if (!op_ret) {
  s->formatter->open_object_section_in_ns("ReplicationConfiguration", XMLNS_AWS_S3);
  conf.dump_xml(s->formatter);
  s->formatter->close_section();
  rgw_flush_formatter_and_reset(s, s->formatter);
  }
}

int RGWPutBucketReplication_ObjStore_S3::get_params(optional_yield y)
{
  RGWXMLParser parser;

  if (!parser.init()){
    return -EINVAL;
  }

  const auto max_size = s->cct->_conf->rgw_max_put_param_size;
  int r = 0;
  bufferlist data;

  std::tie(r, data) = read_all_input(s, max_size, false);

  if (r < 0)
    return r;

  if (!parser.parse(data.c_str(), data.length(), 1)) {
    return -ERR_MALFORMED_XML;
  }

  ReplicationConfiguration conf;
  try {
    RGWXMLDecoder::decode_xml("ReplicationConfiguration", conf, &parser);
  } catch (RGWXMLDecoder::err& err) {

    ldpp_dout(this, 5) << "Malformed tagging request: " << err << dendl;
    return -ERR_MALFORMED_XML;
  }

  r = conf.to_sync_policy_groups(s, store, &sync_policy_groups);
  if (r < 0) {
    return r;
  }

  // forward requests to meta master zone
  if (!store->is_meta_master()) {
    /* only need to keep this data around if we're not meta master */
    in_data = std::move(data);
  }

  return 0;
}

void RGWPutBucketReplication_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
}

void RGWDeleteBucketReplication_ObjStore_S3::update_sync_policy(rgw_sync_policy_info *policy)
{
  policy->groups.erase(enabled_group_id);
  policy->groups.erase(disabled_group_id);
}

void RGWDeleteBucketReplication_ObjStore_S3::send_response()
{
  if (op_ret)
    set_req_state_err(s, op_ret);
  dump_errno(s);
  end_header(s, this, "application/xml");
  dump_start(s);
}
