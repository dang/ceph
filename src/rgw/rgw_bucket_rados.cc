// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include <cerrno>
#include <map>
#include <sstream>
#include <string>
#include <string_view>

#include <boost/format.hpp>

#include "common/errno.h"
#include "common/ceph_json.h"
#include "include/scope_guard.h"

#include "rgw_datalog.h"
#include "rgw_rados.h"
#include "rgw_zone.h"
#include "rgw_acl.h"
#include "rgw_acl_s3.h"
#include "rgw_tag_s3.h"

#include "include/types.h"
#include "rgw_bucket.h"
#include "rgw_bucket_admin.h"
#include "rgw_user.h"
#include "rgw_string.h"
#include "rgw_multi.h"
#include "rgw_op.h"
#include "rgw_bucket_sync.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"
#include "services/svc_bucket.h"
#include "services/svc_bucket_sync.h"
#include "services/svc_meta.h"
#include "services/svc_meta_be_sobj.h"
#include "services/svc_user.h"
#include "services/svc_cls.h"
#include "services/svc_bilog_rados.h"

#include "include/rados/librados.hpp"
// until everything is moved from rgw_common
#include "rgw_common.h"
#include "rgw_reshard.h"
#include "rgw_lc.h"
#include "rgw_bucket_layout.h"

// stolen from src/cls/version/cls_version.cc
#define VERSION_ATTR "ceph.objclass.version"

#include "cls/user/cls_user_types.h"

#include "rgw_sal_rados.h"

#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_rgw

#define BUCKET_TAG_TIMEOUT 30

static void set_err_msg(std::string *sink, std::string msg)
{
  if (sink && !msg.empty())
    *sink = msg;
}

static void dump_bucket_usage(map<RGWObjCategory, RGWStorageStats>& stats, Formatter *formatter)
{
  map<RGWObjCategory, RGWStorageStats>::iterator iter;

  formatter->open_object_section("usage");
  for (iter = stats.begin(); iter != stats.end(); ++iter) {
    RGWStorageStats& s = iter->second;
    const char *cat_name = rgw_obj_category_name(iter->first);
    formatter->open_object_section(cat_name);
    s.dump(formatter);
    formatter->close_section();
  }
  formatter->close_section();
}

static void dump_index_check(map<RGWObjCategory, RGWStorageStats> existing_stats,
        map<RGWObjCategory, RGWStorageStats> calculated_stats,
        Formatter *formatter)
{
  formatter->open_object_section("check_result");
  formatter->open_object_section("existing_header");
  dump_bucket_usage(existing_stats, formatter);
  formatter->close_section();
  formatter->open_object_section("calculated_header");
  dump_bucket_usage(calculated_stats, formatter);
  formatter->close_section();
  formatter->close_section();
}


int RGWBucketAdminOp::get_policy(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  RGWAccessControlPolicy& policy, const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0)
    return ret;

  ret = bucket.get_policy(op_state, policy, null_yield, dpp);
  if (ret < 0)
    return ret;

  return 0;
}

/* Wrappers to facilitate RESTful interface */


int RGWBucketAdminOp::get_policy(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp)
{
  RGWAccessControlPolicy policy(store->ctx());

  int ret = get_policy(store, op_state, policy, dpp);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();

  flusher.start(0);

  formatter->open_object_section("policy");
  policy.dump(formatter);
  formatter->close_section();

  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::dump_s3_policy(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  ostream& os, const DoutPrefixProvider *dpp)
{
  RGWAccessControlPolicy_S3 policy(store->ctx());

  int ret = get_policy(store, op_state, policy, dpp);
  if (ret < 0)
    return ret;

  policy.to_xml(os);

  return 0;
}

int RGWBucketAdminOp::unlink(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0)
    return ret;

  return static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->unlink_bucket(op_state.get_user_id(), op_state.get_bucket()->get_info().bucket, null_yield, dpp, true);
}

int RGWBucketAdminOp::link(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, string *err)
{
  if (!op_state.is_user_op()) {
    set_err_msg(err, "empty user id");
    return -EINVAL;
  }

  RGWBucket bucket;
  int ret = bucket.init(store, op_state, null_yield, dpp, err);
  if (ret < 0)
    return ret;

  string bucket_id = op_state.get_bucket_id();
  std::string display_name = op_state.get_user_display_name();
  std::unique_ptr<rgw::sal::Bucket> loc_bucket;
  std::unique_ptr<rgw::sal::Bucket> old_bucket;

  loc_bucket = op_state.get_bucket()->clone();

  if (!bucket_id.empty() && bucket_id != loc_bucket->get_bucket_id()) {
    set_err_msg(err,
	"specified bucket id does not match " + loc_bucket->get_bucket_id());
    return -EINVAL;
  }

  old_bucket = loc_bucket->clone();

  loc_bucket->get_key().tenant = op_state.get_user_id().tenant;

  if (!op_state.new_bucket_name.empty()) {
    auto pos = op_state.new_bucket_name.find('/');
    if (pos != string::npos) {
      loc_bucket->get_key().tenant = op_state.new_bucket_name.substr(0, pos);
      loc_bucket->get_key().name = op_state.new_bucket_name.substr(pos + 1);
    } else {
      loc_bucket->get_key().name = op_state.new_bucket_name;
    }
  }

  RGWObjVersionTracker objv_tracker;
  RGWObjVersionTracker old_version = loc_bucket->get_info().objv_tracker;

  map<string, bufferlist>::iterator aiter = loc_bucket->get_attrs().find(RGW_ATTR_ACL);
  if (aiter == loc_bucket->get_attrs().end()) {
	// should never happen; only pre-argonaut buckets lacked this.
    ldpp_dout(dpp, 0) << "WARNING: can't bucket link because no acl on bucket=" << old_bucket << dendl;
    set_err_msg(err,
	"While crossing the Anavros you have displeased the goddess Hera."
	"  You must sacrifice your ancient bucket " + loc_bucket->get_bucket_id());
    return -EINVAL;
  }
  bufferlist& aclbl = aiter->second;
  RGWAccessControlPolicy policy;
  ACLOwner owner;
  try {
   auto iter = aclbl.cbegin();
   decode(policy, iter);
   owner = policy.get_owner();
  } catch (buffer::error& e) {
    set_err_msg(err, "couldn't decode policy");
    return -EIO;
  }

  int r = static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->unlink_bucket(owner.get_id(), old_bucket->get_info().bucket, null_yield, dpp, false);
  if (r < 0) {
    set_err_msg(err, "could not unlink policy from user " + owner.get_id().to_str());
    return r;
  }

  // now update the user for the bucket...
  if (display_name.empty()) {
    ldpp_dout(dpp, 0) << "WARNING: user " << op_state.get_user_id() << " has no display name set" << dendl;
  }

  RGWAccessControlPolicy policy_instance;
  policy_instance.create_default(op_state.get_user_id(), display_name);
  owner = policy_instance.get_owner();

  aclbl.clear();
  policy_instance.encode(aclbl);

  bool exclusive = false;
  loc_bucket->get_info().owner = op_state.get_user_id();
  if (*loc_bucket != *old_bucket) {
    loc_bucket->get_info().bucket = loc_bucket->get_key();
    loc_bucket->get_info().objv_tracker.version_for_read()->ver = 0;
    exclusive = true;
  }

  r = loc_bucket->put_instance_info(dpp, exclusive, ceph::real_time());
  if (r < 0) {
    set_err_msg(err, "ERROR: failed writing bucket instance info: " + cpp_strerror(-r));
    return r;
  }

  /* link to user */
  RGWBucketEntryPoint ep;
  ep.bucket = loc_bucket->get_info().bucket;
  ep.owner = op_state.get_user_id();
  ep.creation_time = loc_bucket->get_info().creation_time;
  ep.linked = true;
  rgw::sal::Attrs ep_attrs;
  rgw_ep_info ep_data{ep, ep_attrs};

  r = static_cast<rgw::sal::RadosStore*>(store)->ctl()->bucket->link_bucket(op_state.get_user_id(), loc_bucket->get_info().bucket, loc_bucket->get_info().creation_time, null_yield, dpp, true, &ep_data);
  if (r < 0) {
    set_err_msg(err, "failed to relink bucket");
    return r;
  }

  if (*loc_bucket != *old_bucket) {
    // like RGWRados::delete_bucket -- excepting no bucket_index work.
    r = old_bucket->remove_metadata(dpp, &ep_data.ep_objv, null_yield);
    if (r < 0) {
      set_err_msg(err, "failed to unlink old bucket " + old_bucket->get_tenant() + "/" + old_bucket->get_name());
      return r;
    }
  }

  return 0;
}

int RGWBucketAdminOp::chown(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const string& marker, const DoutPrefixProvider *dpp, string *err)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield, dpp, err);
  if (ret < 0)
    return ret;

  return bucket.chown(op_state, marker, null_yield, dpp, err);

}

int RGWBucketAdminOp::check_index(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, optional_yield y, const DoutPrefixProvider *dpp)
{
  int ret;
  map<RGWObjCategory, RGWStorageStats> existing_stats;
  map<RGWObjCategory, RGWStorageStats> calculated_stats;


  RGWBucket bucket;

  ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0)
    return ret;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  ret = bucket.check_bad_index_multipart(op_state, flusher, dpp);
  if (ret < 0)
    return ret;

  ret = bucket.check_object_index(dpp, op_state, flusher, y);
  if (ret < 0)
    return ret;

  ret = bucket.check_index(dpp, op_state, existing_stats, calculated_stats);
  if (ret < 0)
    return ret;

  dump_index_check(existing_stats, calculated_stats, formatter);
  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::remove_bucket(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
				    optional_yield y, const DoutPrefixProvider *dpp,
                                    bool bypass_gc, bool keep_index_consistent)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  std::unique_ptr<rgw::sal::User> user = store->get_user(op_state.get_user_id());

  int ret = store->get_bucket(dpp, user.get(), user->get_tenant(), op_state.get_bucket_name(),
			      &bucket, y);
  if (ret < 0)
    return ret;

  if (bypass_gc)
    ret = rgw_remove_bucket_bypass_gc(store, bucket.get(), op_state.get_max_aio(), keep_index_consistent, y, dpp);
  else
    ret = bucket->remove_bucket(dpp, op_state.will_delete_children(), string(), string(),
				false, nullptr, y);

  return ret;
}

int RGWBucketAdminOp::remove_object(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0)
    return ret;

  return bucket.remove_object(dpp, op_state);
}

int RGWBucketAdminOp::sync_bucket(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, string *err_msg)
{
  RGWBucket bucket;

  if (!store->is_meta_master()) {
    set_err_msg(err_msg, "ERROR: failed to update bucket sync: only allowed on meta master zone");
    return -EINVAL;
  }

  int r = bucket.init(store, op_state, null_yield, dpp, err_msg);
  if (r < 0)
  {
    return r;
  }
  bool sync = op_state.will_sync_bucket();
  if (sync) {
    bucket.bucket->get_info().flags &= ~BUCKET_DATASYNC_DISABLED;
  } else {
    bucket.bucket->get_info().flags |= BUCKET_DATASYNC_DISABLED;
  }

  r = bucket.bucket->put_instance_info(dpp, false, real_time());
  if (r < 0) {
    set_err_msg(err_msg, "ERROR: failed writing bucket instance info:" + cpp_strerror(-r));
    return r;
  }

  int shards_num = bucket.bucket->get_info().layout.current_index.layout.normal.num_shards? bucket.bucket->get_info().layout.current_index.layout.normal.num_shards : 1;
  int shard_id = bucket.bucket->get_info().layout.current_index.layout.normal.num_shards? 0 : -1;

  if (!sync) {
    r = static_cast<rgw::sal::RadosStore*>(store)->svc()->bilog_rados->log_stop(dpp, bucket.bucket->get_info(), -1);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed writing stop bilog:" + cpp_strerror(-r));
      return r;
    }
  } else {
    r = static_cast<rgw::sal::RadosStore*>(store)->svc()->bilog_rados->log_start(dpp, bucket.bucket->get_info(), -1);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed writing resync bilog:" + cpp_strerror(-r));
      return r;
    }
  }

  for (int i = 0; i < shards_num; ++i, ++shard_id) {
    r = static_cast<rgw::sal::RadosStore*>(store)->svc()->datalog_rados->add_entry(dpp, bucket.bucket->get_info(), shard_id);
    if (r < 0) {
      set_err_msg(err_msg, "ERROR: failed writing data log:" + cpp_strerror(-r));
      return r;
    }
  }

  return 0;
}

static int bucket_stats(rgw::sal::Store* store,
			const std::string& tenant_name,
			const std::string& bucket_name,
			Formatter *formatter,
                        const DoutPrefixProvider *dpp)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  map<RGWObjCategory, RGWStorageStats> stats;

  real_time mtime;
  int ret = store->get_bucket(dpp, nullptr, tenant_name, bucket_name, &bucket, null_yield);
  if (ret < 0) {
    return ret;
  }

  string bucket_ver, master_ver;
  string max_marker;
  ret = bucket->get_bucket_stats(dpp, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, &max_marker);
  if (ret < 0) {
    cerr << "error getting bucket stats bucket=" << bucket->get_name() << " ret=" << ret << std::endl;
    return ret;
  }

  utime_t ut(mtime);
  utime_t ctime_ut(bucket->get_creation_time());

  formatter->open_object_section("stats");
  formatter->dump_string("bucket", bucket->get_name());
  formatter->dump_int("num_shards",
		      bucket->get_info().layout.current_index.layout.normal.num_shards);
  formatter->dump_string("tenant", bucket->get_tenant());
  formatter->dump_string("zonegroup", bucket->get_info().zonegroup);
  formatter->dump_string("placement_rule", bucket->get_info().placement_rule.to_str());
  ::encode_json("explicit_placement", bucket->get_key().explicit_placement, formatter);
  formatter->dump_string("id", bucket->get_bucket_id());
  formatter->dump_string("marker", bucket->get_marker());
  formatter->dump_stream("index_type") << bucket->get_info().layout.current_index.layout.type;
  ::encode_json("owner", bucket->get_info().owner, formatter);
  formatter->dump_string("ver", bucket_ver);
  formatter->dump_string("master_ver", master_ver);
  ut.gmtime(formatter->dump_stream("mtime"));
  ctime_ut.gmtime(formatter->dump_stream("creation_time"));
  formatter->dump_string("max_marker", max_marker);
  dump_bucket_usage(stats, formatter);
  encode_json("bucket_quota", bucket->get_info().quota, formatter);

  // bucket tags
  auto iter = bucket->get_attrs().find(RGW_ATTR_TAGS);
  if (iter != bucket->get_attrs().end()) {
    RGWObjTagSet_S3 tagset;
    bufferlist::const_iterator piter{&iter->second};
    try {
      tagset.decode(piter);
      tagset.dump(formatter);
    } catch (buffer::error& err) {
      cerr << "ERROR: caught buffer:error, couldn't decode TagSet" << std::endl;
    }
  }

  // TODO: bucket CORS
  // TODO: bucket LC
  formatter->close_section();

  return 0;
}

int RGWBucketAdminOp::limit_check(rgw::sal::Store* store,
				  RGWBucketAdminOpState& op_state,
				  const std::list<std::string>& user_ids,
				  RGWFormatterFlusher& flusher, optional_yield y,
                                  const DoutPrefixProvider *dpp,
				  bool warnings_only)
{
  int ret = 0;
  const size_t max_entries =
    store->ctx()->_conf->rgw_list_buckets_max_chunk;

  const size_t safe_max_objs_per_shard =
    store->ctx()->_conf->rgw_safe_max_objects_per_shard;

  uint16_t shard_warn_pct =
    store->ctx()->_conf->rgw_shard_warning_threshold;
  if (shard_warn_pct > 100)
    shard_warn_pct = 90;

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  formatter->open_array_section("users");

  for (const auto& user_id : user_ids) {

    formatter->open_object_section("user");
    formatter->dump_string("user_id", user_id);
    formatter->open_array_section("buckets");

    string marker;
    rgw::sal::BucketList buckets;
    do {
      std::unique_ptr<rgw::sal::User> user = store->get_user(rgw_user(user_id));

      ret = user->list_buckets(dpp, marker, string(), max_entries, false, buckets, y);

      if (ret < 0)
        return ret;

      map<string, std::unique_ptr<rgw::sal::Bucket>>& m_buckets = buckets.get_buckets();

      for (const auto& iter : m_buckets) {
	auto& bucket = iter.second;
	uint32_t num_shards = 1;
	uint64_t num_objects = 0;

	marker = bucket->get_name(); /* Casey's location for marker update,
				     * as we may now not reach the end of
				     * the loop body */

	ret = bucket->get_bucket_info(dpp, null_yield);
	if (ret < 0)
	  continue;

	/* need stats for num_entries */
	string bucket_ver, master_ver;
	std::map<RGWObjCategory, RGWStorageStats> stats;
	ret = bucket->get_bucket_stats(dpp, RGW_NO_SHARD, &bucket_ver, &master_ver, stats, nullptr);

	if (ret < 0)
	  continue;

	for (const auto& s : stats) {
	  num_objects += s.second.num_objects;
	}

	num_shards = bucket->get_info().layout.current_index.layout.normal.num_shards;
	uint64_t objs_per_shard =
	  (num_shards) ? num_objects/num_shards : num_objects;
	{
	  bool warn;
	  stringstream ss;
	  uint64_t fill_pct = objs_per_shard * 100 / safe_max_objs_per_shard;
	  if (fill_pct > 100) {
	    ss << "OVER " << fill_pct << "%";
	    warn = true;
	  } else if (fill_pct >= shard_warn_pct) {
	    ss << "WARN " << fill_pct << "%";
	    warn = true;
	  } else {
	    ss << "OK";
	    warn = false;
	  }

	  if (warn || !warnings_only) {
	    formatter->open_object_section("bucket");
	    formatter->dump_string("bucket", bucket->get_name());
	    formatter->dump_string("tenant", bucket->get_tenant());
	    formatter->dump_int("num_objects", num_objects);
	    formatter->dump_int("num_shards", num_shards);
	    formatter->dump_int("objects_per_shard", objs_per_shard);
	    formatter->dump_string("fill_status", ss.str());
	    formatter->close_section();
	  }
	}
      }
      formatter->flush(cout);
    } while (buckets.is_truncated()); /* foreach: bucket */

    formatter->close_section();
    formatter->close_section();
    formatter->flush(cout);

  } /* foreach: user_id */

  formatter->close_section();
  formatter->flush(cout);

  return ret;
} /* RGWBucketAdminOp::limit_check */

int RGWBucketAdminOp::info(rgw::sal::Store* store,
			   RGWBucketAdminOpState& op_state,
			   RGWFormatterFlusher& flusher,
			   optional_yield y,
                           const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;
  int ret = 0;
  const std::string& bucket_name = op_state.get_bucket_name();
  if (!bucket_name.empty()) {
    ret = bucket.init(store, op_state, null_yield, dpp);
    if (-ENOENT == ret)
      return -ERR_NO_SUCH_BUCKET;
    else if (ret < 0)
      return ret;
  }

  Formatter *formatter = flusher.get_formatter();
  flusher.start(0);

  CephContext *cct = store->ctx();

  const size_t max_entries = cct->_conf->rgw_list_buckets_max_chunk;

  const bool show_stats = op_state.will_fetch_stats();
  const rgw_user& user_id = op_state.get_user_id();
  if (op_state.is_user_op()) {
    formatter->open_array_section("buckets");

    rgw::sal::BucketList buckets;
    std::unique_ptr<rgw::sal::User> user = store->get_user(op_state.get_user_id());
    std::string marker;
    const std::string empty_end_marker;
    constexpr bool no_need_stats = false; // set need_stats to false

    do {
      ret = user->list_buckets(dpp, marker, empty_end_marker, max_entries,
			      no_need_stats, buckets, y);
      if (ret < 0) {
        return ret;
      }

      const std::string* marker_cursor = nullptr;
      map<string, std::unique_ptr<rgw::sal::Bucket>>& m = buckets.get_buckets();

      for (const auto& i : m) {
        const std::string& obj_name = i.first;
        if (!bucket_name.empty() && bucket_name != obj_name) {
          continue;
        }

        if (show_stats) {
          bucket_stats(store, user_id.tenant, obj_name, formatter, dpp);
	} else {
          formatter->dump_string("bucket", obj_name);
	}

        marker_cursor = &obj_name;
      } // for loop
      if (marker_cursor) {
	marker = *marker_cursor;
      }

      flusher.flush();
    } while (buckets.is_truncated());

    formatter->close_section();
  } else if (!bucket_name.empty()) {
    ret = bucket_stats(store, user_id.tenant, bucket_name, formatter, dpp);
    if (ret < 0) {
      return ret;
    }
  } else {
    void *handle = nullptr;
    bool truncated = true;

    formatter->open_array_section("buckets");
    ret = store->meta_list_keys_init(dpp, "bucket", string(), &handle);
    while (ret == 0 && truncated) {
      std::list<std::string> buckets;
      constexpr int max_keys = 1000;
      ret = store->meta_list_keys_next(handle, max_keys, buckets,
						   &truncated);
      for (auto& bucket_name : buckets) {
        if (show_stats) {
          bucket_stats(store, user_id.tenant, bucket_name, formatter, dpp);
	} else {
          formatter->dump_string("bucket", bucket_name);
	}
      }
    }
    store->meta_list_keys_complete(handle);

    formatter->close_section();
  }

  flusher.flush();

  return 0;
}

int RGWBucketAdminOp::set_quota(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp)
{
  RGWBucket bucket;

  int ret = bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0)
    return ret;
  return bucket.set_quota(op_state, dpp);
}

static int purge_bucket_instance(rgw::sal::Store* store, const RGWBucketInfo& bucket_info, const DoutPrefixProvider *dpp)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = store->get_bucket(nullptr, bucket_info, &bucket);
  if (ret < 0)
    return ret;

  return bucket->purge_instance(dpp);
}

inline auto split_tenant(const std::string& bucket_name){
  auto p = bucket_name.find('/');
  if(p != std::string::npos) {
    return std::make_pair(bucket_name.substr(0,p), bucket_name.substr(p+1));
  }
  return std::make_pair(std::string(), bucket_name);
}

using bucket_instance_ls = std::vector<RGWBucketInfo>;
void get_stale_instances(rgw::sal::Store* store, const std::string& bucket_name,
                         const vector<std::string>& lst,
                         bucket_instance_ls& stale_instances,
                         const DoutPrefixProvider *dpp)
{

  bucket_instance_ls other_instances;
// first iterate over the entries, and pick up the done buckets; these
// are guaranteed to be stale
  for (const auto& bucket_instance : lst){
    RGWBucketInfo binfo;
    std::unique_ptr<rgw::sal::Bucket> bucket;
    rgw_bucket rbucket;
    rgw_bucket_parse_bucket_key(store->ctx(), bucket_instance, &rbucket, nullptr);
    int r = store->get_bucket(dpp, nullptr, rbucket, &bucket, null_yield);
    if (r < 0){
      // this can only happen if someone deletes us right when we're processing
      ldpp_dout(dpp, -1) << "Bucket instance is invalid: " << bucket_instance
                          << cpp_strerror(-r) << dendl;
      continue;
    }
    binfo = bucket->get_info();
    if (binfo.reshard_status == cls_rgw_reshard_status::DONE)
      stale_instances.emplace_back(std::move(binfo));
    else {
      other_instances.emplace_back(std::move(binfo));
    }
  }

  // Read the cur bucket info, if the bucket doesn't exist we can simply return
  // all the instances
  auto [tenant, bname] = split_tenant(bucket_name);
  RGWBucketInfo cur_bucket_info;
  std::unique_ptr<rgw::sal::Bucket> cur_bucket;
  int r = store->get_bucket(dpp, nullptr, tenant, bname, &cur_bucket, null_yield);
  if (r < 0) {
    if (r == -ENOENT) {
      // bucket doesn't exist, everything is stale then
      stale_instances.insert(std::end(stale_instances),
                             std::make_move_iterator(other_instances.begin()),
                             std::make_move_iterator(other_instances.end()));
    } else {
      // all bets are off if we can't read the bucket, just return the sureshot stale instances
      ldpp_dout(dpp, -1) << "error: reading bucket info for bucket: "
                          << bname << cpp_strerror(-r) << dendl;
    }
    return;
  }

  // Don't process further in this round if bucket is resharding
  cur_bucket_info = cur_bucket->get_info();
  if (cur_bucket_info.reshard_status == cls_rgw_reshard_status::IN_PROGRESS)
    return;

  other_instances.erase(std::remove_if(other_instances.begin(), other_instances.end(),
                                       [&cur_bucket_info](const RGWBucketInfo& b){
                                         return (b.bucket.bucket_id == cur_bucket_info.bucket.bucket_id ||
                                                 b.bucket.bucket_id == cur_bucket_info.new_bucket_instance_id);
                                       }),
                        other_instances.end());

  // check if there are still instances left
  if (other_instances.empty()) {
    return;
  }

  // Now we have a bucket with instances where the reshard status is none, this
  // usually happens when the reshard process couldn't complete, lockdown the
  // bucket and walk through these instances to make sure no one else interferes
  // with these
  {
    RGWBucketReshardLock reshard_lock(static_cast<rgw::sal::RadosStore*>(store), cur_bucket->get_info(), true);
    r = reshard_lock.lock();
    if (r < 0) {
      // most likely bucket is under reshard, return the sureshot stale instances
      ldpp_dout(dpp, 5) << __func__
                             << "failed to take reshard lock; reshard underway likey" << dendl;
      return;
    }
    auto sg = make_scope_guard([&reshard_lock](){ reshard_lock.unlock();} );
    // this should be fast enough that we may not need to renew locks and check
    // exit status?, should we read the values of the instances again?
    stale_instances.insert(std::end(stale_instances),
                           std::make_move_iterator(other_instances.begin()),
                           std::make_move_iterator(other_instances.end()));
  }

  return;
}

static int process_stale_instances(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                                   RGWFormatterFlusher& flusher,
                                   const DoutPrefixProvider *dpp,
                                   std::function<void(const bucket_instance_ls&,
                                                      Formatter *,
                                                      rgw::sal::Store*)> process_f)
{
  std::string marker;
  void *handle;
  Formatter *formatter = flusher.get_formatter();
  static constexpr auto default_max_keys = 1000;

  int ret = store->meta_list_keys_init(dpp, "bucket.instance", marker, &handle);
  if (ret < 0) {
    cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
    return ret;
  }

  bool truncated;

  formatter->open_array_section("keys");
  auto g = make_scope_guard([&store, &handle, &formatter]() {
                              store->meta_list_keys_complete(handle);
                              formatter->close_section(); // keys
                              formatter->flush(cout);
                            });

  do {
    list<std::string> keys;

    ret = store->meta_list_keys_next(handle, default_max_keys, keys, &truncated);
    if (ret < 0 && ret != -ENOENT) {
      cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
      return ret;
    } if (ret != -ENOENT) {
      // partition the list of buckets by buckets as the listing is un sorted,
      // since it would minimize the reads to bucket_info
      std::unordered_map<std::string, std::vector<std::string>> bucket_instance_map;
      for (auto &key: keys) {
        auto pos = key.find(':');
        if(pos != std::string::npos)
          bucket_instance_map[key.substr(0,pos)].emplace_back(std::move(key));
      }
      for (const auto& kv: bucket_instance_map) {
        bucket_instance_ls stale_lst;
        get_stale_instances(store, kv.first, kv.second, stale_lst, dpp);
        process_f(stale_lst, formatter, store);
      }
    }
  } while (truncated);

  return 0;
}

int RGWBucketAdminOp::list_stale_instances(rgw::sal::Store* store,
                                           RGWBucketAdminOpState& op_state,
                                           RGWFormatterFlusher& flusher,
                                           const DoutPrefixProvider *dpp)
{
  auto process_f = [](const bucket_instance_ls& lst,
                      Formatter *formatter,
                      rgw::sal::Store*){
                     for (const auto& binfo: lst)
                       formatter->dump_string("key", binfo.bucket.get_key());
                   };
  return process_stale_instances(store, op_state, flusher, dpp, process_f);
}


int RGWBucketAdminOp::clear_stale_instances(rgw::sal::Store* store,
                                            RGWBucketAdminOpState& op_state,
                                            RGWFormatterFlusher& flusher,
                                            const DoutPrefixProvider *dpp)
{
  auto process_f = [dpp](const bucket_instance_ls& lst,
                      Formatter *formatter,
                      rgw::sal::Store* store){
                     for (const auto &binfo: lst) {
                       int ret = purge_bucket_instance(store, binfo, dpp);
                       if (ret == 0){
                         auto md_key = "bucket.instance:" + binfo.bucket.get_key();
                         ret = store->meta_remove(dpp, md_key, null_yield);
                       }
                       formatter->open_object_section("delete_status");
                       formatter->dump_string("bucket_instance", binfo.bucket.get_key());
                       formatter->dump_int("status", -ret);
                       formatter->close_section();
                     }
                   };

  return process_stale_instances(store, op_state, flusher, dpp, process_f);
}

static int fix_single_bucket_lc(rgw::sal::Store* store,
                                const std::string& tenant_name,
                                const std::string& bucket_name,
                                const DoutPrefixProvider *dpp)
{
  std::unique_ptr<rgw::sal::Bucket> bucket;
  int ret = store->get_bucket(dpp, nullptr, tenant_name, bucket_name, &bucket, null_yield);
  if (ret < 0) {
    // TODO: Should we handle the case where the bucket could've been removed between
    // listing and fetching?
    return ret;
  }

  return rgw::lc::fix_lc_shard_entry(dpp, store, store->get_rgwlc()->get_lc(), bucket.get());
}

static void format_lc_status(Formatter* formatter,
                             const std::string& tenant_name,
                             const std::string& bucket_name,
                             int status)
{
  formatter->open_object_section("bucket_entry");
  std::string entry = tenant_name.empty() ? bucket_name : tenant_name + "/" + bucket_name;
  formatter->dump_string("bucket", entry);
  formatter->dump_int("status", status);
  formatter->close_section(); // bucket_entry
}

static void process_single_lc_entry(rgw::sal::Store* store,
				    Formatter *formatter,
                                    const std::string& tenant_name,
                                    const std::string& bucket_name,
                                    const DoutPrefixProvider *dpp)
{
  int ret = fix_single_bucket_lc(store, tenant_name, bucket_name, dpp);
  format_lc_status(formatter, tenant_name, bucket_name, -ret);
}

int RGWBucketAdminOp::fix_lc_shards(rgw::sal::Store* store,
                                    RGWBucketAdminOpState& op_state,
                                    RGWFormatterFlusher& flusher,
                                    const DoutPrefixProvider *dpp)
{
  std::string marker;
  void *handle;
  Formatter *formatter = flusher.get_formatter();
  static constexpr auto default_max_keys = 1000;

  bool truncated;
  if (const std::string& bucket_name = op_state.get_bucket_name();
      ! bucket_name.empty()) {
    const rgw_user user_id = op_state.get_user_id();
    process_single_lc_entry(store, formatter, user_id.tenant, bucket_name, dpp);
    formatter->flush(cout);
  } else {
    int ret = store->meta_list_keys_init(dpp, "bucket", marker, &handle);
    if (ret < 0) {
      std::cerr << "ERROR: can't get key: " << cpp_strerror(-ret) << std::endl;
      return ret;
    }

    {
      formatter->open_array_section("lc_fix_status");
      auto sg = make_scope_guard([&store, &handle, &formatter](){
                                   store->meta_list_keys_complete(handle);
                                   formatter->close_section(); // lc_fix_status
                                   formatter->flush(cout);
                                 });
      do {
        list<std::string> keys;
        ret = store->meta_list_keys_next(handle, default_max_keys, keys, &truncated);
        if (ret < 0 && ret != -ENOENT) {
          std::cerr << "ERROR: lists_keys_next(): " << cpp_strerror(-ret) << std::endl;
          return ret;
        } if (ret != -ENOENT) {
          for (const auto &key:keys) {
            auto [tenant_name, bucket_name] = split_tenant(key);
            process_single_lc_entry(store, formatter, tenant_name, bucket_name, dpp);
          }
        }
        formatter->flush(cout); // regularly flush every 1k entries
      } while (truncated);
    }

  }
  return 0;

}

static bool has_object_expired(const DoutPrefixProvider *dpp,
			       rgw::sal::Store* store,
			       rgw::sal::Bucket* bucket,
			       const rgw_obj_key& key, utime_t& delete_at)
{
  std::unique_ptr<rgw::sal::Object> obj = bucket->get_object(key);
  bufferlist delete_at_bl;

  int ret = rgw_object_get_attr(dpp, store, obj.get(), RGW_ATTR_DELETE_AT, delete_at_bl, null_yield);
  if (ret < 0) {
    return false;  // no delete at attr, proceed
  }

  ret = decode_bl(delete_at_bl, delete_at);
  if (ret < 0) {
    return false;  // failed to parse
  }

  if (delete_at <= ceph_clock_now() && !delete_at.is_zero()) {
    return true;
  }

  return false;
}

static int fix_bucket_obj_expiry(const DoutPrefixProvider *dpp,
				 rgw::sal::Store* store,
				 rgw::sal::Bucket* bucket,
				 RGWFormatterFlusher& flusher, bool dry_run)
{
  if (bucket->get_key().bucket_id == bucket->get_key().marker) {
    ldpp_dout(dpp, -1) << "Not a resharded bucket skipping" << dendl;
    return 0;  // not a resharded bucket, move along
  }

  Formatter *formatter = flusher.get_formatter();
  formatter->open_array_section("expired_deletion_status");
  auto sg = make_scope_guard([&formatter] {
			       formatter->close_section();
			       formatter->flush(std::cout);
			     });

  rgw::sal::Bucket::ListParams params;
  rgw::sal::Bucket::ListResults results;

  params.list_versions = bucket->versioned();
  params.allow_unordered = true;

  do {
    int ret = bucket->list(dpp, params, listing_max_entries, results, null_yield);
    if (ret < 0) {
      ldpp_dout(dpp, -1) << "ERROR failed to list objects in the bucket" << dendl;
      return ret;
    }
    for (const auto& obj : results.objs) {
      rgw_obj_key key(obj.key);
      utime_t delete_at;
      if (has_object_expired(dpp, store, bucket, key, delete_at)) {
	formatter->open_object_section("object_status");
	formatter->dump_string("object", key.name);
	formatter->dump_stream("delete_at") << delete_at;

	if (!dry_run) {
	  ret = rgw_remove_object(dpp, store, bucket, key);
	  formatter->dump_int("status", ret);
	}

	formatter->close_section();  // object_status
      }
    }
    formatter->flush(cout); // regularly flush every 1k entries
  } while (results.is_truncated);

  return 0;
}

int RGWBucketAdminOp::fix_obj_expiry(rgw::sal::Store* store,
				     RGWBucketAdminOpState& op_state,
				     RGWFormatterFlusher& flusher,
                                     const DoutPrefixProvider *dpp, bool dry_run)
{
  RGWBucket admin_bucket;
  int ret = admin_bucket.init(store, op_state, null_yield, dpp);
  if (ret < 0) {
    ldpp_dout(dpp, -1) << "failed to initialize bucket" << dendl;
    return ret;
  }
  std::unique_ptr<rgw::sal::Bucket> bucket;
  ret = store->get_bucket(nullptr, admin_bucket.get_bucket_info(), &bucket);
  if (ret < 0) {
    return ret;
  }

  return fix_bucket_obj_expiry(dpp, store, bucket.get(), flusher, dry_run);
}
