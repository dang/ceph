// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#ifndef CEPH_RGW_BUCKET_ADMIN_H
#define CEPH_RGW_BUCKET_ADMIN_H

#include <string>
#include <memory>
#include <variant>

#include <boost/container/flat_map.hpp>
#include <boost/container/flat_set.hpp>

#include "include/types.h"
#include "rgw_common.h"
#include "rgw_tools.h"
#include "rgw_metadata.h"

#include "rgw_string.h"
#include "rgw_sal.h"

#include "common/Formatter.h"
#include "common/lru_map.h"
#include "common/ceph_time.h"

#include "rgw_formats.h"

#include "services/svc_bucket_types.h"
#include "services/svc_bucket_sync.h"

class RGWBucketAdminOp
{
public:
  static int get_policy(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp);
  static int get_policy(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  RGWAccessControlPolicy& policy, const DoutPrefixProvider *dpp);
  static int dump_s3_policy(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  ostream& os, const DoutPrefixProvider *dpp);

  static int unlink(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp);
  static int link(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, string *err_msg = NULL);
  static int chown(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const string& marker, const DoutPrefixProvider *dpp, string *err_msg = NULL);

  static int check_index(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                  RGWFormatterFlusher& flusher, optional_yield y, const DoutPrefixProvider *dpp);

  static int remove_bucket(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, optional_yield y,
			   const DoutPrefixProvider *dpp, bool bypass_gc = false, bool keep_index_consistent = true);
  static int remove_object(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp);
  static int info(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, RGWFormatterFlusher& flusher, optional_yield y, const DoutPrefixProvider *dpp);
  static int limit_check(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
			 const std::list<std::string>& user_ids,
			 RGWFormatterFlusher& flusher, optional_yield y,
                         const DoutPrefixProvider *dpp,
			 bool warnings_only = false);
  static int set_quota(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp);

  static int list_stale_instances(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
				  RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp);

  static int clear_stale_instances(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
				   RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp);
  static int fix_lc_shards(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
                           RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp);
  static int fix_obj_expiry(rgw::sal::Store* store, RGWBucketAdminOpState& op_state,
			    RGWFormatterFlusher& flusher, const DoutPrefixProvider *dpp, bool dry_run = false);

  static int sync_bucket(rgw::sal::Store* store, RGWBucketAdminOpState& op_state, const DoutPrefixProvider *dpp, string *err_msg = NULL);
};


#endif
