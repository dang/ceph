// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2022 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#include "store/rados/admin.h"
#include "rgw_rados.h"
#include "rgw_bucket_sync.h"
#include "rgw_orphan.h"
#include "rgw_pubsub.h"
#include "rgw_reshard.h"
#include "rgw_sync.h"
#include "rgw_sync_checkpoint.h"
#include "rgw_trim_bilog.h"
#include "rgw_trim_datalog.h"
#include "rgw_trim_mdlog.h"
#include "rgw_zone.h"

#include "services/svc_sync_modules.h"
#include "services/svc_cls.h"
#include "services/svc_bilog_rados.h"
#include "services/svc_mdlog.h"
#include "services/svc_meta_be_otp.h"
#include "services/svc_user.h"
#include "services/svc_zone.h"

namespace rgw_admin {

static const DoutPrefixProvider* dpp() {
  struct GlobalPrefix : public DoutPrefixProvider {
    CephContext *get_cct() const override { return dout_context; }
    unsigned get_subsys() const override { return dout_subsys; }
    std::ostream& gen_prefix(std::ostream& out) const override { return out; }
  };
  static GlobalPrefix global_dpp;
  return &global_dpp;
}

static void get_md_sync_status(list<string>& status)
{
  RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor());

  int ret = sync.init(dpp());
  if (ret < 0) {
    status.push_back(string("failed to retrieve sync info: sync.init() failed: ") + cpp_strerror(-ret));
    return;
  }

  rgw_meta_sync_status sync_status;
  ret = sync.read_sync_status(dpp(), &sync_status);
  if (ret < 0) {
    status.push_back(string("failed to read sync status: ") + cpp_strerror(-ret));
    return;
  }

  string status_str;
  switch (sync_status.sync_info.state) {
    case rgw_meta_sync_info::StateInit:
      status_str = "init";
      break;
    case rgw_meta_sync_info::StateBuildingFullSyncMaps:
      status_str = "preparing for full sync";
      break;
    case rgw_meta_sync_info::StateSync:
      status_str = "syncing";
      break;
    default:
      status_str = "unknown";
  }

  status.push_back(status_str);

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  int num_full = 0;
  int num_inc = 0;
  int total_shards = 0;
  set<int> shards_behind_set;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    total_shards++;
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
      num_full++;
      full_complete += marker_iter.second.pos;
      int shard_id = marker_iter.first;
      shards_behind_set.insert(shard_id);
    } else {
      full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync) {
      num_inc++;
    }
  }

  stringstream ss;
  push_ss(ss, status) << "full sync: " << num_full << "/" << total_shards << " shards";

  if (num_full > 0) {
    push_ss(ss, status) << "full sync: " << full_total - full_complete << " entries to sync";
  }

  push_ss(ss, status) << "incremental sync: " << num_inc << "/" << total_shards << " shards";

  map<int, RGWMetadataLogInfo> master_shards_info;
  string master_period = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_current_period_id();

  ret = sync.read_master_log_shards_info(dpp(), master_period, &master_shards_info);
  if (ret < 0) {
    status.push_back(string("failed to fetch master sync status: ") + cpp_strerror(-ret));
    return;
  }

  map<int, string> shards_behind;
  if (sync_status.sync_info.period != master_period) {
    status.push_back(string("master is on a different period: master_period=" +
                            master_period + " local_period=" + sync_status.sync_info.period));
  } else {
    for (auto local_iter : sync_status.sync_markers) {
      int shard_id = local_iter.first;
      auto iter = master_shards_info.find(shard_id);

      if (iter == master_shards_info.end()) {
        /* huh? */
        derr << "ERROR: could not find remote sync shard status for shard_id=" << shard_id << dendl;
        continue;
      }
      auto master_marker = iter->second.marker;
      if (local_iter.second.state == rgw_meta_sync_marker::SyncState::IncrementalSync &&
          master_marker > local_iter.second.marker) {
        shards_behind[shard_id] = local_iter.second.marker;
        shards_behind_set.insert(shard_id);
      }
    }
  }

  // fetch remote log entries to determine the oldest change
  std::optional<std::pair<int, ceph::real_time>> oldest;
  if (!shards_behind.empty()) {
    map<int, rgw_mdlog_shard_data> master_pos;
    ret = sync.read_master_log_shards_next(dpp(), sync_status.sync_info.period, shards_behind, &master_pos);
    if (ret < 0) {
      derr << "ERROR: failed to fetch master next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      for (auto iter : master_pos) {
        rgw_mdlog_shard_data& shard_data = iter.second;

        if (shard_data.entries.empty()) {
          // there aren't any entries in this shard, so we're not really behind
          shards_behind.erase(iter.first);
          shards_behind_set.erase(iter.first);
        } else {
          rgw_mdlog_entry& entry = shard_data.entries.front();
          if (!oldest) {
            oldest.emplace(iter.first, entry.timestamp);
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest->second) {
            oldest.emplace(iter.first, entry.timestamp);
          }
        }
      }
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  if (total_behind == 0) {
    push_ss(ss, status) << "metadata is caught up with master";
  } else {
    push_ss(ss, status) << "metadata is behind on " << total_behind << " shards";
    push_ss(ss, status) << "behind shards: " << "[" << shards_behind_set << "]";
    if (oldest) {
      push_ss(ss, status) << "oldest incremental change not applied: "
          << oldest->second << " [" << oldest->first << ']';
    }
  }

  flush_ss(ss, status);
}

static void get_data_sync_status(const rgw_zone_id& source_zone, list<string>& status, int tab)
{
  stringstream ss;

  RGWZone *sz;

  if (!(sz = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->find_zone(source_zone))) {
    push_ss(ss, status, tab) << string("zone not found");
    flush_ss(ss, status);
    return;
  }

  if (!static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->zone_syncs_from(static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_zone(), *sz)) {
    push_ss(ss, status, tab) << string("not syncing from zone");
    flush_ss(ss, status);
    return;
  }
  RGWDataSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor(), source_zone, nullptr);

  int ret = sync.init(dpp());
  if (ret < 0) {
    push_ss(ss, status, tab) << string("failed to retrieve sync info: ") + cpp_strerror(-ret);
    flush_ss(ss, status);
    return;
  }

  rgw_data_sync_status sync_status;
  ret = sync.read_sync_status(dpp(), &sync_status);
  if (ret < 0 && ret != -ENOENT) {
    push_ss(ss, status, tab) << string("failed read sync status: ") + cpp_strerror(-ret);
    return;
  }

  set<int> recovering_shards;
  ret = sync.read_recovering_shards(dpp(), sync_status.sync_info.num_shards, recovering_shards);
  if (ret < 0 && ret != ENOENT) {
    push_ss(ss, status, tab) << string("failed read recovering shards: ") + cpp_strerror(-ret);
    return;
  }

  string status_str;
  switch (sync_status.sync_info.state) {
    case rgw_data_sync_info::StateInit:
      status_str = "init";
      break;
    case rgw_data_sync_info::StateBuildingFullSyncMaps:
      status_str = "preparing for full sync";
      break;
    case rgw_data_sync_info::StateSync:
      status_str = "syncing";
      break;
    default:
      status_str = "unknown";
  }

  push_ss(ss, status, tab) << status_str;

  uint64_t full_total = 0;
  uint64_t full_complete = 0;

  int num_full = 0;
  int num_inc = 0;
  int total_shards = 0;
  set<int> shards_behind_set;

  for (auto marker_iter : sync_status.sync_markers) {
    full_total += marker_iter.second.total_entries;
    total_shards++;
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::FullSync) {
      num_full++;
      full_complete += marker_iter.second.pos;
      int shard_id = marker_iter.first;
      shards_behind_set.insert(shard_id);
    } else {
      full_complete += marker_iter.second.total_entries;
    }
    if (marker_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync) {
      num_inc++;
    }
  }

  push_ss(ss, status, tab) << "full sync: " << num_full << "/" << total_shards << " shards";

  if (num_full > 0) {
    push_ss(ss, status, tab) << "full sync: " << full_total - full_complete << " buckets to sync";
  }

  push_ss(ss, status, tab) << "incremental sync: " << num_inc << "/" << total_shards << " shards";

  map<int, RGWDataChangesLogInfo> source_shards_info;

  ret = sync.read_source_log_shards_info(dpp(), &source_shards_info);
  if (ret < 0) {
    push_ss(ss, status, tab) << string("failed to fetch source sync status: ") + cpp_strerror(-ret);
    return;
  }

  map<int, string> shards_behind;

  for (auto local_iter : sync_status.sync_markers) {
    int shard_id = local_iter.first;
    auto iter = source_shards_info.find(shard_id);

    if (iter == source_shards_info.end()) {
      /* huh? */
      derr << "ERROR: could not find remote sync shard status for shard_id=" << shard_id << dendl;
      continue;
    }
    auto master_marker = iter->second.marker;
    if (local_iter.second.state == rgw_data_sync_marker::SyncState::IncrementalSync &&
        master_marker > local_iter.second.marker) {
      shards_behind[shard_id] = local_iter.second.marker;
      shards_behind_set.insert(shard_id);
    }
  }

  int total_behind = shards_behind.size() + (sync_status.sync_info.num_shards - num_inc);
  int total_recovering = recovering_shards.size();
  if (total_behind == 0 && total_recovering == 0) {
    push_ss(ss, status, tab) << "data is caught up with source";
  } else if (total_behind > 0) {
    push_ss(ss, status, tab) << "data is behind on " << total_behind << " shards";

    push_ss(ss, status, tab) << "behind shards: " << "[" << shards_behind_set << "]" ;

    map<int, rgw_datalog_shard_data> master_pos;
    ret = sync.read_source_log_shards_next(dpp(), shards_behind, &master_pos);
    if (ret < 0) {
      derr << "ERROR: failed to fetch next positions (" << cpp_strerror(-ret) << ")" << dendl;
    } else {
      std::optional<std::pair<int, ceph::real_time>> oldest;

      for (auto iter : master_pos) {
        rgw_datalog_shard_data& shard_data = iter.second;

        if (!shard_data.entries.empty()) {
          rgw_datalog_entry& entry = shard_data.entries.front();
          if (!oldest) {
            oldest.emplace(iter.first, entry.timestamp);
          } else if (!ceph::real_clock::is_zero(entry.timestamp) && entry.timestamp < oldest->second) {
            oldest.emplace(iter.first, entry.timestamp);
          }
        }
      }

      if (oldest) {
        push_ss(ss, status, tab) << "oldest incremental change not applied: "
            << oldest->second << " [" << oldest->first << ']';
      }
    }
  }

  if (total_recovering > 0) {
    push_ss(ss, status, tab) << total_recovering << " shards are recovering";
    push_ss(ss, status, tab) << "recovering shards: " << "[" << recovering_shards << "]";
  }

  flush_ss(ss, status);
}

static void sync_status(Formatter *formatter)
{
  const rgw::sal::ZoneGroup& zonegroup = store->get_zone()->get_zonegroup();
  rgw::sal::Zone* zone = store->get_zone();

  int width = 15;

  cout << std::setw(width) << "realm" << std::setw(1) << " " << zone->get_realm_id() << " (" << zone->get_realm_name() << ")" << std::endl;
  cout << std::setw(width) << "zonegroup" << std::setw(1) << " " << zonegroup.get_id() << " (" << zonegroup.get_name() << ")" << std::endl;
  cout << std::setw(width) << "zone" << std::setw(1) << " " << zone->get_id() << " (" << zone->get_name() << ")" << std::endl;
  cout << std::setw(width) << "current time" << std::setw(1) << " "
       << to_iso_8601(ceph::real_clock::now(), iso_8601_format::YMDhms) << std::endl;

  const auto& rzg =
    static_cast<const rgw::sal::RadosZoneGroup&>(zonegroup).get_group();

  cout << std::setw(width) << "zonegroup features enabled: " << rzg.enabled_features << std::endl;
  if (auto d = get_disabled_features(rzg.enabled_features); !d.empty()) {
    cout << std::setw(width) << "                   disabled: " << d << std::endl;
  }

  list<string> md_status;

  if (store->is_meta_master()) {
    md_status.push_back("no sync (zone is master)");
  } else {
    get_md_sync_status(md_status);
  }

  tab_dump("metadata sync", width, md_status);

  list<string> data_status;

  auto& zone_conn_map = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->get_zone_conn_map();

  for (auto iter : zone_conn_map) {
    const rgw_zone_id& source_id = iter.first;
    string source_str = "source: ";
    string s = source_str + source_id.id;
    std::unique_ptr<rgw::sal::Zone> sz;
    if (store->get_zone()->get_zonegroup().get_zone_by_id(source_id.id, &sz) == 0) {
      s += string(" (") + sz->get_name() + ")";
    }
    data_status.push_back(s);
    get_data_sync_status(source_id, data_status, source_str.size());
  }

  tab_dump("data sync", width, data_status);
}

static SimpleCmd::Commands radosstore_cmds = {
  { "bi get", CMD(OPT::BI_GET, false, true, false, false) },
  { "bi put", CMD(OPT::BI_PUT, false, false, false, false) },
  { "bi list", CMD(OPT::BI_LIST, false, true, false, false) },
  { "bi purge", CMD(OPT::BI_PURGE, false, false, false, false) },
  { "bucket radoslist", CMD(OPT::BUCKET_RADOS_LIST, false, false, false, false) },
  { "bucket rados list", CMD(OPT::BUCKET_RADOS_LIST, false, false, false, false) },
  { "bucket reshard", CMD(OPT::BUCKET_RESHARD, false, false, false, false) },
  { "data sync status", CMD(OPT::DATA_SYNC_STATUS, false, true, false, false) },
  { "data sync init", CMD(OPT::DATA_SYNC_INIT, false, false, false, false) },
  { "data sync run", CMD(OPT::DATA_SYNC_RUN, false, false, true, false) },
  { "metadata sync status", CMD(OPT::METADATA_SYNC_STATUS, false, true, false, false) },
  { "metadata sync init", CMD(OPT::METADATA_SYNC_INIT, false, false, false, false) },
  { "metadata sync run", CMD(OPT::METADATA_SYNC_RUN, false, false, false, false) },
  { "mfa create", CMD(OPT::MFA_CREATE, false, false, false, true) },
  { "mfa remove", CMD(OPT::MFA_REMOVE, false, false, false, true) },
  { "mfa get", CMD(OPT::MFA_GET, false, false, false, false) },
  { "mfa list", CMD(OPT::MFA_LIST, false, false, false, false) },
  { "mfa check", CMD(OPT::MFA_CHECK, false, false, false, false) },
  { "mfa resync", CMD(OPT::MFA_RESYNC, false, false, false, true) },
  { "orphans find", CMD(OPT::ORPHANS_FIND, false, false, false, false) },
  { "orphans finish", CMD(OPT::ORPHANS_FINISH, false, false, false, false) },
  { "orphans list jobs", CMD(OPT::ORPHANS_LIST_JOBS, false, true, false, false) },
  { "orphans list-jobs", CMD(OPT::ORPHANS_LIST_JOBS, false, true, false, false) },
  { "pool add", CMD(OPT::POOL_ADD, false, false, false, false) },
  { "pool rm", CMD(OPT::POOL_RM, false, false, false, false) },
  { "pool list", CMD(OPT::POOLS_LIST, false, false, false, false) },
  { "pools list", CMD(OPT::POOLS_LIST, false, false, false, false) },
  { "topic list", CMD(OPT::PUBSUB_TOPICS_LIST, false, true, false, false) },
  { "topic get", CMD(OPT::PUBSUB_TOPIC_GET, false, true, false, false) },
  { "topic rm", CMD(OPT::PUBSUB_TOPIC_RM, false, false, false, false) },
  { "subscription get", CMD(OPT::PUBSUB_SUB_GET, false, true, false, false) },
  { "subscription rm", CMD(OPT::PUBSUB_SUB_RM, false, false, false, false) },
  { "subscription pull", CMD(OPT::PUBSUB_SUB_PULL, false, true, false, false) },
  { "subscription ack", CMD(OPT::PUBSUB_EVENT_RM, false, false, false, false) },
  { "reshard add", CMD(OPT::RESHARD_ADD, false, false, false, false) },
  { "reshard list", CMD(OPT::RESHARD_LIST, false, true, false, false) },
  { "reshard status", CMD(OPT::RESHARD_STATUS, false, true, false, false) },
  { "reshard process", CMD(OPT::RESHARD_PROCESS, false, false, false, false) },
  { "reshard cancel", CMD(OPT::RESHARD_CANCEL, false, false, false, false) },
  { "reshard stale-instances list", CMD(OPT::RESHARD_STALE_INSTANCES_LIST, false, false, false, false) },
  { "reshard stale list", CMD(OPT::RESHARD_STALE_INSTANCES_LIST, false, false, false, false) },
  { "reshard stale-instances delete", CMD(OPT::RESHARD_STALE_INSTANCES_DELETE, false, false, false, false) },
  { "reshard stale delete", CMD(OPT::RESHARD_STALE_INSTANCES_DELETE, false, false, false, false) },
  { "sync info", CMD(OPT::SYNC_INFO, false, true, false, false) },
  { "sync status", CMD(OPT::SYNC_STATUS, false, true, false, false) },
};

void AdminStoreRados::add_cmds(SimpleCmd* cmd)
{
  cmd->add_commands(radosstore_cmds);
}

int AdminStoreRados::process_cmd(CMD opt_cmd, rgw::sal::Store* store, AdminArgs* admin_args, RGWUserAdminOpState* user_op, RGWUser* ruser)
{
  int ret;
  rgw_pool pool;
  RGWObjVersionTracker objv_tracker;
  std::unique_ptr<rgw::sal::Bucket> bucket;

  if (!admin_args->pool_name.empty())
    pool = rgw_pool(admin_args->pool_name);

  if (opt_cmd == OPT::BUCKET_RADOS_LIST) {
    RGWRadosList lister(static_cast<rgw::sal::RadosStore*>(store),
			admin_args->max_concurrent_ios, admin_args->orphan_stale_secs, admin_args->tenant);
    if (admin_args->rgw_obj_fs) {
      lister.set_field_separator(*admin_args->rgw_obj_fs);
    }

    if (admin_args->bucket_name.empty()) {
      // yes_i_really_mean_it means continue with listing even if
      // there are indexless buckets
      ret = lister.run(dpp(), admin_args->yes_i_really_mean_it);
    } else {
      ret = lister.run(dpp(), admin_args->bucket_name);
    }

    if (ret < 0) {
      std::cerr <<
	"ERROR: bucket radoslist failed to finish before " <<
	"encountering error: " << cpp_strerror(-ret) << std::endl;
      std::cerr << "************************************"
	"************************************" << std::endl;
      std::cerr << "WARNING: THE RESULTS ARE NOT RELIABLE AND SHOULD NOT " <<
	"BE USED IN DELETING ORPHANS" << std::endl;
      std::cerr << "************************************"
	"************************************" << std::endl;
      return -ret;
    }
  }
  if (opt_cmd == OPT::POOL_ADD) {
    if (admin_args->pool_name.empty()) {
      cerr << "need to specify pool to add!" << std::endl;
      exit(1);
    }

    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->add_bucket_placement(dpp(), pool, null_yield);
    if (ret < 0)
      cerr << "failed to add bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT::POOL_RM) {
    if (admin_args->pool_name.empty()) {
      cerr << "need to specify pool to remove!" << std::endl;
      exit(1);
    }

    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->remove_bucket_placement(dpp(), pool, null_yield);
    if (ret < 0)
      cerr << "failed to remove bucket placement: " << cpp_strerror(-ret) << std::endl;
  }

  if (opt_cmd == OPT::POOLS_LIST) {
    set<rgw_pool> pools;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone->list_placement_set(dpp(), pools, null_yield);
    if (ret < 0) {
      cerr << "could not list placement set: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    admin_args->formatter->reset();
    admin_args->formatter->open_array_section("pools");
    for (auto siter = pools.begin(); siter != pools.end(); ++siter) {
      admin_args->formatter->open_object_section("pool");
      admin_args->formatter->dump_string("name",  siter->to_str());
      admin_args->formatter->close_section();
    }
    admin_args->formatter->close_section();
    admin_args->formatter->flush(cout);
    cout << std::endl;
  }

  if (opt_cmd == OPT::ORPHANS_FIND) {
    if (!admin_args->yes_i_really_mean_it) {
      cerr << "this command is now deprecated; please consider using the rgw-orphan-list tool; "
	   << "accidental removal of active objects cannot be reversed; "
	   << "do you really mean it? (requires --yes-i-really-mean-it)"
	   << std::endl;
      return EINVAL;
    } else {
      cerr << "IMPORTANT: this command is now deprecated; please consider using the rgw-orphan-list tool"
	   << std::endl;
    }

    RGWOrphanSearch search(static_cast<rgw::sal::RadosStore*>(store), admin_args->max_concurrent_ios, admin_args->orphan_stale_secs);

    if (admin_args->job_id.empty()) {
      cerr << "ERROR: --job-id not specified" << std::endl;
      return EINVAL;
    }
    if (admin_args->pool_name.empty()) {
      cerr << "ERROR: --pool not specified" << std::endl;
      return EINVAL;
    }

    RGWOrphanSearchInfo info;

    info.pool = pool;
    info.job_name = admin_args->job_id;
    info.num_shards = admin_args->num_shards;

    int ret = search.init(dpp(), admin_args->job_id, &info, admin_args->detail);
    if (ret < 0) {
      cerr << "could not init search, ret=" << ret << std::endl;
      return -ret;
    }
    ret = search.run(dpp());
    if (ret < 0) {
      return -ret;
    }
  }

  if (opt_cmd == OPT::ORPHANS_FINISH) {
    if (!admin_args->yes_i_really_mean_it) {
      cerr << "this command is now deprecated; please consider using the rgw-orphan-list tool; "
	   << "accidental removal of active objects cannot be reversed; "
	   << "do you really mean it? (requires --yes-i-really-mean-it)"
	   << std::endl;
      return EINVAL;
    } else {
      cerr << "IMPORTANT: this command is now deprecated; please consider using the rgw-orphan-list tool"
	   << std::endl;
    }

    RGWOrphanSearch search(static_cast<rgw::sal::RadosStore*>(store), admin_args->max_concurrent_ios, admin_args->orphan_stale_secs);

    if (admin_args->job_id.empty()) {
      cerr << "ERROR: --job-id not specified" << std::endl;
      return EINVAL;
    }
    int ret = search.init(dpp(), admin_args->job_id, NULL);
    if (ret < 0) {
      if (ret == -ENOENT) {
        cerr << "job not found" << std::endl;
      }
      return -ret;
    }
    ret = search.finish();
    if (ret < 0) {
      return -ret;
    }
  }

  if (opt_cmd == OPT::ORPHANS_LIST_JOBS) {
    if (!admin_args->yes_i_really_mean_it) {
      cerr << "this command is now deprecated; please consider using the rgw-orphan-list tool; "
	   << "do you really mean it? (requires --yes-i-really-mean-it)"
	   << std::endl;
      return EINVAL;
    } else {
      cerr << "IMPORTANT: this command is now deprecated; please consider using the rgw-orphan-list tool"
	   << std::endl;
    }

    RGWOrphanStore orphan_store(static_cast<rgw::sal::RadosStore*>(store));
    int ret = orphan_store.init(dpp());
    if (ret < 0){
      cerr << "connection to cluster failed!" << std::endl;
      return -ret;
    }

    map <string,RGWOrphanSearchState> m;
    ret = orphan_store.list_jobs(m);
    if (ret < 0) {
      cerr << "job list failed" << std::endl;
      return -ret;
    }
    admin_args->formatter->open_array_section("entries");
    for (const auto &it: m){
      if (!admin_args->extra_info){
	admin_args->formatter->dump_string("job-id",it.first);
      } else {
	encode_json("orphan_search_state", it.second, admin_args->formatter.get());
      }
    }
    admin_args->formatter->close_section();
    admin_args->formatter->flush(cout);
  }

  if (opt_cmd == OPT::MFA_CREATE) {
    rados::cls::otp::otp_info_t config;

    if (rgw::sal::User::empty(admin_args->user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (admin_args->totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    if (admin_args->totp_seed.empty()) {
      cerr << "ERROR: TOTP device seed was not provided (via --totp-seed)" << std::endl;
      return EINVAL;
    }


    rados::cls::otp::SeedType seed_type;
    if (admin_args->totp_seed_type == "hex") {
      seed_type = rados::cls::otp::OTP_SEED_HEX;
    } else if (admin_args->totp_seed_type == "base32") {
      seed_type = rados::cls::otp::OTP_SEED_BASE32;
    } else {
      cerr << "ERROR: invalid seed type: " << admin_args->totp_seed_type << std::endl;
      return EINVAL;
    }

    config.id = admin_args->totp_serial;
    config.seed = admin_args->totp_seed;
    config.seed_type = seed_type;

    if (admin_args->totp_seconds > 0) {
      config.step_size = admin_args->totp_seconds;
    }

    if (admin_args->totp_window > 0) {
      config.window = admin_args->totp_window;
    }

    real_time mtime = real_clock::now();
    string oid = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.get_mfa_oid(admin_args->user->get_id());

    int ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->meta.mgr->mutate(RGWSI_MetaBackend_OTP::get_meta_key(admin_args->user->get_id()),
					     mtime, &objv_tracker,
					     null_yield, dpp(),
					     MDLOG_STATUS_WRITE,
					     [&] {
      return static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.create_mfa(dpp(), admin_args->user->get_id(), config, &objv_tracker, mtime, null_yield);
    });
    if (ret < 0) {
      cerr << "MFA creation failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    
    RGWUserInfo& user_info = user_op->get_user_info();
    user_info.mfa_ids.insert(admin_args->totp_serial);
    user_op->set_mfa_ids(user_info.mfa_ids);
    string err;
    ret = ruser->modify(dpp(), *user_op, null_yield, &err);
    if (ret < 0) {
      cerr << "ERROR: failed storing user info, error: " << err << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::MFA_REMOVE) {
    if (rgw::sal::User::empty(admin_args->user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (admin_args->totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    real_time mtime = real_clock::now();

    int ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->meta.mgr->mutate(RGWSI_MetaBackend_OTP::get_meta_key(admin_args->user->get_id()),
					     mtime, &objv_tracker,
					     null_yield, dpp(),
					     MDLOG_STATUS_WRITE,
					     [&] {
      return static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.remove_mfa(dpp(), admin_args->user->get_id(), admin_args->totp_serial, &objv_tracker, mtime, null_yield);
    });
    if (ret < 0) {
      cerr << "MFA removal failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWUserInfo& user_info = user_op->get_user_info();
    user_info.mfa_ids.erase(admin_args->totp_serial);
    user_op->set_mfa_ids(user_info.mfa_ids);
    string err;
    ret = ruser->modify(dpp(), *user_op, null_yield, &err);
    if (ret < 0) {
      cerr << "ERROR: failed storing user info, error: " << err << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::MFA_GET) {
    if (rgw::sal::User::empty(admin_args->user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (admin_args->totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    rados::cls::otp::otp_info_t result;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.get_mfa(dpp(), admin_args->user->get_id(), admin_args->totp_serial, &result, null_yield);
    if (ret < 0) {
      if (ret == -ENOENT || ret == -ENODATA) {
        cerr << "MFA serial id not found" << std::endl;
      } else {
        cerr << "MFA retrieval failed, error: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }
    admin_args->formatter->open_object_section("result");
    encode_json("entry", result, admin_args->formatter.get());
    admin_args->formatter->close_section();
    admin_args->formatter->flush(cout);
  }

  if (opt_cmd == OPT::MFA_LIST) {
    if (rgw::sal::User::empty(admin_args->user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    list<rados::cls::otp::otp_info_t> result;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.list_mfa(dpp(), admin_args->user->get_id(), &result, null_yield);
    if (ret < 0) {
      cerr << "MFA listing failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    admin_args->formatter->open_object_section("result");
    encode_json("entries", result, admin_args->formatter.get());
    admin_args->formatter->close_section();
    admin_args->formatter->flush(cout);
  }

  if (opt_cmd == OPT::MFA_CHECK) {
    if (rgw::sal::User::empty(admin_args->user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (admin_args->totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    if (admin_args->totp_pin.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-pin)" << std::endl;
      return EINVAL;
    }

    list<rados::cls::otp::otp_info_t> result;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.check_mfa(dpp(), admin_args->user->get_id(), admin_args->totp_serial, admin_args->totp_pin.front(), null_yield);
    if (ret < 0) {
      cerr << "MFA check failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    cout << "ok" << std::endl;
  }

  if (opt_cmd == OPT::MFA_RESYNC) {
    if (rgw::sal::User::empty(admin_args->user)) {
      cerr << "ERROR: user id was not provided (via --uid)" << std::endl;
      return EINVAL;
    }

    if (admin_args->totp_serial.empty()) {
      cerr << "ERROR: TOTP device serial number was not provided (via --totp-serial)" << std::endl;
      return EINVAL;
    }

    if (admin_args->totp_pin.size() != 2) {
      cerr << "ERROR: missing two --totp-pin params (--totp-pin=<first> --totp-pin=<second>)" << std::endl;
      return EINVAL;
    }

    rados::cls::otp::otp_info_t config;
    int ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.get_mfa(dpp(), admin_args->user->get_id(), admin_args->totp_serial, &config, null_yield);
    if (ret < 0) {
      if (ret == -ENOENT || ret == -ENODATA) {
        cerr << "MFA serial id not found" << std::endl;
      } else {
        cerr << "MFA retrieval failed, error: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }

    ceph::real_time now;

    ret = static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.otp_get_current_time(dpp(), admin_args->user->get_id(), &now, null_yield);
    if (ret < 0) {
      cerr << "ERROR: failed to fetch current time from osd: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    time_t time_ofs;

    ret = scan_totp(store->ctx(), now, config, admin_args->totp_pin, &time_ofs);
    if (ret < 0) {
      if (ret == -ENOENT) {
        cerr << "failed to resync, TOTP values not found in range" << std::endl;
      } else {
        cerr << "ERROR: failed to scan for TOTP values: " << cpp_strerror(-ret) << std::endl;
      }
      return -ret;
    }

    config.time_ofs = time_ofs;

    /* now update the backend */
    real_time mtime = real_clock::now();

    ret = static_cast<rgw::sal::RadosStore*>(store)->ctl()->meta.mgr->mutate(RGWSI_MetaBackend_OTP::get_meta_key(admin_args->user->get_id()),
				         mtime, &objv_tracker,
				         null_yield, dpp(),
				         MDLOG_STATUS_WRITE,
				         [&] {
      return static_cast<rgw::sal::RadosStore*>(store)->svc()->cls->mfa.create_mfa(dpp(), admin_args->user->get_id(), config, &objv_tracker, mtime, null_yield);
    });
    if (ret < 0) {
      cerr << "MFA update failed, error: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::BI_GET) {
    if (admin_args->bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    if (admin_args->object.empty()) {
      cerr << "ERROR: object not specified" << std::endl;
      return EINVAL;
    }
    BIIndexType bi_index_type = BIIndexType::Plain;
    if (!admin_args->index_type_str.empty()) {
      bi_index_type = get_bi_index_type(admin_args->index_type_str);
      if (bi_index_type == BIIndexType::Invalid) {
	cerr << "ERROR: invalid bucket index entry type" << std::endl;
	return EINVAL;
      }
    }
    int ret = init_bucket(admin_args->user.get(), admin_args->tenant, admin_args->bucket_name, admin_args->bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    rgw_obj obj(bucket->get_key(), admin_args->object);
    if (!admin_args->object_version.empty()) {
      obj.key.set_instance(admin_args->object_version);
    }

    rgw_cls_bi_entry entry;

    ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->bi_get(dpp(), bucket->get_info(), obj, bi_index_type, &entry);
    if (ret < 0) {
      cerr << "ERROR: bi_get(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    encode_json("entry", entry, admin_args->formatter.get());
    admin_args->formatter->flush(cout);
  }

  if (opt_cmd == OPT::BI_PUT) {
    if (admin_args->bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(admin_args->user.get(), admin_args->tenant, admin_args->bucket_name, admin_args->bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    rgw_cls_bi_entry entry;
    cls_rgw_obj_key key;
    ret = read_decode_json(admin_args->infile, entry, &key);
    if (ret < 0) {
      return 1;
    }

    rgw_obj obj(bucket->get_key(), key);

    ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->bi_put(dpp(), bucket->get_key(), obj, entry);
    if (ret < 0) {
      cerr << "ERROR: bi_put(): " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::BI_LIST) {
    if (admin_args->bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(admin_args->user.get(), admin_args->tenant, admin_args->bucket_name, admin_args->bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    list<rgw_cls_bi_entry> entries;
    bool is_truncated;
    if (admin_args->max_entries < 0) {
      admin_args->max_entries = 1000;
    }

    const auto& index = bucket->get_info().layout.current_index;
    const int max_shards = rgw::num_shards(index);

    admin_args->formatter->open_array_section("entries");

    int i = safe_opt(admin_args->shard_id);
    for (; i < max_shards; i++) {
      RGWRados::BucketShard bs(static_cast<rgw::sal::RadosStore*>(store)->getRados());
      int ret = bs.init(dpp(), bucket->get_info(), index, i);
      admin_args->marker.clear();

      if (ret < 0) {
        cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << i << "): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      do {
        entries.clear();
	// if object is specified, we use that as a filter to only retrieve some some entries
        ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->bi_list(bs, admin_args->object, admin_args->marker, admin_args->max_entries, &entries, &is_truncated);
        if (ret < 0) {
          cerr << "ERROR: bi_list(): " << cpp_strerror(-ret) << std::endl;
          return -ret;
        }

        list<rgw_cls_bi_entry>::iterator iter;
        for (iter = entries.begin(); iter != entries.end(); ++iter) {
          rgw_cls_bi_entry& entry = *iter;
          encode_json("entry", entry, admin_args->formatter.get());
          admin_args->marker = entry.idx;
        }
        admin_args->formatter->flush(cout);
      } while (is_truncated);
      admin_args->formatter->flush(cout);

      if (admin_args->shard_id)
        break;
    }
    admin_args->formatter->close_section();
    admin_args->formatter->flush(cout);
  }

  if (opt_cmd == OPT::BI_PURGE) {
    if (admin_args->bucket_name.empty()) {
      cerr << "ERROR: bucket name not specified" << std::endl;
      return EINVAL;
    }
    int ret = init_bucket(admin_args->user.get(), admin_args->tenant, admin_args->bucket_name, admin_args->bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    std::unique_ptr<rgw::sal::Bucket> cur_bucket;
    ret = init_bucket(admin_args->user.get(), admin_args->tenant, admin_args->bucket_name, string(), &cur_bucket);
    if (ret == -ENOENT) {
      // no bucket entrypoint
    } else if (ret < 0) {
      cerr << "ERROR: could not init current bucket info for bucket_name=" << admin_args->bucket_name << ": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    } else if (cur_bucket->get_bucket_id() == bucket->get_bucket_id() &&
               !admin_args->yes_i_really_mean_it) {
      cerr << "specified bucket instance points to a current bucket instance" << std::endl;
      cerr << "do you really mean it? (requires --yes-i-really-mean-it)" << std::endl;
      return EINVAL;
    }

    const auto& index = bucket->get_info().layout.current_index;
    if (index.layout.type == rgw::BucketIndexType::Indexless) {
      cerr << "ERROR: indexless bucket has no index to purge" << std::endl;
      return EINVAL;
    }

    const int max_shards = rgw::num_shards(index);
    for (int i = 0; i < max_shards; i++) {
      RGWRados::BucketShard bs(static_cast<rgw::sal::RadosStore*>(store)->getRados());
      int ret = bs.init(dpp(), bucket->get_info(), index, i);
      if (ret < 0) {
        cerr << "ERROR: bs.init(bucket=" << bucket << ", shard=" << i << "): " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      ret = static_cast<rgw::sal::RadosStore*>(store)->getRados()->bi_remove(dpp(), bs);
      if (ret < 0) {
        cerr << "ERROR: failed to remove bucket index object: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
    }
  }
  if (opt_cmd == OPT::BUCKET_RESHARD) {
    int ret = check_reshard_bucket_params(store,
					  admin_args.bucket_name,
					  admin_args.tenant,
					  admin_args.bucket_id,
					  admin_args.num_shards_specified,
					  admin_args.num_shards,
					  admin_args.yes_i_really_mean_it,
					  &bucket);
    if (ret < 0) {
      return ret;
    }

    auto zone_svc = static_cast<rgw::sal::RadosStore*>(store)->svc()->zone;
    if (!zone_svc->can_reshard()) {
      const auto& zonegroup = zone_svc->get_zonegroup();
      std::cerr << "The zonegroup '" << zonegroup.get_name() << "' does not "
          "have the resharding feature enabled." << std::endl;
      return ENOTSUP;
    }
    if (!RGWBucketReshard::can_reshard(bucket->get_info(), zone_svc) &&
        !admin_args.yes_i_really_mean_it) {
      std::cerr << "Bucket '" << bucket->get_name() << "' already has too many "
          "log generations (" << bucket->get_info().layout.logs.size() << ") "
          "from previous reshards that peer zones haven't finished syncing. "
          "Resharding is not recommended until the old generations sync, but "
          "you can force a reshard with --yes-i-really-mean-it." << std::endl;
      return EINVAL;
    }

    RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(store),
			bucket->get_info(), bucket->get_attrs(),
			nullptr /* no callback */);

#define DEFAULT_RESHARD_MAX_ENTRIES 1000
    if (admin_args.max_entries < 1) {
      admin_args.max_entries = DEFAULT_RESHARD_MAX_ENTRIES;
    }

    ReshardFaultInjector fault;
    if (admin_args.inject_error_at) {
      const int code = -admin_args.inject_error_code.value_or(EIO);
      fault.inject(*admin_args.inject_error_at, InjectError{code, dpp()});
    } else if (admin_args.inject_abort_at) {
      fault.inject(*admin_args.inject_abort_at, InjectAbort{});
    }
    ret = br.execute(admin_args.num_shards, fault, admin_args.max_entries, dpp(),
                     admin_args.verbose, &cout, admin_args.formatter.get());
    return -ret;
  }

  if (opt_cmd == OPT::RESHARD_ADD) {
    int ret = check_reshard_bucket_params(store,
					  admin_args.bucket_name,
					  admin_args.tenant,
					  admin_args.bucket_id,
					  admin_args.num_shards_specified,
					  admin_args.num_shards,
					  admin_args.yes_i_really_mean_it,
					  &bucket);
    if (ret < 0) {
      return ret;
    }

    int num_source_shards = rgw::current_num_shards(bucket->get_info().layout);

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(store), dpp());
    cls_rgw_reshard_entry entry;
    entry.time = real_clock::now();
    entry.tenant = admin_args.tenant;
    entry.bucket_name = admin_args.bucket_name;
    entry.bucket_id = bucket->get_info().bucket.bucket_id;
    entry.old_num_shards = num_source_shards;
    entry.new_num_shards = admin_args.num_shards;

    return reshard.add(dpp(), entry);
  }

  if (opt_cmd == OPT::RESHARD_LIST) {
    int ret;
    int count = 0;
    if (admin_args.max_entries < 0) {
      admin_args.max_entries = 1000;
    }

    int num_logshards =
      store->ctx()->_conf.get_val<uint64_t>("rgw_reshard_num_logs");

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(store), dpp());

    admin_args.formatter->open_array_section("reshard");
    for (int i = 0; i < num_logshards; i++) {
      bool is_truncated = true;
      std::string marker;
      do {
	std::list<cls_rgw_reshard_entry> entries;
        ret = reshard.list(dpp(), i, marker, admin_args.max_entries - count, entries, &is_truncated);
        if (ret < 0) {
          cerr << "Error listing resharding buckets: " << cpp_strerror(-ret) << std::endl;
          return ret;
        }
        for (const auto& entry : entries) {
          encode_json("entry", entry, admin_args.formatter.get());
        }
	if (is_truncated) {
	  entries.crbegin()->get_key(&marker); // last entry's key becomes marker
	}
        count += entries.size();
        admin_args.formatter->flush(cout);
      } while (is_truncated && count < admin_args.max_entries);

      if (count >= admin_args.max_entries) {
        break;
      }
    }

    admin_args.formatter->close_section();
    admin_args.formatter->flush(cout);

    return 0;
  }

  if (opt_cmd == OPT::RESHARD_STATUS) {
    if (admin_args.bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    ret = init_bucket(admin_args.user.get(), admin_args.tenant, admin_args.bucket_name, admin_args.bucket_id, &bucket);
    if (ret < 0) {
      cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }

    RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(store),
			bucket->get_info(), bucket->get_attrs(),
			nullptr /* no callback */);
    list<cls_rgw_bucket_instance_entry> status;
    int r = br.get_status(dpp(), &status);
    if (r < 0) {
      cerr << "ERROR: could not get resharding status for bucket " <<
	admin_args.bucket_name << std::endl;
      return -r;
    }

    show_reshard_status(status, admin_args.formatter.get());
  }

  if (opt_cmd == OPT::RESHARD_PROCESS) {
    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(store), true, &cout);

    int ret = reshard.process_all_logshards(dpp());
    if (ret < 0) {
      cerr << "ERROR: failed to process reshard logs, error=" << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::RESHARD_CANCEL) {
    if (admin_args.bucket_name.empty()) {
      cerr << "ERROR: bucket not specified" << std::endl;
      return EINVAL;
    }

    bool bucket_initable = true;
    ret = init_bucket(admin_args.user.get(), admin_args.tenant, admin_args.bucket_name, admin_args.bucket_id, &bucket);
    if (ret < 0) {
      if (admin_args.yes_i_really_mean_it) {
        bucket_initable = false;
      } else {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) <<
          "; if you want to cancel the reshard request nonetheless, please "
          "use the --yes-i-really-mean-it option" << std::endl;
        return -ret;
      }
    }

    bool resharding_underway = true;

    if (bucket_initable) {
      // we did not encounter an error, so let's work with the bucket
	RGWBucketReshard br(static_cast<rgw::sal::RadosStore*>(store),
			    bucket->get_info(), bucket->get_attrs(),
			    nullptr /* no callback */);
      int ret = br.cancel(dpp());
      if (ret < 0) {
        if (ret == -EBUSY) {
          cerr << "There is ongoing resharding, please retry after " <<
            store->ctx()->_conf.get_val<uint64_t>("rgw_reshard_bucket_lock_duration") <<
            " seconds." << std::endl;
	  return -ret;
	} else if (ret == -EINVAL) {
	  resharding_underway = false;
	  // we can continue and try to unschedule
        } else {
          cerr << "Error cancelling bucket \"" << admin_args.bucket_name <<
            "\" resharding: " << cpp_strerror(-ret) << std::endl;
	  return -ret;
        }
      }
    }

    RGWReshard reshard(static_cast<rgw::sal::RadosStore*>(store), dpp());

    cls_rgw_reshard_entry entry;
    entry.tenant = admin_args.tenant;
    entry.bucket_name = admin_args.bucket_name;

    ret = reshard.remove(dpp(), entry);
    if (ret == -ENOENT) {
      if (!resharding_underway) {
	cerr << "Error, bucket \"" << admin_args.bucket_name <<
	  "\" is neither undergoing resharding nor scheduled to undergo "
	  "resharding." << std::endl;
	return EINVAL;
      } else {
	// we cancelled underway resharding above, so we're good
	return 0;
      }
    } else if (ret < 0) {
      cerr << "Error in updating reshard log with bucket \"" <<
        admin_args.bucket_name << "\": " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  } // OPT_RESHARD_CANCEL

  if (opt_cmd == OPT::PUBSUB_TOPICS_LIST) {

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), admin_args.tenant);

    if (!admin_args.bucket_name.empty()) {
      rgw_pubsub_bucket_topics result;
      int ret = init_bucket(admin_args.user.get(), admin_args.tenant, admin_args.bucket_name, admin_args.bucket_id, &bucket);
      if (ret < 0) {
        cerr << "ERROR: could not init bucket: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }

      auto b = ps.get_bucket(bucket->get_key());
      ret = b->get_topics(&result);
      if (ret < 0) {
        cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      encode_json("result", result, admin_args.formatter.get());
    } else {
      rgw_pubsub_topics result;
      int ret = ps.get_topics(&result);
      if (ret < 0) {
        cerr << "ERROR: could not get topics: " << cpp_strerror(-ret) << std::endl;
        return -ret;
      }
      encode_json("result", result, admin_args.formatter.get());
    }
    admin_args.formatter->flush(cout);
  }

  if (opt_cmd == OPT::PUBSUB_TOPIC_GET) {
    if (admin_args.topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), admin_args.tenant);

    rgw_pubsub_topic_subs topic;
    ret = ps.get_topic(admin_args.topic_name, &topic);
    if (ret < 0) {
      cerr << "ERROR: could not get topic: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("topic", topic, admin_args.formatter.get());
    admin_args.formatter->flush(cout);
  }

  if (opt_cmd == OPT::PUBSUB_TOPIC_RM) {
    if (admin_args.topic_name.empty()) {
      cerr << "ERROR: topic name was not provided (via --topic)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), admin_args.tenant);

    ret = ps.remove_topic(dpp(), admin_args.topic_name, null_yield);
    if (ret < 0) {
      cerr << "ERROR: could not remove topic: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::PUBSUB_SUB_GET) {
    if (store->get_zone()->get_tier_type() != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (admin_args.sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --subscription)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), admin_args.tenant);

    rgw_pubsub_sub_config sub_conf;

    auto sub = ps.get_sub(admin_args.sub_name);
    ret = sub->get_conf(&sub_conf);
    if (ret < 0) {
      cerr << "ERROR: could not get subscription info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("sub", sub_conf, admin_args.formatter.get());
    admin_args.formatter->flush(cout);
  }

 if (opt_cmd == OPT::PUBSUB_SUB_RM) {
    if (store->get_zone()->get_tier_type() != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (admin_args.sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --subscription)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), admin_args.tenant);

    auto sub = ps.get_sub(admin_args.sub_name);
    ret = sub->unsubscribe(dpp(), admin_args.topic_name, null_yield);
    if (ret < 0) {
      cerr << "ERROR: could not get subscription info: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

 if (opt_cmd == OPT::PUBSUB_SUB_PULL) {
    if (store->get_zone()->get_tier_type() != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (admin_args.sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --subscription)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), admin_args.tenant);

    if (!admin_args.max_entries_specified) {
      admin_args.max_entries = RGWPubSub::Sub::DEFAULT_MAX_EVENTS;
    }
    auto sub = ps.get_sub_with_events(admin_args.sub_name);
    ret = sub->list_events(dpp(), admin_args.marker, admin_args.max_entries);
    if (ret < 0) {
      cerr << "ERROR: could not list events: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
    encode_json("result", *sub, admin_args.formatter.get());
    admin_args.formatter->flush(cout);
 }

 if (opt_cmd == OPT::PUBSUB_EVENT_RM) {
    if (store->get_zone()->get_tier_type() != "pubsub") {
      cerr << "ERROR: only pubsub tier type supports this command" << std::endl;
      return EINVAL;
    }
    if (admin_args.sub_name.empty()) {
      cerr << "ERROR: subscription name was not provided (via --subscription)" << std::endl;
      return EINVAL;
    }
    if (admin_args.event_id.empty()) {
      cerr << "ERROR: event id was not provided (via --event-id)" << std::endl;
      return EINVAL;
    }

    RGWPubSub ps(static_cast<rgw::sal::RadosStore*>(store), admin_args.tenant);

    auto sub = ps.get_sub_with_events(admin_args.sub_name);
    ret = sub->remove_event(dpp(), admin_args.event_id);
    if (ret < 0) {
      cerr << "ERROR: could not remove event: " << cpp_strerror(-ret) << std::endl;
      return -ret;
    }
  }

  if (opt_cmd == OPT::SYNC_INFO) {
    sync_info(admin_args.opt_effective_zone_id, admin_args.opt_bucket, admin_args.zone_formatter.get());
  }

  if (opt_cmd == OPT::SYNC_STATUS) {
    sync_status(admin_args.formatter.get());
  }

  if (opt_cmd == OPT::METADATA_SYNC_STATUS) {
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor());

    int ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    rgw_meta_sync_status sync_status;
    ret = sync.read_sync_status(dpp(), &sync_status);
    if (ret < 0) {
      cerr << "ERROR: sync.read_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }

    admin_args.formatter->open_object_section("summary");
    encode_json("sync_status", sync_status, admin_args.formatter.get());

    uint64_t full_total = 0;
    uint64_t full_complete = 0;

    for (auto marker_iter : sync_status.sync_markers) {
      full_total += marker_iter.second.total_entries;
      if (marker_iter.second.state == rgw_meta_sync_marker::SyncState::FullSync) {
        full_complete += marker_iter.second.pos;
      } else {
        full_complete += marker_iter.second.total_entries;
      }
    }

    admin_args.formatter->open_object_section("full_sync");
    encode_json("total", full_total, admin_args.formatter.get());
    encode_json("complete", full_complete, admin_args.formatter.get());
    admin_args.formatter->close_section();
    admin_args.formatter->dump_string("current_time",
			   to_iso_8601(ceph::real_clock::now(),
				       iso_8601_format::YMDhms));
    admin_args.formatter->close_section();

    admin_args.formatter->flush(cout);

  }

  if (opt_cmd == OPT::METADATA_SYNC_INIT) {
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor());

    int ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }
    ret = sync.init_sync_status(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init_sync_status() returned ret=" << ret << std::endl;
      return -ret;
    }
  }


  if (opt_cmd == OPT::METADATA_SYNC_RUN) {
    RGWMetaSyncStatusManager sync(static_cast<rgw::sal::RadosStore*>(store), static_cast<rgw::sal::RadosStore*>(store)->svc()->rados->get_async_processor());

    int ret = sync.init(dpp());
    if (ret < 0) {
      cerr << "ERROR: sync.init() returned ret=" << ret << std::endl;
      return -ret;
    }

    ret = sync.run(dpp(), null_yield);
    if (ret < 0) {
      cerr << "ERROR: sync.run() returned ret=" << ret << std::endl;
      return -ret;
    }
  }


  return 0;
}

} // namespace rgw_admin
