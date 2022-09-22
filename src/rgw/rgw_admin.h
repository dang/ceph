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

#pragma once

#include <string>
#include <vector>
#include <set>
#include <map>
#include "common/Formatter.h"
#include "rgw_sal.h"
#include "rgw_user.h"


namespace rgw_admin {

enum class OPT;

struct CMD {
  std::any id{0};
  bool raw_storage{false};
  bool read_only{false};
  bool gc{false};
  bool master_only{false};

  CMD(std::any _id, bool _rs, bool _ro, bool _gc, bool _mo) :
	id(_id), raw_storage(_rs), read_only(_ro), gc(_gc), master_only(_mo) {}
  CMD() = default;

  operator OPT() { return std::any_cast<OPT>(id); }
  bool operator==(const OPT o) const { return o == std::any_cast<OPT>(id); }
  bool operator!=(const OPT o) const { return o != std::any_cast<OPT>(id); }
};

class SimpleCmd {
public:
  struct Def {
    std::string cmd;
    std::any opt;
  };

  using Aliases = std::vector<std::set<std::string> >;
  using Commands = std::vector<Def>;

private:
  struct Node {
    std::map<std::string, Node> next;
    std::set<std::string> expected; /* separate un-normalized list */
    std::any opt;
  };

  Node cmd_root;
  std::map<std::string, std::string> alias_map;

  std::string normalize_alias(const std::string& s) const {
    auto iter = alias_map.find(s);
    if (iter == alias_map.end()) {
      return s;
    }

    return iter->second;
  }
  void init_alias_map(Aliases& aliases) {
    for (auto& alias_set : aliases) {
      std::optional<std::string> first;

      for (auto& alias : alias_set) {
        if (!first) {
          first = alias;
        } else {
          alias_map[alias] = *first;
        }
      }
    }
  }

  bool gen_next_expected(Node *node, std::vector<std::string> *expected, bool ret) {
    for (auto& next_cmd : node->expected) {
      expected->push_back(next_cmd);
    }
    return ret;
  }

  Node root;

public:
  SimpleCmd() {}

  SimpleCmd(std::optional<Commands> cmds,
            std::optional<Aliases> aliases) {
    if (aliases) {
      add_aliases(*aliases);
    }

    if (cmds) {
      add_commands(*cmds);
    }
  }

  void add_aliases(Aliases& aliases) {
    init_alias_map(aliases);
  }

  void add_commands(std::vector<Def>& cmds) {
    for (auto& cmd : cmds) {
      std::vector<std::string> words;
      get_str_vec(cmd.cmd, " ", words);

      auto node = &cmd_root;
      for (auto& word : words) {
        auto norm = normalize_alias(word);
        auto parent = node;

        node->expected.insert(word);

        node = &node->next[norm];

        if (norm == "[*]") { /* optional param at the end */
          parent->next["*"] = *node; /* can be also looked up by '*' */
          parent->opt = cmd.opt;
        }
      }

      node->opt = cmd.opt;
    }
  }

  template <class Container>
  bool find_command(Container& args,
                    std::any *opt_cmd,
                    std::vector<std::string> *extra_args,
                    std::string *error,
                    std::vector<std::string> *expected) {
    auto node = &cmd_root;

    std::optional<std::any> found_opt;

    for (auto& arg : args) {
      std::string norm = normalize_alias(arg);
      auto iter = node->next.find(norm);
      if (iter == node->next.end()) {
        iter = node->next.find("*");
        if (iter == node->next.end()) {
          *error = std::string("ERROR: Unrecognized argument: '") + arg + "'";
          return gen_next_expected(node, expected, false);
        }
        extra_args->push_back(arg);
        if (!found_opt) {
          found_opt = node->opt;
        }
      }
      node = &(iter->second);
    }

    *opt_cmd = found_opt.value_or(node->opt);

    if (!opt_cmd->has_value()) {
      *error ="ERROR: Unknown command";
      return gen_next_expected(node, expected, false);
    }

    return true;
  }
};

struct AdminArgs {
  std::unique_ptr<rgw::sal::User> user;
  std::string tenant;
  std::string access_key;
  std::string secret_key;
  std::string bucket_name;
  std::string pool_name;
  std::string object;
  std::string date;
  std::string subuser;
  std::string format;
  std::string start_date;
  std::string end_date;
  std::string period_id;
  std::string period_epoch;
  std::string remote;
  std::string url;
  std::optional<std::string> opt_region;
  std::string realm_name;
  std::string realm_id;
  std::string realm_new_name;
  std::string zone_name;
  std::string zone_id;
  std::string zonegroup_name;
  std::string zonegroup_id;
  std::string api_name;
  std::string role_name;
  std::string path;
  std::string assume_role_doc;
  std::string policy_name;
  std::string perm_policy_doc;
  std::string path_prefix;
  std::string max_session_duration;
  std::optional<std::string> opt_redirect_zone;
  std::list<std::string> endpoints;
  int max_concurrent_ios{32};
  uint64_t orphan_stale_secs{(24 * 3600)};
  std::string job_id;
  std::unique_ptr<Formatter> formatter;
  std::unique_ptr<Formatter> zone_formatter;
  int num_shards{0};
  bool num_shards_specified{false};
  std::optional<std::string> rgw_obj_fs; // radoslist field separator
  int extra_info = false;
  int detail = false;
  std::optional<bool> sync_from_all;
  std::list<std::string> sync_from;
  std::list<std::string> sync_from_rm;
  int set_default{0};
  std::optional<bool> opt_is_master;
  std::optional<bool> opt_read_only;
  int commit{false};
  int staging{false};
  int key_type{KEY_TYPE_UNDEFINED};
  std::string bucket_id;
  std::string new_bucket_name;
  int show_log_entries{true};
  int show_log_sum{true};
  int skip_zero_entries{false};  // log show
  int fix{false};
  int remove_bad{false};
  int check_head_obj_locator{false};
  std::map<std::string, bool> categories;
  std::string infile;
  std::string metadata_key;
  std::string totp_serial;
  std::string totp_seed;
  std::string totp_seed_type{"hex"};
  std::vector<std::string> totp_pin;
  int totp_seconds{0};
  int totp_window{0};
  std::string marker;
  std::string start_marker;
  std::string end_marker;
  int max_entries{-1};
  bool max_entries_specified{false};
  std::optional<int> opt_admin;
  std::optional<int> opt_system;
  std::optional<int> shard_id;
  std::string quota_scope;
  std::string ratelimit_scope;
  std::string object_version;
  std::string placement_id;
  std::optional<std::string> opt_storage_class;
  std::list<std::string> tags;
  std::list<std::string> tags_add;
  std::list<std::string> tags_rm;
  std::optional<int64_t> max_objects;
  std::optional<int64_t> max_size;
  std::optional<int64_t> max_read_ops;
  std::optional<int64_t> max_write_ops;
  std::optional<int64_t> max_read_bytes;
  std::optional<int64_t> max_write_bytes;
  int include_all{false};
  int allow_unordered{false};
  int sync_stats{false};
  int reset_stats{false};
  int bypass_gc{false};
  int warnings_only{false};
  int inconsistent_index{false};
  int verbose{false};
  uint64_t min_rewrite_size{4 * 1024 * 1024};
  uint64_t max_rewrite_size{ULLONG_MAX};
  uint64_t min_rewrite_stripe_size{0};
  std::string index_type_str;
  std::optional<log_type> opt_log_type;
  std::optional<int> bucket_index_max_shards;
  rgw_zone_id source_zone;
  std::optional<std::string> opt_tier_type;
  std::map<std::string, std::string, ltstr_nocase> tier_config_add;
  std::map<std::string, std::string, ltstr_nocase> tier_config_rm;
  
  int yes_i_really_mean_it{false};
};

class AdminStore {
  public:
    virtual ~AdminStore() {}

    virtual void add_cmds(SimpleCmd* cmd) = 0;
    virtual int process_cmd(CMD opt_cmd, rgw::sal::Store* store, AdminArgs* admin_args, RGWUserAdminOpState* user_op, RGWUser* ruser) = 0;
};

class AdminStoreRados : public AdminStore {
  public:
    virtual ~AdminStoreRados() {}

    virtual void add_cmds(SimpleCmd* cmd) override;
    virtual int process_cmd(CMD opt_cmd, rgw::sal::Store* store, AdminArgs* admin_args, RGWUserAdminOpState* user_op, RGWUser* ruser) override;
};

class AdminStoreDBStore : public AdminStore {
  public:
    virtual ~AdminStoreDBStore() {}

    virtual void add_cmds(SimpleCmd* cmd) override;
    virtual int process_cmd(CMD opt_cmd, rgw::sal::Store* store, AdminArgs* admin_args, RGWUserAdminOpState* user_op, RGWUser* ruser) override;
};

} // namespace rgw_admin
