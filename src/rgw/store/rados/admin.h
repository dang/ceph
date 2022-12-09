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

#include "rgw_admin.h"

namespace rgw_admin {

class AdminStoreRados : public AdminStore {
  public:
    virtual ~AdminStoreRados() {}

    virtual void add_cmds(SimpleCmd* cmd) override;
    virtual int process_cmd(CMD opt_cmd, rgw::sal::Store* store, AdminArgs* admin_args, RGWUserAdminOpState* user_op, RGWUser* ruser) override;
};

} // namespace rgw_admin
