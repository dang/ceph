// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2021 Red Hat, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation. See file COPYING.
 *
 */

#pragma once

namespace rgw {
  namespace sal {
    class Store;
    class User;
    class Bucket;
    class Object;
    class BucketList;
    struct MPSerializer;
    class Lifecycle;
    class Notification;
    class GCChain;
    class Writer;
    class Zone;
    class LuaScriptManager;
    class RGWOIDCProvider;
    class RGWRole;
  }
}
