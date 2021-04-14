// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#pragma once

#define TIME_BUF_SIZE 128

#include <mutex>
#include <string_view>

#include <boost/container/static_vector.hpp>
#include <boost/crc.hpp>

#include "common/sstring.hh"
#include "rgw_op.h"
#include "rgw_rest.h"
#include "rgw_http_errors.h"

class RGWGetBucketReplication_ObjStore_S3 : public RGWGetBucketReplication_ObjStore
{
public:
  void send_response_data() override;
};

class RGWPutBucketReplication_ObjStore_S3 : public RGWPutBucketReplication_ObjStore
{
public:
  int get_params(optional_yield y) override;
  void send_response() override;
};

class RGWDeleteBucketReplication_ObjStore_S3 : public RGWDeleteBucketReplication_ObjStore
{
protected:
  void update_sync_policy(rgw_sync_policy_info *policy) override;
public:
  void send_response() override;
};
