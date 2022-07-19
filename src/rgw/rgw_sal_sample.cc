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

#include "rgw_sal_sample.h"

namespace rgw { namespace sal {

int SampleFilter::initialize(CephContext *cct, const DoutPrefixProvider* dpp)
{
  int ret = FilterStore::initialize(cct, dpp);
  if (ret < 0)
    return ret;

  save_dpp = dpp;

  ldpp_dout(dpp, 0) << __func__ << dendl;
  return 0;
}

std::unique_ptr<User> SampleFilter::get_user(const rgw_user &u)
{
  ldpp_dout(dpp(), 0) << __func__ << dendl;
  return FilterStore::get_user(u);
}

int SampleFilter::get_user_by_access_key(const DoutPrefixProvider* dpp, const std::string& key, optional_yield y, std::unique_ptr<User>* user)
{
  ldpp_dout(dpp, 0) << __func__ << dendl;
  return FilterStore::get_user_by_access_key(dpp, key, y, user);
}

int SampleFilter::get_user_by_email(const DoutPrefixProvider* dpp, const std::string& email, optional_yield y, std::unique_ptr<User>* user)
{
  ldpp_dout(dpp, 0) << __func__ << dendl;
  return FilterStore::get_user_by_email(dpp, email, y, user);
}

int SampleFilter::get_user_by_swift(const DoutPrefixProvider* dpp, const std::string& user_str, optional_yield y, std::unique_ptr<User>* user)
{
  ldpp_dout(dpp, 0) << __func__ << dendl;
  return FilterStore::get_user_by_swift(dpp, user_str, y, user);
}

std::unique_ptr<Object> SampleFilter::get_object(const rgw_obj_key& k)
{
  ldpp_dout(dpp(), 0) << __func__ << dendl;
  std::unique_ptr<Object> o = next->get_object(k);
  return std::make_unique<SampleObject>(std::move(o), dpp());
}

int SampleFilter::get_bucket(const DoutPrefixProvider* dpp, User* u, const rgw_bucket& b, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  ldpp_dout(dpp, 0) << __func__ << dendl;
  return FilterStore::get_bucket(dpp, u, b, bucket, y);
}

int SampleFilter::get_bucket(User* u, const RGWBucketInfo& i, std::unique_ptr<Bucket>* bucket)
{
  ldpp_dout(dpp(), 0) << __func__ << dendl;
  return FilterStore::get_bucket(u, i, bucket);
}

int SampleFilter::get_bucket(const DoutPrefixProvider* dpp, User* u, const std::string& tenant, const std::string& name, std::unique_ptr<Bucket>* bucket, optional_yield y)
{
  ldpp_dout(dpp, 0) << __func__ << dendl;
  return FilterStore::get_bucket(dpp, u, tenant, name, bucket, y);
}


void SampleObject::set_atomic()
{
  ldpp_dout(dpp(), 0) << __func__ << dendl;
  return FilterObject::set_atomic();
}

int SampleObject::get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **pstate,
				optional_yield y, bool follow_olh)
{
  ldpp_dout(dpp, 0) << __func__ << dendl;
  return FilterObject::get_obj_state(dpp, pstate, y, follow_olh);
}


} } // namespace rgw::sal

extern "C" {

rgw::sal::Store* newSampleFilter(rgw::sal::Store* next)
{
  rgw::sal::SampleFilter* store = new rgw::sal::SampleFilter(next);

  return store;
}

}
