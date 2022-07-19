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

#include "rgw_sal_filter.h"

namespace rgw { namespace sal {


class SampleFilter : public FilterStore {
private:
  const DoutPrefixProvider* save_dpp;
public:
  SampleFilter(Store* _next) : FilterStore(_next) {}
  virtual ~SampleFilter() = default;

  virtual int initialize(CephContext *cct, const DoutPrefixProvider *dpp) override;
  virtual std::unique_ptr<User> get_user(const rgw_user& u) override;
  virtual int get_user_by_access_key(const DoutPrefixProvider* dpp, const
				     std::string& key, optional_yield y,
				     std::unique_ptr<User>* user) override;
  virtual int get_user_by_email(const DoutPrefixProvider* dpp, const
				std::string& email, optional_yield y,
				std::unique_ptr<User>* user) override;
  virtual int get_user_by_swift(const DoutPrefixProvider* dpp, const
				std::string& user_str, optional_yield y,
				std::unique_ptr<User>* user) override;
  virtual std::unique_ptr<Object> get_object(const rgw_obj_key& k) override;
  virtual int get_bucket(User* u, const RGWBucketInfo& i,
			 std::unique_ptr<Bucket>* bucket) override;
  virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const
			 rgw_bucket& b, std::unique_ptr<Bucket>* bucket,
			 optional_yield y) override;
  virtual int get_bucket(const DoutPrefixProvider* dpp, User* u, const
			 std::string& tenant, const std::string& name,
			 std::unique_ptr<Bucket>* bucket, optional_yield y) override;

  const DoutPrefixProvider* dpp() { return save_dpp; }
};

class SampleObject : public FilterObject {
private:
  const DoutPrefixProvider* save_dpp;

public:
  SampleObject(std::unique_ptr<Object> _next, const DoutPrefixProvider* _dpp) :
	    FilterObject(std::move(_next)), save_dpp(_dpp) {}
  SampleObject(std::unique_ptr<Object> _next, Bucket* _bucket, const DoutPrefixProvider* _dpp) :
			FilterObject(std::move(_next), _bucket), save_dpp(_dpp) {}
  SampleObject(SampleObject& _o) : FilterObject(_o) {
    save_dpp = _o.save_dpp;
  }
  virtual ~SampleObject() = default;
  virtual void set_atomic() override;
  virtual int get_obj_state(const DoutPrefixProvider* dpp, RGWObjState **state,
			    optional_yield y, bool follow_olh = true) override;

  const DoutPrefixProvider* dpp() { return save_dpp; }
};


} } // namespace rgw::sal
