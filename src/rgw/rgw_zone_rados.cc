// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab ft=cpp

#include "common/errno.h"

#include "rgw_zone_rados.h"
#include "rgw_realm_watcher.h"
#include "rgw_meta_sync_status.h"
#include "rgw_sync.h"

#include "services/svc_zone.h"
#include "services/svc_sys_obj.h"

#define dout_subsys ceph_subsys_rgw


int RGWSystemMetaObj::read_default(const DoutPrefixProvider *dpp,
                                   RGWDefaultSystemMetaObjInfo& default_info,
				   const string& oid, optional_yield y)
{
  using ceph::decode;
  rgw_pool(get_pool_name());
  bufferlist bl;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0)
    return ret;

  try {
    auto iter = bl.cbegin();
    decode(default_info, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "error decoding data from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWSystemMetaObj::read_default_id(const DoutPrefixProvider *dpp, string& default_id, optional_yield y,
				      bool old_format)
{
  RGWDefaultSystemMetaObjInfo default_info;

  int ret = read_default(dpp, default_info, get_default_oid(old_format), y);
  if (ret < 0) {
    return ret;
  }

  default_id = default_info.default_id;

  return 0;
}

int RGWSystemMetaObj::use_default(const DoutPrefixProvider *dpp, optional_yield y, bool old_format)
{
  return read_default_id(dpp, id, y, old_format);
}

int RGWSystemMetaObj::set_as_default(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  using ceph::encode;
  string oid  = get_default_oid();

  rgw_pool pool(get_pool_name());
  bufferlist bl;

  RGWDefaultSystemMetaObjInfo default_info;
  default_info.default_id = id;

  encode(default_info, bl);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  int ret = sysobj.wop()
                  .set_exclusive(exclusive)
                  .write(dpp, bl, y);
  if (ret < 0)
    return ret;

  return 0;
}

int RGWSystemMetaObj::read_id(const DoutPrefixProvider *dpp, const string& obj_name, string& object_id,
			      optional_yield y)
{
  using ceph::decode;
  rgw_pool pool(get_pool_name());
  bufferlist bl;

  string oid = get_names_oid_prefix() + obj_name;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    return ret;
  }

  RGWNameToId nameToId;
  try {
    auto iter = bl.cbegin();
    decode(nameToId, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode obj from " << pool << ":" << oid << dendl;
    return -EIO;
  }
  object_id = nameToId.obj_id;
  return 0;
}

int RGWSystemMetaObj::delete_obj(const DoutPrefixProvider *dpp, optional_yield y, bool old_format)
{
  rgw_pool pool(get_pool_name());

  auto obj_ctx = sysobj_svc->init_obj_ctx();

  /* check to see if obj is the default */
  RGWDefaultSystemMetaObjInfo default_info;
  int ret = read_default(dpp, default_info, get_default_oid(old_format), y);
  if (ret < 0 && ret != -ENOENT)
    return ret;
  if (default_info.default_id == id || (old_format && default_info.default_id == name)) {
    string oid = get_default_oid(old_format);
    rgw_raw_obj default_named_obj(pool, oid);
    auto sysobj = sysobj_svc->get_obj(obj_ctx, default_named_obj);
    ret = sysobj.wop().remove(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "Error delete default obj name  " << name << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }
  if (!old_format) {
    string oid  = get_names_oid_prefix() + name;
    rgw_raw_obj object_name(pool, oid);
    auto sysobj = sysobj_svc->get_obj(obj_ctx, object_name);
    ret = sysobj.wop().remove(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "Error delete obj name  " << name << ": " << cpp_strerror(-ret) << dendl;
      return ret;
    }
  }

  string oid = get_info_oid_prefix(old_format);
  if (old_format) {
    oid += name;
  } else {
    oid += id;
  }

  rgw_raw_obj object_id(pool, oid);
  auto sysobj = sysobj_svc->get_obj(obj_ctx, object_id);
  ret = sysobj.wop().remove(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error delete object id " << id << ": " << cpp_strerror(-ret) << dendl;
  }

  return ret;
}

int RGWSystemMetaObj::store_name(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  rgw_pool pool(get_pool_name());
  string oid = get_names_oid_prefix() + name;

  RGWNameToId nameToId;
  nameToId.obj_id = id;

  bufferlist bl;
  using ceph::encode;
  encode(nameToId, bl);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWSystemMetaObj::rename(const DoutPrefixProvider *dpp, const string& new_name, optional_yield y)
{
  string new_id;
  int ret = read_id(dpp, new_name, new_id, y);
  if (!ret) {
    return -EEXIST;
  }
  if (ret < 0 && ret != -ENOENT) {
    ldpp_dout(dpp, 0) << "Error read_id " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  string old_name = name;
  name = new_name;
  ret = update(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error storing new obj info " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  ret = store_name(dpp, true, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error storing new name " << new_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  /* delete old name */
  rgw_pool pool(get_pool_name());
  string oid = get_names_oid_prefix() + old_name;
  rgw_raw_obj old_name_obj(pool, oid);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, old_name_obj);
  ret = sysobj.wop().remove(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "Error delete old obj name  " << old_name << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  return ret;
}

int RGWSystemMetaObj::read_info(const DoutPrefixProvider *dpp, const string& obj_id, optional_yield y,
				bool old_format)
{
  rgw_pool pool(get_pool_name());

  bufferlist bl;

  string oid = get_info_oid_prefix(old_format) + obj_id;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed reading obj info from " << pool << ":" << oid << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }
  using ceph::decode;

  try {
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode obj from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWSystemMetaObj::store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  rgw_pool pool(get_pool_name());

  string oid = get_info_oid_prefix() + id;

  bufferlist bl;
  using ceph::encode;
  encode(*this, bl);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWRealm::create_control(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  rgw_pool pool(get_pool_name());
  auto oid = get_control_oid();
  bufferlist bl;
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWRealm::delete_control(const DoutPrefixProvider *dpp, optional_yield y)
{
  rgw_pool pool(get_pool_name());
  auto obj = rgw_raw_obj{pool, get_control_oid()};
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, obj);
  return sysobj.wop().remove(dpp, y);
}

int RGWRealm::notify_zone(const DoutPrefixProvider *dpp, bufferlist& bl, optional_yield y)
{
  rgw_pool pool{get_pool_name()};
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, get_control_oid()});
  int ret = sysobj.wn().notify(dpp, bl, 0, nullptr, y);
  if (ret < 0) {
    return ret;
  }
  return 0;
}

int RGWPeriodConfig::read(const DoutPrefixProvider *dpp, RGWSI_SysObj *sysobj_svc, const std::string& realm_id,
			  optional_yield y)
{
  rgw_pool pool(get_pool_name());
  const auto& oid = get_oid(realm_id);
  bufferlist bl;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    return ret;
  }
  using ceph::decode;
  try {
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    return -EIO;
  }
  return 0;
}

int RGWPeriodConfig::write(const DoutPrefixProvider *dpp,
                           RGWSI_SysObj *sysobj_svc,
			   const std::string& realm_id, optional_yield y)
{
  rgw_pool pool(get_pool_name());
  const auto& oid = get_oid(realm_id);
  bufferlist bl;
  using ceph::encode;
  encode(*this, bl);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  return sysobj.wop()
               .set_exclusive(false)
               .write(dpp, bl, y);
}

int RGWPeriod::read_latest_epoch(const DoutPrefixProvider *dpp,
                                 RGWPeriodLatestEpochInfo& info,
				 optional_yield y,
                                 RGWObjVersionTracker *objv)
{
  string oid = get_period_oid_prefix() + get_latest_epoch_oid();

  rgw_pool pool(get_pool_name());
  bufferlist bl;
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, oid});
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 1) << "error read_lastest_epoch " << pool << ":" << oid << dendl;
    return ret;
  }
  try {
    auto iter = bl.cbegin();
    using ceph::decode;
    decode(info, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "error decoding data from " << pool << ":" << oid << dendl;
    return -EIO;
  }

  return 0;
}

int RGWPeriod::set_latest_epoch(const DoutPrefixProvider *dpp,
                                optional_yield y,
				epoch_t epoch, bool exclusive,
                                RGWObjVersionTracker *objv)
{
  string oid = get_period_oid_prefix() + get_latest_epoch_oid();

  rgw_pool pool(get_pool_name());
  bufferlist bl;

  RGWPeriodLatestEpochInfo info;
  info.epoch = epoch;

  using ceph::encode;
  encode(info, bl);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWPeriod::delete_obj(const DoutPrefixProvider *dpp, optional_yield y)
{
  rgw_pool pool(get_pool_name());

  // delete the object for each period epoch
  for (epoch_t e = 1; e <= epoch; e++) {
    RGWPeriod p{get_id(), e};
    rgw_raw_obj oid{pool, p.get_period_oid()};
    auto obj_ctx = sysobj_svc->init_obj_ctx();
    auto sysobj = sysobj_svc->get_obj(obj_ctx, oid);
    int ret = sysobj.wop().remove(dpp, y);
    if (ret < 0) {
      ldpp_dout(dpp, 0) << "WARNING: failed to delete period object " << oid
          << ": " << cpp_strerror(-ret) << dendl;
    }
  }

  // delete the .latest_epoch object
  rgw_raw_obj oid{pool, get_period_oid_prefix() + get_latest_epoch_oid()};
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, oid);
  int ret = sysobj.wop().remove(dpp, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "WARNING: failed to delete period object " << oid
        << ": " << cpp_strerror(-ret) << dendl;
  }
  return ret;
}

int RGWPeriod::read_info(const DoutPrefixProvider *dpp, optional_yield y)
{
  rgw_pool pool(get_pool_name());

  bufferlist bl;

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj{pool, get_period_oid()});
  int ret = sysobj.rop().read(dpp, &bl, y);
  if (ret < 0) {
    ldpp_dout(dpp, 0) << "failed reading obj info from " << pool << ":" << get_period_oid() << ": " << cpp_strerror(-ret) << dendl;
    return ret;
  }

  try {
    using ceph::decode;
    auto iter = bl.cbegin();
    decode(*this, iter);
  } catch (buffer::error& err) {
    ldpp_dout(dpp, 0) << "ERROR: failed to decode obj from " << pool << ":" << get_period_oid() << dendl;
    return -EIO;
  }

  return 0;
}

int RGWPeriod::store_info(const DoutPrefixProvider *dpp, bool exclusive, optional_yield y)
{
  rgw_pool pool(get_pool_name());

  string oid = get_period_oid();
  bufferlist bl;
  using ceph::encode;
  encode(*this, bl);

  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, rgw_raw_obj(pool, oid));
  return sysobj.wop()
               .set_exclusive(exclusive)
               .write(dpp, bl, y);
}

int RGWZoneParams::create(const DoutPrefixProvider *dpp, optional_yield y, bool exclusive)
{
  /* check for old pools config */
  rgw_raw_obj obj(domain_root, avail_pools);
  auto obj_ctx = sysobj_svc->init_obj_ctx();
  auto sysobj = sysobj_svc->get_obj(obj_ctx, obj);
  int r = sysobj.rop().stat(y, dpp);
  if (r < 0) {
    ldpp_dout(dpp, 10) << "couldn't find old data placement pools config, setting up new ones for the zone" << dendl;
    /* a new system, let's set new placement info */
    RGWZonePlacementInfo default_placement;
    default_placement.index_pool = name + "." + default_bucket_index_pool_suffix;
    rgw_pool pool = name + "." + default_storage_pool_suffix;
    default_placement.storage_classes.set_storage_class(RGW_STORAGE_CLASS_STANDARD, &pool, nullptr);
    default_placement.data_extra_pool = name + "." + default_storage_extra_pool_suffix;
    placement_pools["default-placement"] = default_placement;
  }

  r = fix_pool_names(dpp, y);
  if (r < 0) {
    ldpp_dout(dpp, 0) << "ERROR: fix_pool_names returned r=" << r << dendl;
    return r;
  }

  r = RGWSystemMetaObj::create(dpp, y, exclusive);
  if (r < 0) {
    return r;
  }

  // try to set as default. may race with another create, so pass exclusive=true
  // so we don't override an existing default
  r = set_as_default(dpp, y, true);
  if (r < 0 && r != -EEXIST) {
    ldpp_dout(dpp, 10) << "WARNING: failed to set zone as default, r=" << r << dendl;
  }

  return 0;
}
