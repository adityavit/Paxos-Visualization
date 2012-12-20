// RPC stubs for clients to talk to extent_server

#include "extent_client_cache.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

// The calls assume that the caller holds a lock on the extent

extent_client_cache::extent_client_cache(std::string dst):
  extent_client(dst)
{
  VERIFY(pthread_mutex_init(&extent_m, NULL) == 0);
}

  extent_protocol::status
extent_client_cache::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  extent_map::iterator itre;
  bool fetchExtentRemotely = true;
  {
    ScopedLock em(&extent_m);
    itre = extent_.find(eid);
    //If the extent's value is present in the cache
    if(itre != extent_.end() && itre->second.flagValCached) {
      printf("get: eid %llu's value is present in the cache\n", eid);
      buf = itre->second.value.substr(0, itre->second.value.size());
      fetchExtentRemotely = false;
    }
  }
  if(fetchExtentRemotely) {
    printf("get: eid %llu's value is absent from the cache, fetch via RPC\n", eid);
    fetchExtentRemotely = true;
    ret = cl->call(extent_protocol::get, eid, buf);
  }
  if(fetchExtentRemotely && ret == extent_protocol::OK) {
    ScopedLock em(&extent_m);
    itre = extent_.find(eid);
    
    if(itre == extent_.end()) {
      extent_val_attr eva;
      eva.value = buf.substr(0, buf.size());
      eva.flagValCached = true;
      eva.flagValModified = eva.flagAttrCached = false;
      extent_.insert(std::pair<extent_protocol::extentid_t,extent_val_attr>(eid, eva));
      printf("get: creating a new entry in cache for eid %llu with it's value\n", eid);
    }
    else {
      itre->second.value = buf.substr(0, buf.size());
      itre->second.flagValCached = true;
      printf("get: updating existing cache entry for eid %llu with it's value\n", eid);
    }
  }
  printf("get: eid %llu returns %d bytes\n", eid, buf.size());

  return ret;
}

  extent_protocol::status
extent_client_cache::getattr(extent_protocol::extentid_t eid, 
    extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  extent_map::iterator itre;
  bool fetchExtentRemotely = true;
  {
    ScopedLock em(&extent_m);
    itre = extent_.find(eid);
    //If the extent's attribute is present in the cache
    if(itre != extent_.end() && itre->second.flagAttrCached) {
      printf("getattr: eid %llu's attribute is present in the cache\n", eid);
      attr.atime = itre->second.a.atime;
      attr.mtime = itre->second.a.mtime;
      attr.ctime = itre->second.a.ctime;
      attr.size = itre->second.a.size;
      fetchExtentRemotely = false;
    }
  }
  if(fetchExtentRemotely) {
    printf("getattr: eid %llu's attribute is absent from the cache, fetch via RPC\n", eid);
    fetchExtentRemotely = true;
    ret = cl->call(extent_protocol::getattr, eid, attr);
    printf("getattr eid %llu's attribute returned by extent server is size:%llu atime:%llu ctime:%llu mtime: %llu\n", eid, attr.size, attr.atime, attr.ctime, attr.mtime);
  }

  if(fetchExtentRemotely && ret == extent_protocol::OK) {
    ScopedLock em(&extent_m);
    itre = extent_.find(eid);

    if(itre == extent_.end()) {
      extent_val_attr eva;
      eva.value.clear();
      eva.flagAttrCached = true;
      eva.flagValModified = eva.flagValCached = false;
      eva.a.atime = attr.atime;
      eva.a.mtime = attr.mtime;
      eva.a.ctime = attr.ctime;
      eva.a.size = attr.size;
      extent_.insert(std::pair<extent_protocol::extentid_t,extent_val_attr>(eid, eva));
      printf("getattr: creating a new entry in cache for eid %llu with it's attributes\n", eid);
    }
    else {
      itre->second.a.atime = attr.atime;
      itre->second.a.ctime = attr.ctime;
      itre->second.a.mtime = attr.mtime;
      itre->second.a.size = attr.size;
      itre->second.flagAttrCached = true;
      printf("getattr: updating existing cache entry for eid %llu with it's attribute\n", eid);
    }
  }

  return ret;
}

  extent_protocol::status
extent_client_cache::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  extent_map::iterator itre;
  printf("put: eid %llu to write %d bytes\n", eid, buf.size());

  ScopedLock em(&extent_m);

  itre = extent_.find(eid);
  if(itre == extent_.end()) {
    extent_val_attr eva;
    eva.value = buf.substr(0, buf.size());
    eva.a.atime = eva.a.mtime = eva.a.ctime = time(NULL);
    eva.a.size = buf.size();
    eva.flagAttrCached = eva.flagValCached = eva.flagValModified = true;

    extent_.insert(std::pair<extent_protocol::extentid_t,extent_val_attr>(eid, eva));
    printf("put: creating a new entry in cache for eid %llu with it's new value\n", eid);
  }
  else {
    itre->second.value = buf.substr(0, buf.size());
    itre->second.a.atime = itre->second.a.mtime = itre->second.a.ctime = time(NULL);
    itre->second.a.size = buf.size();
    itre->second.flagAttrCached = itre->second.flagValCached = itre->second.flagValModified = true;
    printf("put: updating existing cache for eid %llu with it's new value\n", eid);
  }
  return ret;
}

  extent_protocol::status
extent_client_cache::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;

  ScopedLock em(&extent_m);
  printf("remove: removing local cache for eid %llu\n", eid);
  extent_.erase(eid);

  return ret;
}

void
extent_client_cache::flush(extent_protocol::extentid_t eid) {
  extent_protocol::status ret = extent_protocol::OK;
  extent_map::iterator itre;
  std::string buf;
  int r;
  bool removeExtent = false, dirtyExtent = false;
  {
    ScopedLock em(&extent_m);
    itre = extent_.find(eid);
    if(itre == extent_.end()) {
      removeExtent = true;
    }
    else {
      if(itre->second.flagValModified) {
        dirtyExtent = true;
        buf = itre->second.value.substr(0, itre->second.value.size());
      }
      extent_.erase(eid);
    }
  }
  if(removeExtent) {
    printf("flush: eid %llu has been removed locally, hence calling remove at extent server\n", eid);
    ret = cl->call(extent_protocol::remove, eid, r);
    VERIFY(ret == extent_protocol::OK);
  }
  else if(dirtyExtent) {
    printf("flush: eid %llu has been modified locally, hence calling put at extent server (size %d)\n", eid, buf.size());
    ret = cl->call(extent_protocol::put, eid, buf, r);
    VERIFY(ret == extent_protocol::OK);
  }
}
