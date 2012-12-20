// the extent server implementation

#include "extent_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <time.h>
#include "rpc/slock.h"

extent_server::extent_server() {
  VERIFY(pthread_mutex_init(&extent_server_m_, NULL) == 0);
}

extent_server::~extent_server() {
  extent_server::extents_.clear();
}

int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  // You fill this in for Lab 2.
  ScopedLock extnt(&extent_server_m_);
  extent_server::extent_map::iterator itr;
  itr = extents_.find(id);
  if(itr == extents_.end()) {
    //This is a new extent, so lets create it
    extent_server::extent_val_attr va;
    va.a.mtime = va.a.ctime = va.a.atime = time(NULL);
    va.a.size = buf.size();
    va.value.append(buf, 0, buf.size());
    extents_[id] = va;
  }
  else {
    itr->second.a.mtime = itr->second.a.ctime = time(NULL);
    itr->second.a.size = buf.size();
    itr->second.value = buf;
  }
  itr = extents_.find(id);
  std::cout << "put id " << itr->first <<" size: "<<itr->second.a.size <<std::endl;
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  // You fill this in for Lab 2.
  int ret;
  ScopedLock extnt(&extent_server_m_);
  extent_map::iterator itr;
  itr = extents_.find(id);
  if(itr == extents_.end()) {
    std::cout<<"get id not found"<< id <<"  "<< extents_.size()<< std::endl;
    ret = extent_protocol::NOENT;
  }
  else {
    itr->second.a.atime = time(NULL);
    buf = itr->second.value;
    std::cout<<"get id: "<< id << " buf size: " << buf.size() <<" attr size: " << itr->second.a.size << std::endl;
    ret = extent_protocol::OK;
  }
  return ret;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &at)
{
  // You fill this in for Lab 2.
  // You replace this with a real implementation. We send a phony response
  // for now because it's difficult to get FUSE to do anything (including
  // unmount) if getattr fails.
  int ret;
  ScopedLock extnt(&extent_server_m_);
  extent_map::iterator itr;
  itr = extents_.find(id);
  if(itr == extents_.end()) {
    std::cout<<"getattr id not found "<< id << std::endl;
    ret = extent_protocol::OK;
    //Dummy response
    at.atime = 0;
    at.mtime = 0;
    at.ctime = 0;
    at.size = 0;
  }
  else {
    at.atime = itr->second.a.atime;
    at.ctime = itr->second.a.ctime;
    at.mtime = itr->second.a.mtime;
    at.size = itr->second.a.size;
    ret = extent_protocol::OK;
  }
  std::cout <<"getattr id " <<"id: " << id << " size: " << at.size << " times: " << at.atime << " " << at.mtime << " " << at.ctime << std::endl;
  return ret;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  // You fill this in for Lab 2.
  int ret;
  ScopedLock extnt(&extent_server_m_);
  extent_map::iterator itr;
  itr = extents_.find(id);
  if(itr == extents_.end()) {
    std::cout<<"remove id not found: "<< id << std::endl;
    ret = extent_protocol::NOENT;
  }
  else {
    std::cout<<"removing id: "<< id << std::endl;
    extents_.erase(id);
    ret = extent_protocol::OK;
    std::cout<<"removed id: "<< id << std::endl;
  }
  return ret;
}

