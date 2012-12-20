// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"
#define _DEBUG_
lock_server_cache::~lock_server_cache() {
  freelocks();
}

//This thread will retry the list of clients waiting for a lid, once that lid has been released
void * lock_server_cache::manageRetryQueue(void) {
  while(1) {
    lock_protocol::lockid_t releasedlid;
    {
      ScopedLock retrym(&retry_m);
      while(release_lid_list.empty()) {
        VERIFY(pthread_cond_wait(&retry_cond, &retry_m) == 0);
      }
      releasedlid = release_lid_list.front();
      release_lid_list.pop_front();
    }
    std::string waitingID ;
    rlock_protocol::status ret ;
    {
      ScopedLock lidm(&lid_m);
      waitingID = lockstat_[releasedlid].waitingcltIDs.front();
      lockstat_[releasedlid].waitingcltIDs.pop_front();
    }
    ret = makeclientRPC(rlock_protocol::retry, releasedlid, waitingID);
    if(ret != rlock_protocol::OK) {
      ScopedLock lidm(&lid_m);
      lockstat_[releasedlid].waitingcltIDs.push_back(waitingID); //push it back to waiting list if retry RPC call was not successful
      tprintf("manageRetryQueue: failed to retry lid clt %s for lid %llu\n", waitingID.c_str(), releasedlid);
    }
    else {
      tprintf("manageRetryQueue: sent retry to clt %s for lid %llu size %d\n", waitingID.c_str(), releasedlid, release_lid_list.size());
    }
  }
  return NULL;
}

//This thread manages a queue of pending locks by sending revoke RPCs to the owner
void * lock_server_cache::manageRevokeQueue(void) {
  while(1) {
    struct revoke re ;
    std::string lockOwnerID ;
    rlock_protocol::status ret ;
    bool flag_revoke;
    {
      ScopedLock revokem(&revoke_m);
      while(revoke_list.empty()) {
        VERIFY(pthread_cond_wait(&revoke_cond, &revoke_m) == 0);
      }
      re = revoke_list.front();
    }
    {
      ScopedLock lidm(&lid_m);
      lockOwnerID = lockstat_[re.lid].cltID;
      if(lockstat_[re.lid].revoke) {
        while(lockstat_[re.lid].revoke) {
          VERIFY(pthread_cond_wait(&lid_cond, &lid_m) == 0);
        }
        flag_revoke = false;
      }
      else {
        flag_revoke = true; //Revoke only if there is no existing revoke in progress
        lockstat_[re.lid].revoke = true;
      }
    }
    if(flag_revoke){
      ScopedLock revokem(&revoke_m);
      revoke_list.pop_front();  //Pop only if we will send the revoke
    }
    if(!lockOwnerID.empty() && flag_revoke) {
      ret = makeclientRPC(rlock_protocol::revoke, re.lid, lockOwnerID);
      if(ret != rlock_protocol::OK) {
        tprintf("manageRevokeQueue: failed to revoke lid %llu from %s\n", re.lid, lockOwnerID.c_str());
      }
      else {
        tprintf("manageRevokeQueue: revoked lid %llu from %s %d\n", re.lid, lockOwnerID.c_str(), revoke_list.size());
      }
    }
  }
  return NULL;
}


void lock_server_cache::freelocks() {
  ScopedLock lidm(&lid_m);
  lockstat_.clear();
}

int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
    int &r)
{
  lock_protocol::status ret ;
  r = nacquire;

  //Mutex locks are applied in same order in all threads to prevent deadlocks
  ScopedLock revokem(&revoke_m);
  ScopedLock lidm(&lid_m);
  std::map<lock_protocol::lockid_t, struct lockstat>::iterator itrl;
  itrl = lockstat_.find(lid);

  if(itrl == lockstat_.end()) { //If the given lid is new
    struct lockstat lstat;
    lstat.cltID = id;
    lstat.stats = 1;
    lstat.revoke = false;
    lockstat_.insert(std::pair<lock_protocol::lockid_t,struct lockstat>(lid,lstat));
    VERIFY(pthread_cond_signal(&lid_cond) == 0);
    //lockstat_[lid] = lstat;
#ifdef _DEBUG_
    tprintf("acquire lock granted to clt: %s for new lid %llu\n", id.c_str(), lid);
#endif
    ret = lock_protocol::OK;
  }
  else if(itrl->second.cltID.empty() ){ //If the given lid is free
    itrl->second.stats++;
    itrl->second.cltID = id;
    itrl->second.revoke = false;
#ifdef _DEBUG_
    tprintf("acquire lock granted to clt: %s for lid %llu\n", id.c_str(), lid);
#endif
    VERIFY(pthread_cond_signal(&lid_cond) == 0);
    ret = lock_protocol::OK;
  }
  //Lock is occupied send a RETRY reply to the client
  else {
    struct revoke r;
    r.cltID = id;
    r.lid = lid;
    revoke_list.push_back(r);
    itrl->second.waitingcltIDs.push_back(id);
#ifdef _DEBUG_
    tprintf("acquire lock cannot be granted to clt: %s for lid %llu, sending RETRY instead\n", id.c_str(), lid);
#endif
    VERIFY(pthread_cond_signal(&revoke_cond) == 0);
    ret = lock_protocol::RETRY;
  }
  return ret;
}

  int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
    int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  ScopedLock retrym(&retry_m);
  ScopedLock lidm(&lid_m);
  tprintf("release lid %llu released by clt: %s %s\n", lid, id.c_str(), lockstat_[lid].cltID.c_str());

  //Add to the list of released lids
  release_lid_list.push_back(lid);
  //Clear the lock holder
  lockstat_[lid].cltID.clear();
  //Signal the retry thread about the newly released lid
  pthread_cond_signal(&retry_cond);
  return ret;
}

  lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  ScopedLock lidm(&lid_m);
  r = lockstat_[lid].stats;
  tprintf("stat for lid %llu = %d\n", lid, r);
  //r = nacquire;
  return lock_protocol::OK;
}

rlock_protocol::status
lock_server_cache::makeclientRPC(int rpc, lock_protocol::lockid_t lid, std::string id) {
  rlock_protocol::status ret;
  handle h(id);
  rpcc *cl = h.safebind();
  if(cl) {
    int r;
    //Send RPC call to clt
    ret = cl->call(rpc, lid, r);
    if(ret != lock_protocol::OK) {
      tprintf("makeclientRPC lid: %llu failed for %s\n", lid, id.c_str());
    }
  }
  else {
    tprintf("makeclientRPC lid: %llu bind handle failed for %s\n", lid, id.c_str());
  }
  return ret;
}
