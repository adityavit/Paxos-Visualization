// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"
#include <pthread.h>
#include "tprintf.h"

#define _DEBUG_

lock_release_flush::lock_release_flush(extent_client_cache *_ec):
  ec(_ec)
{
}

void
lock_release_flush::dorelease(lock_protocol::lockid_t lid) {
  //Lock ID is same as extent ID which is same as inum
  ec->flush(lid);
}
//This will set lock state to none if its a new lock and set it to acquiring/locked/retry_later
//Will return when acquire RPC returns the lock
  lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int r;
  bool acquiredLock = false;
  while(!acquiredLock)
  {
    {
      ScopedLock lm(&lid_m);
      if(lockm_.find(lid) == lockm_.end()) {
        //If the server has not seen this lock before
#ifdef _DEBUG_
        tprintf("lock_client_cache: acquire lock lid %llu is unknown for client %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
#endif
        struct lockstatus st;
        st.status = none;
        st.toRevoke = false;
        lockm_.insert(std::pair<lock_protocol::lockid_t, struct lockstatus> (lid, st));
      }
      else if(lockm_[lid].status == acquiring || lockm_[lid].status == locked || lockm_[lid].status == releasing || lockm_[lid].status == retry_later) {
        while(!(lockm_[lid].status == free || lockm_[lid].status == none)) {
          VERIFY(pthread_cond_wait(&lid_cond, &lid_m) == 0);
        }
      }
      if(lockm_[lid].status == none) {
        lockm_[lid].status = acquiring;
      }
      else if(lockm_[lid].status == free){
        lockm_[lid].status = locked;
        tprintf("lock_client_cache: acquire client already has the lock lid %llu and its free. So lock has been acquired: %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
        acquiredLock = true;
      }
    }

    if(!acquiredLock) {
      tprintf("lock_client_cache: acquire client acquiring lid %llu by making a RPC call %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());

      lock_protocol::status ret = cl->call(lock_protocol::acquire, lid, id, r);

      VERIFY(ret == lock_protocol::OK || ret == lock_protocol::RETRY);
      if(ret == lock_protocol::OK) {
        ScopedLock lm(&lid_m);
        tprintf("lock_client_cache: acquired lock %llu via RPC %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
        lockm_[lid].status = locked;   //Mark it as acquired lock successfully
        acquiredLock = true;
      }
      else if(ret == lock_protocol::RETRY){
        ScopedLock lm(&lid_m);
        tprintf("lock_client_cache: acquire lock %llu resulted in retry later %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
        lockm_[lid].status = retry_later;   //Mark it as acquired lock successfully
        VERIFY(pthread_cond_broadcast(&lid_cond) == 0);  //broadcast it so that revoke thread knows acquire has returned and hence revoke can return
      }
    }
  }
  return r;
}

//Sets lock state to free indicating that no thread has the lock and hence can be acquired by any thread

  lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  lock_protocol::status ret = lock_protocol::OK;
  int r;
  bool flagRelease = false;
#ifdef _DEBUG_
  tprintf("lock_client_cache: release call for lid %llu %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
#endif
  {
    ScopedLock lm(&lid_m);
    if(lockm_[lid].toRevoke) {
      lockm_[lid].status = releasing;
      lockm_[lid].toRevoke = false;
      flagRelease = true;
#ifdef _DEBUG_
      tprintf("lock_client_cache: release for lid %llu leading to releasing state since revoke has been invoked %s status %d (%lu)\n", lid, id.c_str(), lockm_[lid].status, (unsigned long)pthread_self());
#endif
    }
    else {
#ifdef _DEBUG_
      tprintf("lock_client_cache: release for lid %llu leading to free state %s (%lu)\n", lid, id.c_str(), (unsigned long)pthread_self());
#endif
      lockm_[lid].status = free;
    }
  }
  if(flagRelease) {

    lu->dorelease(lid);

    lock_protocol::status ret = cl->call(lock_protocol::release, lid, id, r);
    tprintf("lock_client: release call return for lid %llu client %s (%lu)\n", lid, id.c_str(), (unsigned long) pthread_self());
    VERIFY (ret == lock_protocol::OK);

    ScopedLock lm(&lid_m);
    lockm_[lid].status = none;  //Release is done, so mark the status as none
  }
  ScopedLock lm(&lid_m);
  //Signal other threads that lid is no longer locked
  VERIFY(pthread_cond_broadcast(&lid_cond) == 0);
  return ret;
}

//This function revokes lid if no other thread has currently locked it or no other thread has made acquiring call for lid
  rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
    int &)
{
  int ret = rlock_protocol::OK;
  ScopedLock rm(&revoke_m);
#ifdef _DEBUG_
  tprintf("lock_client_cache: revoke received for lid %llu %s (%lu)\n", lid, id.c_str(), pthread_self());
#endif
  revoke_id_list.push_back(lid);
  VERIFY(pthread_cond_signal(&revoke_cond) == 0);
  return ret;
}

  void *
lock_client_cache::manageRevokeQueue(void)
{
  while(1) {
    lock_protocol::lockid_t lid = revoke_id_list.front();
    {
      ScopedLock rm(&revoke_m);
      while(revoke_id_list.empty()) {
        VERIFY(pthread_cond_wait(&revoke_cond, &revoke_m) == 0);
      }
      lid = revoke_id_list.front();
      revoke_id_list.pop_front();
    }

    {
      ScopedLock lm(&lid_m);
      //Wait for current acquire RPC to return or current lock to be released
      while(lockm_[lid].status != free) {
        tprintf("lock_client_cache: managingRevokeQueue %s waiting for lid %llu  to be released or its acquire to complete status %d (%lu)\n", id.c_str(), lid, lockm_[lid].status, (unsigned long)pthread_self());
        VERIFY(pthread_cond_wait(&lid_cond, &lid_m) == 0);
      }
#ifdef _DEBUG_
      tprintf("lock_client_cache: managingRevokeQueue for lid %llu calling release RPC %s (%lu)\n", lid, id.c_str(), pthread_self());
#endif
      lockm_[lid].toRevoke = true;
    }
    //Call release for lid
    lock_protocol::status ret = release(lid);
    VERIFY(ret == lock_protocol::OK);
  }
  return NULL;
}

  rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
    int &)
{
  int ret = rlock_protocol::OK;
  ScopedLock re(&retry_m);
  tprintf("lock_client_cache: retry_handler for lid %llu invoked with status %d %s (%lu)\n", lid, lockm_[lid].status, id.c_str(), pthread_self());
  retry_id_list.push_back(lid);
  VERIFY(pthread_cond_signal(&retry_cond) == 0);
  return ret;
}

  void *
lock_client_cache::manageRetryQueue(void)
{
  while(1){
    lock_protocol::lockid_t lid ;
    {
      ScopedLock re(&retry_m);
      while(retry_id_list.empty()) {
        VERIFY(pthread_cond_wait(&retry_cond, &retry_m) == 0);
      }
      lid = retry_id_list.front();
      retry_id_list.pop_front();
    }
    ScopedLock lm(&lid_m);
    while(lockm_[lid].status == acquiring) {
#ifdef _DEBUG_
      tprintf("lock_client_cache: manageRetryQueue client %s waiting for lid %llu existing acquire to return before retrying (%lu)\n", id.c_str(), lid, pthread_self());
#endif
      VERIFY(pthread_cond_wait(&lid_cond, &lid_m) == 0);
    }
    //Only if client does not have the lock
    if(lockm_[lid].status != locked) {
#ifdef _DEBUG_
      tprintf("lock_client_cache: manageRetryQueue client %s lid %llu can now retry %d (%lu)\n", id.c_str(), lid, lockm_[lid].status, pthread_self());
#endif
      lockm_[lid].status = none;
      VERIFY(pthread_cond_broadcast(&lid_cond) == 0);
    }
  }
  return NULL;
}
