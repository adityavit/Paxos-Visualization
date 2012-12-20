// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

//#define _DEBUG_

lock_server::lock_server():
  nacquire (0)
{
  VERIFY(pthread_mutex_init(&lid_m, NULL) == 0);
  VERIFY(pthread_cond_init(&lid_available, 0) == 0);
}

lock_server::~lock_server()
{
  lock_server::freelocks();
}
  
  void
lock_server::freelocks()
{
  locks_.clear();
  lockstats_.clear();
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  ScopedLock lidm(&lid_m);
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d for lid %llu\n", clt, lid);
  r = lockstats_[lid];
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret;
  r = nacquire;
  if(clt < 0) {
#ifdef _DEBUG_
    printf("acquire request from from invalid clt %d for lid %llu\n", clt, lid);
#endif
    ret = lock_protocol::RPCERR;
  }
  else {
    ScopedLock lidm(&lid_m);
    while(1) {
      if(locks_.find(lid) == locks_.end()) {
        //Create a new entry for lid in the map
        locks_[lid] = clt;
        lockstats_[lid] = 1;
        ret = lock_protocol::OK;
#ifdef _DEBUG_
        printf("acquire lock granted to clt %d for new lid %llu\n", clt, lid);
#endif
        break;
      }
      else if(locks_[lid] < 0) {
        lockstats_[lid]++;
        locks_[lid] = clt;
        ret = lock_protocol::OK;
#ifdef _DEBUG_
        printf("acquire lock granted clt %d for lid %llu\n", clt, lid);
#endif
        break;
      }
      else {
#ifdef _DEBUG_
        printf("acquire request from clt %d for lid %llu, waiting for lid to be available\n", clt, lid);
#endif
        VERIFY(pthread_cond_wait(&lid_available, &lid_m) == 0);
      }
    }
  }
  return ret;
}

  lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret ;
  r = nacquire;
  if(clt < 0) {
#ifdef _DEBUG_
    printf("release request from from invalid clt %d for lid %llu\n", clt, lid);
#endif
    ret = lock_protocol::RPCERR;
  }
  else {
    ScopedLock lidm(&lid_m);
    if(locks_.find(lid) == locks_.end()) {
#ifdef _DEBUG_
      printf("release request from clt %d for lid %llu and no such lockID exists\n", clt, lid);
#endif
      ret = lock_protocol::RPCERR;
    }
    else if(locks_[lid] != clt) {
#ifdef _DEBUG_
      printf("release request from clt %d for lid %llu and client has not acquired the lock\n", clt, lid);
#endif
      ret = lock_protocol::RPCERR;
    }
    else {
#ifdef _DEBUG_
      printf("release granted to clt %d for lid %llu\n", clt, lid);
#endif
      locks_[lid] = -1;
      VERIFY( pthread_cond_broadcast(&lid_available)== 0);
      ret = lock_protocol::OK;
    }
  }
  return ret;
}
