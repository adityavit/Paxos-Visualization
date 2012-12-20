// this is the lock server
// the lock client has a similar interface

#ifndef lock_server_h
#define lock_server_h

#include <string>
#include <pthread.h>
#include "lock_protocol.h"
#include "lock_client.h"
#include "rpc.h"

class lock_server {
 private:
  void freelocks();
  std::map<lock_protocol::lockid_t,int>locks_;
  std::map<lock_protocol::lockid_t,int>lockstats_;
  pthread_mutex_t lid_m;
  pthread_cond_t lid_available;
 protected:
  int nacquire;

 public:
  lock_server();
  ~lock_server();
  lock_protocol::status stat(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status acquire(int clt, lock_protocol::lockid_t lid, int &);
  lock_protocol::status release(int clt, lock_protocol::lockid_t lid, int &);
};

#endif 
