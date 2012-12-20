#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <string>

#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"

class lock_server_cache {
  private:
    pthread_t tidRevoke, tidRetry;
    pthread_mutex_t lid_m, revoke_m, retry_m;
    pthread_cond_t lid_cond, revoke_cond, retry_cond;
    struct lockstat {
      std::string cltID;    //The ID string of the client currently holding the lock
      std::list<std::string>waitingcltIDs;   //The list of IDs waiting for lock to be released
      bool revoke;
      int stats;
    };
    std::map<lock_protocol::lockid_t, struct lockstat> lockstat_;
    void freelocks();

    struct revoke {
      std::string cltID;
      lock_protocol::lockid_t lid;
    };
    std::list<struct revoke>revoke_list;
    std::list<lock_protocol::lockid_t> release_lid_list; //List of locks that have been revoked

    rlock_protocol::status makeclientRPC(int, lock_protocol::lockid_t, std::string);
    int nacquire;

  public:
    lock_server_cache() :
      nacquire(0)
  {
    //Making reference to static function hence defined in header file
    VERIFY(pthread_mutex_init(&lid_m, NULL) == 0);
    VERIFY(pthread_cond_init(&lid_cond, 0) == 0);
    VERIFY(pthread_mutex_init(&revoke_m, NULL) == 0);
    VERIFY(pthread_cond_init(&revoke_cond, 0) == 0);
    VERIFY(pthread_mutex_init(&retry_m, NULL) == 0);
    VERIFY(pthread_cond_init(&retry_cond, 0) == 0);
    VERIFY(pthread_create(&tidRevoke, NULL, &threadWrapper_revoke, this) == 0);
    VERIFY(pthread_create(&tidRetry, NULL, &threadWrapper_retry, this) == 0);
    pthread_detach(tidRevoke);
    pthread_detach(tidRetry);
  }
    ~lock_server_cache();
    lock_protocol::status stat(lock_protocol::lockid_t, int &);
    lock_protocol::status acquire(lock_protocol::lockid_t, std::string id, int &);
    void *manageRevokeQueue(void );
    void *manageRetryQueue(void );
    static void *threadWrapper_revoke(void *arg) {
      return ((lock_server_cache *)arg)->manageRevokeQueue();
    }
    static void *threadWrapper_retry(void *arg) {
      return ((lock_server_cache *)arg)->manageRetryQueue();
    }
    lock_protocol::status release(lock_protocol::lockid_t, std::string id, int &);
};

#endif
