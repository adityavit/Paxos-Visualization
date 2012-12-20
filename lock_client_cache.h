// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <string>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "extent_client_cache.h"
#include "lang/verify.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t) = 0;
  virtual ~lock_release_user() {};
};

class lock_release_flush : public lock_release_user {
  private:
    extent_client_cache *ec;
  public:
    lock_release_flush(extent_client_cache *);
    void dorelease(lock_protocol::lockid_t);
};
class lock_client_cache : public lock_client {
  private:
    class lock_release_user *lu;
    int rlock_port;
    std::string hostname;
    std::string id;
    enum lock_status {
      none=0,
      free,
      locked,
      acquiring,
      releasing,
      retry_later
    };
    struct lockstatus {
      int status;
      bool toRevoke;
    };
    std::map<lock_protocol::lockid_t,struct lockstatus> lockm_;
    std::list<lock_protocol::lockid_t>revoke_id_list;
    std::list<lock_protocol::lockid_t>retry_id_list;
    pthread_mutex_t lid_m, revoke_m, retry_m;
    pthread_cond_t revoke_cond, retry_cond, lid_cond;
    pthread_t tidRevoke, tidRetry;
  public:

    //Constructor is defined here since it invokes a static member function
    //lock_client_cache(std::string xdst, class lock_release_user *l = 0);
    lock_client_cache(std::string xdst, class lock_release_user *_lu )
      : lock_client(xdst), lu(_lu)
    {
      rpcs *rlsrpc = new rpcs(0);
      rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
      rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

      VERIFY(pthread_mutex_init(&lid_m, NULL) == 0);
      VERIFY(pthread_mutex_init(&revoke_m, NULL) == 0);
      VERIFY(pthread_mutex_init(&retry_m, NULL) == 0);
      VERIFY(pthread_cond_init(&lid_cond, 0) == 0);
      VERIFY(pthread_cond_init(&revoke_cond, 0) == 0);
      VERIFY(pthread_cond_init(&retry_cond, 0) == 0);

      const char *hname;
      hname = "127.0.0.1";
      std::ostringstream host;
      host << hname << ":" << rlsrpc->port();
      id = host.str();

      VERIFY(pthread_create(&tidRevoke, NULL, &threadWrapper_revoke, this) == 0);
      VERIFY(pthread_create(&tidRetry, NULL, &threadWrapper_retry, this) == 0);
      pthread_detach(tidRevoke);
      pthread_detach(tidRetry);
    }

    /*virtual ~lock_client_cache() {};*/
    lock_protocol::status acquire(lock_protocol::lockid_t);
    lock_protocol::status release(lock_protocol::lockid_t);

    rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
        int &);
    rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
        int &);
    void *manageRevokeQueue(void );
    void *manageRetryQueue(void );
    static void *threadWrapper_revoke(void *arg) {
      return ((lock_client_cache *)arg)->manageRevokeQueue();
    }
    static void *threadWrapper_retry(void *arg) {
      return ((lock_client_cache *)arg)->manageRetryQueue();
    }
};

#endif
