// extent client interface.

#ifndef extent_client_cache_h
#define extent_client_cache_h

#include <map>
#include <pthread.h>
#include "extent_client.h"
#include "rpc.h"

class extent_client_cache : public extent_client {
  private:
    pthread_mutex_t extent_m;
    struct extent_val_attr {
      std::string value;
      extent_protocol::attr a;
      bool flagValModified, flagValCached, flagAttrCached;
    };
    typedef struct extent_val_attr extent_val_attr;
    typedef std::map <extent_protocol::extentid_t,extent_val_attr> extent_map;
    extent_map extent_;
  public:
    extent_client_cache(std::string dst);

    extent_protocol::status get(extent_protocol::extentid_t eid, 
        std::string &buf);
    extent_protocol::status getattr(extent_protocol::extentid_t eid, 
        extent_protocol::attr &a);
    extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
    extent_protocol::status remove(extent_protocol::extentid_t eid);
    void flush(extent_protocol::extentid_t eid);
};

#endif 

