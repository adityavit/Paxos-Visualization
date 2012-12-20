// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include <pthread.h>
#include "extent_protocol.h"

class extent_server {
  protected:
    struct extent_val_attr_ {
      extent_protocol::attr a;
      std::string value;
    };
    typedef struct extent_val_attr_ extent_val_attr;
    pthread_mutex_t extent_server_m_;
    typedef std::map <extent_protocol::extentid_t, extent_val_attr> extent_map;
    extent_map extents_;

  public:
    extent_server();
    ~extent_server();

    virtual int put(extent_protocol::extentid_t id, std::string, int &);
    virtual int get(extent_protocol::extentid_t id, std::string &);
    virtual int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
    virtual int remove(extent_protocol::extentid_t id, int &);
};
#endif 
