#ifndef yfs_client_h
#define yfs_client_h

#include <string>
//#include "extent_client.h"
#include "extent_client_cache.h"
#include <vector>
//#include "lock_client.h"
#include "lock_client_cache.h"
#include "lock_protocol.h"

class yfs_client {
  //extent_client *ec;
  extent_client_cache *ec;
  //lock_client *lc;
  lock_client_cache *lc;
  lock_release_flush *lf;
 public:

  typedef unsigned long long inum;
  enum xxstatus { OK, RPCERR, NOENT, IOERR, EXIST };
  typedef int status;

  struct fileinfo {
    unsigned long long size;
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirinfo {
    unsigned long atime;
    unsigned long mtime;
    unsigned long ctime;
  };
  struct dirent {
    std::string name;
    yfs_client::inum inum;
  };

 private:
  static std::string filename(inum);
  static inum n2i(std::string);
 public:

  yfs_client(std::string, std::string);

  bool isfile(inum);
  bool isdir(inum);

  int createentry(inum, const char *, inum);
  bool dirlookup(inum, const char *, inum &);
  int setvalue(inum, std::string);
  int getvalue(inum ino, std::string &);
  int getfile(inum, fileinfo &);
  int getdir(inum, dirinfo &);
  int removefile(inum, inum, const char *);
  void acquirelock(lock_protocol::lockid_t);
  void releaselock(lock_protocol::lockid_t);
};

#endif 
