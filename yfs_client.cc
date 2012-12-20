// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  //ec = new extent_client(extent_dst);
  ec = new extent_client_cache(extent_dst);
  //lc = new lock_client(lock_dst);
  lf = new lock_release_flush(ec);
  lc = new lock_client_cache(lock_dst, lf);

  //Create the inode for the root of the file system
  std::string dummy;
  acquirelock(1);
  extent_protocol::status ret = ec->put(1, dummy);
  VERIFY(ret == extent_protocol::OK);
  releaselock(1);
}

  yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

  std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

  bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

  bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int yfs_client::getvalue(inum ino, std::string &content) {
  int r;
  r = ec->get(ino, content) ;
  printf("getvalue %llu -> content size %u\n", ino, content.size());
  return r;
}

int yfs_client::setvalue(inum ino, std::string content) {
  int r;
  r = ec->put(ino, content) ;
  return r;
}

bool yfs_client::dirlookup(inum parent, const char *name, inum &entry_inum) {
  bool found ;
  size_t pos, pos1;
  std::string entryname(name);
  entryname += ":";
  std::string dircontent;

  //Get the existing list of files for parent dir
  int r = getvalue(parent, dircontent);
  if(r != extent_protocol::OK) {
    printf("yfs_client: dirlookup could not get contents for parent dir %llu\n", parent);
    found = false;
  }
  else {
    if((pos = dircontent.find(entryname,0)) == std::string::npos) {
      printf("yfs_client: dirlookup parent dir %llu does not contain entry %s\n", parent, name);
      found = false;
    }
    else {
      pos += entryname.length();
      pos1 = dircontent.find("|",pos);
      std::string entry_inum_str(dircontent,pos, pos1-pos);
      entry_inum = atoll(entry_inum_str.c_str());
      found = true;
      printf("yfs_client: dirlookup parent dir %llu contains entry %s %llu\n", parent, name, entry_inum);
    }
  }
  return found;
}

int yfs_client::removefile(inum parent, inum ino, const char *name) {
  size_t pos;
  std::string dircontent;

  //Constructing the file entry string present in directory's contents
  std::string entryname(name);
  entryname += ":";
  entryname += filename(ino);
  entryname += "|";

  int r = ec->remove(ino);
  if(r != extent_protocol::OK) {
    printf("yfs_client: removefile could not remove inode %llu\n", ino);
    goto release;
  }
  else {
    //Get the existing list of files for parent dir
    r = getvalue(parent, dircontent);
    if(r != extent_protocol::OK) {
      printf("yfs_client: removefile could not get contents for parent dir %llu for file %s : %llu\n", parent,name,ino);
      goto release;
    }
    else {
      if((pos = dircontent.find(entryname,0)) == std::string::npos) {
        printf("yfs_client: removefile parent dir %llu does not contain entry for file %s : %llu\n", parent, name, ino);
        r = extent_protocol::NOENT;
        goto release;
      }
      else {
          //printf("yfs_client: removefile parent directory %llu old contents %s\n", parent, dircontent.c_str());
        dircontent.replace(pos, entryname.size(),"");
        //Modifying content of parent dir to remove the deleted file's information
        r = ec->put(parent, dircontent);
        if(r != extent_protocol::OK) {
          printf("yfs_client: removefile could not modify parent directory %llu 's content after removing file %s : %llu\n", parent, name, ino);
          goto release;
        }
        else {
          printf("yfs_client: removefile parent directory %llu no longer contains file %s : %llu\n", parent, name, ino);
          //printf("yfs_client: removefile parent directory %llu new contents %s %d\n", parent, dircontent.c_str(), dircontent.size());
          goto release;
        }
      }
    }
  }
release:
  return r;
}

int yfs_client::createentry(inum parent, const char *name, inum ino) {
  int r ;
  std::string dircontent, dummycontent;

  inum t;
  bool entryExists = dirlookup(parent, name, t);
  if(entryExists == true) {
    printf("yfs_client: createentry parent dir %llu already contains entry %s\n", parent, name);
    r = EXIST;
    goto release;
  }

  //Get the existing list of entities in parent dir
  r = getvalue(parent, dircontent);
  if(r != extent_protocol::OK) {
    printf("yfs_client: createentry could not get contents for parent dir %llu\n", parent);
    r = NOENT;
    goto release;
  }

  //Create empty extent for new entry with key ino
  r = ec->put(ino, dummycontent);
  if(r != extent_protocol::OK) {
    printf("yfs_client: createentry could not create empty extent for new entry %s\n", name);
    goto release;
  }

  dircontent += name;
  dircontent += ":";
  dircontent += filename(ino);
  dircontent += "|";
  r = ec->put(parent, dircontent);    //Append the new entry to directory
  if(r != extent_protocol::OK) {
    printf("yfs_client: createentry could not append %s to dir %llu\n", name, parent);
    goto release;
  }
  printf("yfs_client: createentry %s : %llu in dir %llu\n", name, ino, parent);
  printf("yfs_client: createentry dir %llu contains %s\n", parent, dircontent.c_str());

release:
  return r;
}
  int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the file lock

  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %llu -> attr size %llu atime %llu ctime %llu mtime %llu\n", inum, a.size, a.atime, a.ctime, a.mtime);
  printf("getfile %llu -> fin size %llu atime %lu ctime %lu mtime %lu\n", inum, fin.size, fin.atime, fin.ctime, fin.mtime);
  //printf("getfile %lu -> sz %llu %llu %llu\n", inum, fin.size, a.size, a.mtime);

release:
  return r;
}

  int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the directory lock

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

release:
  return r;
}

void yfs_client::acquirelock(lock_protocol::lockid_t lid) {
  lc->acquire(lid);
}
void yfs_client::releaselock(lock_protocol::lockid_t lid) {
  lc->release(lid);
}
