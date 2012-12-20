/*
 * receive request from fuse and call methods of yfs_client
 *
 * started life as low-level example in the fuse distribution.  we
 * have to use low-level interface in order to get i-numbers.  the
 * high-level interface only gives us complete paths.
 */

#include <fuse_lowlevel.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "yfs_client.h"

int myid;
yfs_client *yfs;

int id() { 
  return myid;
}

//
// A file/directory's attributes are a set of information
// including owner, permissions, size, &c. The information is
// much the same as that returned by the stat() system call.
// The kernel needs attributes in many situations, and some
// fuse functions (such as lookup) need to return attributes
// as well as other information, so getattr() gets called a lot.
//
// YFS fakes most of the attributes. It does provide more or
// less correct values for the access/modify/change times
// (atime, mtime, and ctime), and correct values for file sizes.
//
  yfs_client::status
getattr(yfs_client::inum inum, struct stat &st)
{
  yfs_client::status ret;

  bzero(&st, sizeof(st));
  st.st_ino = inum;
  if(yfs->isfile(inum)){
    yfs_client::fileinfo info;
    ret = yfs->getfile(inum, info);
    if(ret != yfs_client::OK) {
      goto release;
    }
    st.st_mode = S_IFREG | 0666;
    st.st_nlink = 1;
    st.st_atime = info.atime;
    st.st_mtime = info.mtime;
    st.st_ctime = info.ctime;
    st.st_size = info.size;
    printf("getattr -> %llu fileinfo size %llu atime %lu ctime %lu mtime %lu\n", inum, info.size, info.atime, info.ctime, info.mtime);
    printf("getattr -> %llu stat st size %llu atime %lu ctime %lu mtime %lu\n", inum, st.st_size, st.st_atime, st.st_ctime, st.st_mtime);
  } else {
    yfs_client::dirinfo info;
    ret = yfs->getdir(inum, info);
    if(ret != yfs_client::OK) {
      goto release;
    }
    st.st_mode = S_IFDIR | 0777;
    st.st_nlink = 2;
    st.st_atime = info.atime;
    st.st_mtime = info.mtime;
    st.st_ctime = info.ctime;
    printf("getdir -> %llu dirinfo atime %lu ctime %lu mtime %lu\n", inum, info.atime, info.ctime, info.mtime);
    printf("getattr -> %llu dir stat atime %lu ctime %lu mtime %lu\n", inum, st.st_atime, st.st_ctime, st.st_mtime);
  }
release:
  return yfs_client::OK;
}

//
// This is a typical fuse operation handler; you'll be writing
// a bunch of handlers like it.
//
// A handler takes some arguments
// and supplies either a success or failure response. It provides
// an error response by calling either fuse_reply_err(req, errno), and
// a normal response by calling ruse_reply_xxx(req, ...). The req
// argument serves to link up this response with the original
// request; just pass the same @req that was passed into the handler.
// 
// The @ino argument indicates the file or directory FUSE wants
// you to operate on. It's a 32-bit FUSE identifier; just assign
// it to a yfs_client::inum to get a 64-bit YFS inum.
//
  void
fuseserver_getattr(fuse_req_t req, fuse_ino_t ino,
    struct fuse_file_info *fi)
{
  bool inum_locked = false;
  unsigned long long inum_lid = ino; 
  struct stat st;
  bzero(&st, sizeof(st));
  yfs_client::inum inum = ino; // req->in.h.nodeid;
  yfs_client::status ret;

  //Acquire lock
  printf("fuseserver_getattr: acquiring lock on ino %lu\n", ino);
  yfs->acquirelock(inum_lid);
  inum_locked = true;
  printf("fuseserver_getattr: acquired lock on ino %lu\n", ino);

  ret = getattr(inum, st);
  if(ret != yfs_client::OK){
    fuse_reply_err(req, ENOENT);
    goto release;
  }
  fuse_reply_attr(req, &st, 0);
release:
  if(inum_locked) {
    //Release the lock
    printf("fuseserver_getattr: releasing lock on ino %lu\n", ino);
    yfs->releaselock(inum_lid);
    printf("fuseserver_getattr: released lock on ino %lu\n", ino);
  }
  return;
}

//
// Set the attributes of a file. Often used as part of overwriting
// a file, to set the file length to zero.
//
// to_set is a bitmap indicating which attributes to set. You only
// have to implement the FUSE_SET_ATTR_SIZE bit, which indicates
// that the size of the file should be changed. The new size is
// in attr->st_size. If the new size is bigger than the current
// file size, fill the new bytes with '\0'.
//
// On success, call fuse_reply_attr, passing the file's new
// attributes (from a call to getattr()).
//
  void
fuseserver_setattr(fuse_req_t req, fuse_ino_t ino, struct stat *attr,
    int to_set, struct fuse_file_info *fi)
{
  printf("fuseserver_setattr 0x%x\n", to_set);
  bool inum_locked = false;
  unsigned long long inum_lid = ino; 
  yfs_client::status ret;
  if (FUSE_SET_ATTR_SIZE & to_set) {
    struct stat st;
    bzero(&st, sizeof(st));
    // You fill this in for Lab 2
#if 1
    // Change the above line to "#if 1", and your code goes here
    // Note: fill st using getattr before fuse_reply_attr

    //Acquire lock
    printf("fuseserver_setattr: acquiring lock on ino %lu\n", ino);
    yfs->acquirelock(inum_lid);
    inum_locked = true;
    printf("fuseserver_setattr: acquired lock on ino %lu\n", ino);

    ret = getattr(ino, st);
    if(ret != yfs_client::OK) {
      fuse_reply_err(req, ENOENT);
      goto release;
    }
    std::string val;
    ret = yfs->getvalue(ino, val);   //Get the previous value in file
    if(ret != yfs_client::OK) {
      fuse_reply_err(req, ENOENT);
      goto release;
    }
    if(attr->st_size != val.size()) {
      if(attr->st_size < val.size()) {   //Truncate
        val.resize(attr->st_size);
        printf("truncate %d %lld\n", val.size(), attr->st_size);
      }
      else if(attr->st_size > val.size()) { //Pad with 0s
        val.resize(attr->st_size, '\0');
        printf("Pad %d %lld\n", val.size(), attr->st_size);
      }
      ret = yfs->setvalue(ino, val);
      if(ret != yfs_client::OK) {
        fuse_reply_err(req, ENOENT);
        goto release;
      }
      else {
        st.st_size = val.size();
        fuse_reply_attr(req, &st, 0);
        goto release;
      }
    }
    else {
      st.st_size = val.size();
      fuse_reply_attr(req, &st, 0);
      goto release;
    }
#else
    fuse_reply_err(req, ENOSYS);
#endif
  } else {
    fuse_reply_err(req, ENOSYS);
  }
release:
  if(inum_locked) {
    //Release the lock
    printf("fuseserver_setattr: releasing lock on ino %lu\n", ino);
    yfs->releaselock(inum_lid);
    printf("fuseserver_setattr: released lock on ino %lu\n", ino);
  }
  return;
}

//
// Read up to @size bytes starting at byte offset @off in file @ino.
//
// Pass the number of bytes actually read to fuse_reply_buf.
// If there are fewer than @size bytes to read between @off and the
// end of the file, read just that many bytes. If @off is greater
// than or equal to the size of the file, read zero bytes.
//
// Ignore @fi. 
// @req identifies this request, and is used only to send a 
// response back to fuse with fuse_reply_buf or fuse_reply_err.
//
  void
fuseserver_read(fuse_req_t req, fuse_ino_t ino, size_t size,
    off_t off, struct fuse_file_info *fi)
{
  // You fill this in for Lab 2
#if 1
  // Change the above "#if 0" to "#if 1", and your code goes here
  bool inum_locked = false;
  unsigned long long inum_lid = ino; 
  std::string buf;
  yfs_client::status ret;
  std::string val;

  //Acquire lock
  printf("fuseserver_read: acquiring lock on ino %lu\n", ino);
  yfs->acquirelock(inum_lid);
  inum_locked = true;
  printf("fuseserver_read: acquired lock on ino %lu\n", ino);

  ret = yfs->getvalue(ino, val); 
  if(ret != yfs_client::OK) {
    fuse_reply_err(req, ENOENT);
    goto release;
  }
  if(off >= val.size() ) {
    printf("fuseserver_read: ino %lu offset %llu has already reached EOF %u, reading zero bytes, to read %u bytes\n", ino, off, val.size(), size);
    fuse_reply_buf(req, buf.data(), buf.size());
    goto release;
  }
  else if((val.size()-off) < size) {
    unsigned int availableNoBytes = val.size() - (unsigned int)off;
    printf("fuseserver_read: ino %lu only %u bytes available to read and not %u bytes\n", ino, availableNoBytes, size);
    size = availableNoBytes;
    buf = val.substr(off, size);
    printf("fuseserver_read: %u %u\n", size, buf.size());
    fuse_reply_buf(req, buf.data(), buf.size());
    goto release;
  }
  else {
    buf = val.substr(off, size);
    fuse_reply_buf(req, buf.data(), buf.size());
    goto release;
  }

#else
  fuse_reply_err(req, ENOSYS);
#endif
release:
  if(inum_locked) {
    //Release the lock
    printf("fuseserver_read: releasing lock on ino %lu\n", ino);
    yfs->releaselock(inum_lid);
    printf("fuseserver_read: released lock on ino %lu\n", ino);
  }
  return;
}

//
// Write @size bytes from @buf to file @ino, starting
// at byte offset @off in the file.
//
// If @off + @size is greater than the current size of the
// file, the write should cause the file to grow. If @off is
// beyond the end of the file, fill the gap with null bytes.
//
// Set the file's mtime to the current time.
//
// Ignore @fi.
//
// @req identifies this request, and is used only to send a 
// response back to fuse with fuse_reply_buf or fuse_reply_err.
//
  void
fuseserver_write(fuse_req_t req, fuse_ino_t ino,
    const char *buf, size_t size, off_t off,
    struct fuse_file_info *fi)
{
  // You fill this in for Lab 2
#if 1
  // Change the above line to "#if 1", and your code goes here
  bool inum_locked = false;
  yfs_client::status ret;
  unsigned long long inum_lid = ino; 
  std::string val;
  std::string bufToWrite;
  bufToWrite.append(buf,size);

  //Acquire lock
  printf("fuseserver_write: acquiring lock on ino %lu\n", ino);
  yfs->acquirelock(inum_lid);
  inum_locked = true;
  printf("fuseserver_write: acquired lock on ino %lu\n", ino);

  ret = yfs->getvalue(ino, val);
  if(ret != yfs_client::OK) {
    fuse_reply_err(req, ENOENT);
    goto release;
  }
  else {
    printf("Write %lu off %llu size_to_write %u (buf size %u) current_size %u\n", ino, off, size, bufToWrite.size(), val.size());
    if(off > val.size()) {
      printf("Offset exceeds size of file\n");
      std::string nullbytes(off-val.size(), '\0');  //Creating null bytes to be padded
      val.append(nullbytes,0,off-val.size());
      val.append(bufToWrite,0,bufToWrite.size());
    }
    else if(off == val.size()){
      printf("Writing from EOF\n");
      val.append(bufToWrite,0,bufToWrite.size());
    }
    else {
      printf("Offset %llu in mid file\n", off);
      //Replace content at offset position with buf's contents upto size bytes
      val.replace(off,size,bufToWrite);
    }
    printf("ino %lu size after writing %u\n", ino, val.size());
    //Write to extent server
    ret = yfs->setvalue(ino, val);
    if(ret != yfs_client::OK) {
      fuse_reply_err(req, ENOENT);
      goto release;
    }
    else {
      fuse_reply_write(req, size);
      goto release;
    }
  }
#else
  fuse_reply_err(req, ENOSYS);
#endif
release:
  if(inum_locked) {
    //Release the lock
    printf("fuseserver_write: releasing lock on ino %lu\n", ino);
    yfs->releaselock(inum_lid);
    printf("fuseserver_write: released lock on ino %lu\n", ino);
  }
  return;
}

//
// Create file @name in directory @parent. 
//
// - @mode specifies the create mode of the file. Ignore it - you do not
//   have to implement file mode.
// - If a file named @name already exists in @parent, return EXIST.
// - Pick an ino (with type of yfs_client::inum) for file @name. 
//   Make sure ino indicates a file, not a directory!
// - Create an empty extent for ino.
// - Add a <name, ino> entry into @parent.
// - Change the parent's mtime and ctime to the current time/date
//   (this may fall naturally out of your extent server code).
// - On success, store the inum of newly created file into @e->ino, 
//   and the new file's attribute into @e->attr. Get the file's
//   attributes with getattr().
//
// @return yfs_client::OK on success, and EXIST if @name already exists.
//
  yfs_client::status
fuseserver_createhelper(fuse_ino_t parent, const char *name,
    mode_t mode, struct fuse_entry_param *e)
{
  bool inum_locked = false, parent_locked = false;
  unsigned long long inum_lid, parent_lid = parent; 
  yfs_client::status ret;
  // In yfs, timeouts are always set to 0.0, and generations are always set to 0
  e->attr_timeout = 0.0;
  e->entry_timeout = 0.0;
  e->generation = 0;

  // You fill this in for Lab 2

  //srand(getpid()+time(NULL));
  unsigned long randomNum  = rand();
  yfs_client::inum ino = 0;
  randomNum |= (1<<31);  //Setting 31st bit of random number to be 1
  ino = randomNum;

  inum_lid = ino;
  printf("fuseserver_createhelper: assigning inum %llu to %s\n", inum_lid, name);
  //Acquire lock
  printf("fuseserver_createhelper: acquiring lock on parent %lu\n", parent);
  yfs->acquirelock(parent_lid);
  parent_locked = true;
  printf("fuseserver_createhelper: acquired lock on parent %lu\n", parent);
  //Acquire lock
  printf("fuseserver_createhelper: acquiring lock on ino %llu\n", ino);
  yfs->acquirelock(inum_lid);
  inum_locked = true;
  printf("fuseserver_createhelper: acquired lock on ino %llu\n", ino);

  ret = yfs->createentry(parent, name, ino);
  if(ret == yfs_client::OK) {
    e->ino = ino;
    ret = getattr(ino, e->attr);  //Get attributes of file via getattr with inode ino as key
  }
  if(parent_locked) {
    //Release the lock
    printf("fuseserver_createhelper: releasing lock on parent %lu\n", parent);
    yfs->releaselock(parent_lid);
    printf("fuseserver_createhelper: released lock on parent %lu\n", parent);
  }
  if(inum_locked) {
    //Release the lock
    printf("fuseserver_createhelper: releasing lock on ino %llu\n", ino);
    yfs->releaselock(inum_lid);
    printf("fuseserver_createhelper: released lock on ino %llu\n", ino);
  }
  return ret;
}

  void
fuseserver_create(fuse_req_t req, fuse_ino_t parent, const char *name,
    mode_t mode, struct fuse_file_info *fi)
{
  struct fuse_entry_param e;
  yfs_client::status ret;
  if( (ret = fuseserver_createhelper( parent, name, mode, &e )) == yfs_client::OK ) {
    fuse_reply_create(req, &e, fi);
  } else {
    if (ret == yfs_client::EXIST) {
      fuse_reply_err(req, EEXIST);
    }else{
      fuse_reply_err(req, ENOENT);
    }
  }
}

void fuseserver_mknod( fuse_req_t req, fuse_ino_t parent, 
    const char *name, mode_t mode, dev_t rdev ) {
  struct fuse_entry_param e;
  yfs_client::status ret;
  if( (ret = fuseserver_createhelper( parent, name, mode, &e )) == yfs_client::OK ) {
    fuse_reply_entry(req, &e);
  } else {
    if (ret == yfs_client::EXIST) {
      fuse_reply_err(req, EEXIST);
    }else{
      fuse_reply_err(req, ENOENT);
    }
  }
}

//
// Look up file or directory @name in the directory @parent. If @name is
// found, set e.attr (using getattr) and e.ino to the attribute and inum of
// the file.
//
  void
fuseserver_lookup(fuse_req_t req, fuse_ino_t parent, const char *name)
{
  bool inum_locked = false, parent_locked = false, found;
  unsigned long long inum_lid, parent_lid = parent; 
  struct fuse_entry_param e;
  // In yfs, timeouts are always set to 0.0, and generations are always set to 0
  e.attr_timeout = 0.0;
  e.entry_timeout = 0.0;
  e.generation = 0;
  yfs_client::inum ino;

  //Acquire lock
  printf("fuseserver_lookup: acquiring lock on parent %lu\n", parent);
  yfs->acquirelock(parent_lid);
  parent_locked = true;
  printf("fuseserver_lookup: acquired lock on parent %lu\n", parent);
  found = yfs->dirlookup(parent, name, ino);

  // You fill this in for Lab 2
  if (found) {
    e.ino = ino;
    inum_lid = ino;
    //Acquire lock
    printf("fuseserver_lookup: acquiring lock on ino %llu\n", ino);
    yfs->acquirelock(inum_lid);
    inum_locked = true;
    printf("fuseserver_lookup: acquired lock on ino %llu\n", ino);

    yfs_client::status ret = getattr(e.ino, e.attr);  //Get attributes of file via getattr with inode ino as key
    if(ret != yfs_client::OK) {
      fuse_reply_err(req, ENOENT);
      goto release;
    }
    else {
      fuse_reply_entry(req, &e);
      goto release;
    }
  }
  else {
    fuse_reply_err(req, ENOENT);
  }
release:
  if(parent_locked) {
    //Release the lock
    printf("fuseserver_lookup: releasing lock on parent %lu\n", parent);
    yfs->releaselock(parent_lid);
    printf("fuseserver_lookup: released lock on parent %lu\n", parent);
  }
  if(inum_locked) {
    //Release the lock
    printf("fuseserver_lookup: releasing lock on ino %llu\n", ino);
    yfs->releaselock(inum_lid);
    printf("fuseserver_lookup: released lock on ino %llu\n", ino);
  }
  return;
}

struct dirbuf {
  char *p;
  size_t size;
};

void dirbuf_add(struct dirbuf *b, const char *name, fuse_ino_t ino)
{
  struct stat stbuf;
  size_t oldsize = b->size;
  b->size += fuse_dirent_size(strlen(name));
  b->p = (char *) realloc(b->p, b->size);
  memset(&stbuf, 0, sizeof(stbuf));
  stbuf.st_ino = ino;
  fuse_add_dirent(b->p + oldsize, name, &stbuf, b->size);
}

#define min(x, y) ((x) < (y) ? (x) : (y))

int reply_buf_limited(fuse_req_t req, const char *buf, size_t bufsize,
    off_t off, size_t maxsize)
{
  if ((size_t)off < bufsize)
    return fuse_reply_buf(req, buf + off, min(bufsize - off, maxsize));
  else
    return fuse_reply_buf(req, NULL, 0);
}

//
// Retrieve all the file names / i-numbers pairs
// in directory @ino. Send the reply using reply_buf_limited.
//
// You can ignore @size and @off (except that you must pass
// them to reply_buf_limited).
//
// Call dirbuf_add(&b, name, inum) for each entry in the directory.
//
  void
fuseserver_readdir(fuse_req_t req, fuse_ino_t ino, size_t size,
    off_t off, struct fuse_file_info *fi)
{
  bool inum_locked = false;
  unsigned long long inum_lid = ino; 
  yfs_client::inum inum = ino; // req->in.h.nodeid;
  struct dirbuf b;
  fuse_ino_t inumEntry;
  std::string dircontent, entryname, inoStr;
  size_t currentpos = 0, pos = 0;

  if(!yfs->isdir(inum)){
    fuse_reply_err(req, ENOTDIR);
    return;
  }
  //Acquire lock
  printf("fuseserver_readdir: acquiring lock on ino %lu\n", ino);
  yfs->acquirelock(inum_lid);
  inum_locked = true;
  printf("fuseserver_readdir: acquired lock on ino %lu\n", ino);

  if(yfs->getvalue(ino, dircontent) != yfs_client::OK) {
    fuse_reply_err(req, ENOENT);
    goto release;
  }
  memset(&b, 0, sizeof(b));
  // You fill this in for Lab 2
  while(currentpos != dircontent.size() && (pos = dircontent.find(':', currentpos)) != std::string::npos) {
    //Extract the entry name
    entryname = dircontent.substr(currentpos, pos-currentpos);
    currentpos = pos+1;
    pos = dircontent.find("|",currentpos) ;
    inoStr = dircontent.substr(currentpos, pos-currentpos);
    //Extract the entry inode
    inumEntry = atoll(inoStr.c_str());
    currentpos = pos+1;

    //Adding each entry via dirbuf_add
    dirbuf_add(&b, entryname.c_str(), inumEntry);
    printf("fuseserver_readdir: name %s ino %lu\n", entryname.c_str(), inumEntry);
  }
  reply_buf_limited(req, b.p, b.size, off, size);
  free(b.p);
release:
  if(inum_locked) {
    //Release the lock
    printf("fuseserver_readdir: releasing lock on ino %lu\n", ino);
    yfs->releaselock(inum_lid);
    printf("fuseserver_readdir: released lock on ino %lu\n", ino);
  }
  return;
}


  void
fuseserver_open(fuse_req_t req, fuse_ino_t ino,
    struct fuse_file_info *fi)
{
  fuse_reply_open(req, fi);
}

//
// Create a new directory with name @name in parent directory @parent.
// Leave new directory's inum in e.ino and attributes in e.attr.
//
// The new directory should be empty (no . or ..).
// 
// If a file/directory named @name already exists, indicate error EEXIST.
//
// Ignore mode.
//
  void
fuseserver_mkdir(fuse_req_t req, fuse_ino_t parent, const char *name,
    mode_t mode)
{
  bool inum_locked = false, parent_locked = false;
  unsigned long long inum_lid, parent_lid = parent; 
  struct fuse_entry_param e;
  yfs_client::status ret;
  // In yfs, timeouts are always set to 0.0, and generations are always set to 0
  e.attr_timeout = 0.0;
  e.entry_timeout = 0.0;
  e.generation = 0;

  unsigned long randomNum  = rand();
  yfs_client::inum ino = 0;
  randomNum &= ~(1<<31);  //Setting 31st bit of random number to be 0
  ino = randomNum;
  if(!yfs->isdir(ino)){
    fuse_reply_err(req, ENOTDIR);
    goto release;
  }

  // You fill this in for Lab 3
#if 1
  inum_lid = ino;
  printf("fuseserver_mkdir: assigning inum %llu to %s\n", inum_lid, name);
  //Acquire lock
  printf("fuseserver_mkdir: acquiring lock on parent %lu\n", parent);
  yfs->acquirelock(parent_lid);
  parent_locked = true;
  printf("fuseserver_mkdir: acquired lock on parent %lu\n", parent);

  //Acquire lock
  printf("fuseserver_mkdir: acquiring lock on ino %llu\n", ino);
  yfs->acquirelock(inum_lid);
  inum_locked = true;
  printf("fuseserver_mkdir: acquired lock on ino %llu\n", ino);

  ret = yfs->createentry(parent, name, ino);
  if(ret == yfs_client::OK) {
    e.ino = ino;
    ret = getattr(ino, e.attr);  //Get attributes of file via getattr with inode ino as key
    fuse_reply_entry(req, &e);
  }
  else if(ret == yfs_client::EXIST){
    fuse_reply_err(req, EEXIST);
  }
#else
  fuse_reply_err(req, ENOSYS);
#endif
release:
  if(parent_locked) {
    //Release the lock
    printf("fuseserver_mkdir: releasing lock on parent %lu\n", parent);
    yfs->releaselock(parent_lid);
    printf("fuseserver_mkdir: released lock on parent %lu\n", parent);
  }
  if(inum_locked) {
    //Release the lock
    printf("fuseserver_mkdir: releasing lock on ino %llu\n", ino);
    yfs->releaselock(inum_lid);
    printf("fuseserver_mkdir: release lock on ino %llu\n", ino);
  }
  return;
}

//
// Remove the file named @name from directory @parent.
// Free the file's extent.
// If the file doesn't exist, indicate error ENOENT.
//
// Do *not* allow unlinking of a directory.
//
  void
fuseserver_unlink(fuse_req_t req, fuse_ino_t parent, const char *name)
{

  // You fill this in for Lab 3
  bool inum_locked = false, parent_locked = false, found;
  unsigned long long inum_lid, parent_lid = parent; 
  yfs_client::inum ino;

  //Acquire lock
  printf("fuseserver_unlink: acquiring lock on ino %lu\n", parent);
  yfs->acquirelock(parent_lid);
  parent_locked = true;
  printf("fuseserver_unlink: acquired lock on ino %lu\n", parent);

  found = yfs->dirlookup(parent, name, ino);
  // You fill this in for Lab 2
  if (found && yfs->isfile(ino)) {
    inum_lid = ino;

    //Acquire lock
    printf("fuseserver_unlink: acquiring lock on ino %llu\n", ino);
    yfs->acquirelock(inum_lid);
    inum_locked = true;
    printf("fuseserver_unlink: acquired lock on ino %llu\n", ino);

    printf("fuseserver_unlink: removing inode %llu for file %s\n", ino, name);
    yfs_client::status ret = yfs->removefile(parent, ino, name);
    if(ret != yfs_client::OK) {
      fuse_reply_err(req, ENOENT);
      goto release;
    }
    else { 
      printf("fuseserver_unlink: inode %llu for file %s successfully removed\n", ino, name);
      fuse_reply_err(req, 0);
      goto release;
    }
  }
  else {
    if(found) {
      printf("fuseserver_unlink: entry %s is a dir. Cannot unlink\n", name);
    }
    else {
      printf("fuseserver_unlink: dir does not contain the file %s\n", name);
    }
    fuse_reply_err(req, ENOENT);
    goto release;
  }
release:
  if(parent_locked) {
    //Release the lock
    printf("fuseserver_unlink: releasing lock on ino %llu\n", ino);
    yfs->releaselock(parent_lid);
    printf("fuseserver_unlink: released lock on ino %llu\n", ino);
  }
  if(inum_locked) {
    //Release the lock
    printf("fuseserver_unlink: releasing lock on ino %llu\n", ino);
    yfs->releaselock(inum_lid);
    printf("fuseserver_unlink: released lock on ino %llu\n", ino);
  }
}

  void
fuseserver_statfs(fuse_req_t req)
{
  struct statvfs buf;

  printf("statfs\n");

  memset(&buf, 0, sizeof(buf));

  buf.f_namemax = 255;
  buf.f_bsize = 512;

  fuse_reply_statfs(req, &buf);
}

struct fuse_lowlevel_ops fuseserver_oper;

  int
main(int argc, char *argv[])
{
  char *mountpoint = 0;
  int err = -1;
  int fd;

  setvbuf(stdout, NULL, _IONBF, 0);

  if(argc != 4){
    fprintf(stderr, "Usage: yfs_client <mountpoint> <port-extent-server> <port-lock-server>\n");
    exit(1);
  }
  mountpoint = argv[1];

  //srand(getpid()+time(NULL));
  srand(getpid());

  myid = rand();

  yfs = new yfs_client(argv[2], argv[3]);

  fuseserver_oper.getattr    = fuseserver_getattr;
  fuseserver_oper.statfs     = fuseserver_statfs;
  fuseserver_oper.readdir    = fuseserver_readdir;
  fuseserver_oper.lookup     = fuseserver_lookup;
  fuseserver_oper.create     = fuseserver_create;
  fuseserver_oper.mknod      = fuseserver_mknod;
  fuseserver_oper.open       = fuseserver_open;
  fuseserver_oper.read       = fuseserver_read;
  fuseserver_oper.write      = fuseserver_write;
  fuseserver_oper.setattr    = fuseserver_setattr;
  fuseserver_oper.unlink     = fuseserver_unlink;
  fuseserver_oper.mkdir      = fuseserver_mkdir;

  const char *fuse_argv[20];
  int fuse_argc = 0;
  fuse_argv[fuse_argc++] = argv[0];
#ifdef __APPLE__
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "nolocalcaches"; // no dir entry caching
  fuse_argv[fuse_argc++] = "-o";
  fuse_argv[fuse_argc++] = "daemon_timeout=86400";
#endif

  // everyone can play, why not?
  //fuse_argv[fuse_argc++] = "-o";
  //fuse_argv[fuse_argc++] = "allow_other";

  fuse_argv[fuse_argc++] = mountpoint;
  fuse_argv[fuse_argc++] = "-d";

  fuse_args args = FUSE_ARGS_INIT( fuse_argc, (char **) fuse_argv );
  int foreground;
  int res = fuse_parse_cmdline( &args, &mountpoint, 0 /*multithreaded*/, 
      &foreground );
  if( res == -1 ) {
    fprintf(stderr, "fuse_parse_cmdline failed\n");
    return 0;
  }

  args.allocated = 0;

  fd = fuse_mount(mountpoint, &args);
  if(fd == -1){
    fprintf(stderr, "fuse_mount failed\n");
    exit(1);
  }

  struct fuse_session *se;

  se = fuse_lowlevel_new(&args, &fuseserver_oper, sizeof(fuseserver_oper),
      NULL);
  if(se == 0){
    fprintf(stderr, "fuse_lowlevel_new failed\n");
    exit(1);
  }

  struct fuse_chan *ch = fuse_kern_chan_new(fd);
  if (ch == NULL) {
    fprintf(stderr, "fuse_kern_chan_new failed\n");
    exit(1);
  }

  fuse_session_add_chan(se, ch);
  // err = fuse_session_loop_mt(se);   // FK: wheelfs does this; why?
  err = fuse_session_loop(se);

  fuse_session_destroy(se);
  close(fd);
  fuse_unmount(mountpoint);

  return err ? 1 : 0;
}
