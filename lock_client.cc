// RPC stubs for clients to talk to lock_server

#include "lock_client.h"
#include "rpc.h"
#include <arpa/inet.h>

#include <sstream>
#include <iostream>
#include <stdio.h>

#define _DEBUG_

lock_client::lock_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() < 0) {
    printf("lock_client: call bind\n");
  }
}

lock_protocol::status
lock_client::stat(lock_protocol::lockid_t lid)
{
  int r;
  lock_protocol::status ret = cl->call(lock_protocol::stat, cl->id(), lid, r);
  printf("lock_client: stat call return for lid %llu\n", lid);
  VERIFY (ret == lock_protocol::OK);
  return r;
}

lock_protocol::status
lock_client::acquire(lock_protocol::lockid_t lid)
{
  int r;
  lock_protocol::status ret = cl->call(lock_protocol::acquire, cl->id(), lid, r);
  printf("lock_client: acquire call return for lid %llu\n", lid);
  VERIFY (ret == lock_protocol::OK);
  return r;
}

lock_protocol::status
lock_client::release(lock_protocol::lockid_t lid)
{
  int r;
  lock_protocol::status ret = cl->call(lock_protocol::release, cl->id(), lid, r);
  printf("lock_client: release call return for lid %llu\n", lid);
  VERIFY (ret == lock_protocol::OK);
  return r;
}

