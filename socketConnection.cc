#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <pthread.h>
#include <list>
#include <string>
#include <string.h>
#include <sys/select.h>
#include "lang/verify.h"
#include "socketConnection.h"

#define PORT 21567

pthread_mutex_t sock_m = PTHREAD_MUTEX_INITIALIZER;
struct dataToSend {
  int sockfd;
  std::string buff;
};
std::list<struct dataToSend>sendList;
void Sleep(int nsec);

void *senderThread(void *p) {
  while(1) {
    VERIFY(pthread_mutex_lock(&sock_m) == 0);
    if(!sendList.empty()) {
      struct dataToSend ds = sendList.front();
      sendList.pop_front();
      unsigned int totalSent = 0;
      const char *buf = ds.buff.c_str();
      if(strncmp(buf, "DECIDE",strlen("DECIDE")) == 0) {
        Sleep(2);
      }
      //printf("senderThread: %s\n", buf);
      while(totalSent != strlen(buf)) {
        int sent = send(ds.sockfd, buf+totalSent, strlen(buf)-totalSent, 0);
        if(sent < 0) {
          perror("senderThread: ");
          break;
        }
        totalSent += sent;
      }
      /*printf("senderThread to recv\n");
      char buffer[1024];
        int ret = recv(sockfd, buffer, 100, 0);
        if(ret < 0) {
        perror("sendData: recv error ");
        }
        printf("sendData received %s\n", buffer);*/
    }
    VERIFY(pthread_mutex_unlock(&sock_m) == 0);
    Sleep(2);
  }
  return NULL;
}


int makeSocket() {
  int sockfd ;
  sockfd  = socket(AF_INET, SOCK_STREAM, 0);

  if(sockfd < 0) {
    perror("makeSocket creation error: ");
  }
  return sockfd;
}

int makeConnection() {
  int sockfd = makeSocket();
  if(sockfd < 0) {
    return -1;
  }
  struct sockaddr_in dest;
  memset(&dest, 0, sizeof(dest));                /* zero the struct */
  dest.sin_family = AF_INET;
  dest.sin_addr.s_addr = inet_addr("127.0.0.1"); /* set destination IP number */ 
  dest.sin_port = htons(PORT);                /* set destination port number */

  int ret = connect(sockfd, (struct sockaddr *)&dest, sizeof(struct sockaddr));

  if(ret < 0) {
    perror("makeConnect failed: ");
    return -1;
  }
  pthread_t tsend_t;
  VERIFY(pthread_create(&tsend_t, NULL, &senderThread, NULL) == 0);
  pthread_detach(tsend_t);
  return sockfd;
}

int sendData(int sockfd, const char *buff) {
  if(sockfd == -1) {
    return -1;
  }
  char buf[1024];
  strcpy(buf, buff);
  strcat(buf, "\n");
  VERIFY(pthread_mutex_lock(&sock_m) == 0);
  //printf("sendData: %d %s\n", sockfd, buf);
  struct dataToSend ds;
  ds.sockfd = sockfd;
  ds.buff = buf;
  sendList.push_back(ds);
  VERIFY(pthread_mutex_unlock(&sock_m) == 0);

  return 0;
}

int makeNewConnection(const char *buff) {
  int sockfd = makeConnection();
  if(sockfd < 0) {
    return 0;
  }
  sendData(sockfd, buff);
  return  sockfd;
}

void Sleep(int nsec) {
  struct timeval tv;
  tv.tv_sec = nsec;
  tv.tv_usec = 0;
  select(0, NULL, NULL, NULL, &tv);
}
