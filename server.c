#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <time.h>
#include <errno.h>
#include <stdarg.h>

#define PORT 9999
#define NUM_OF_SERVERS 4
#define HEAP_MAX_SIZE NUM_OF_SERVERS+1

struct backend_appliance
{
  char ip[16];
  uint16_t port;
  size_t active_connections;
  size_t total_selected;
  size_t failure_count;
  int server_index;
  int is_healthy;
  time_t last_failed_time;
};

enum close_reason {
  CLOSE_CLIENT_EOF, //0
  CLOSE_BACKEND_EOF, //1
  CLOSE_CLIENT_ERROR, //2
  CLOSE_BACKEND_ERROR,//3
  CLOSE_TIMEOUT, //4
  CLOSE_CONNECT_FAILED,//5
  CLOSE_PROXY_INTERNAL_ERROR//6
};

const char *close_reason_names[] = {
    "CLOSE_CLIENT_EOF",
    "CLOSE_BACKEND_EOF",
    "CLOSE_CLIENT_ERROR",
    "CLOSE_BACKEND_ERROR",
    "CLOSE_TIMEOUT",
    "CLOSE_CONNECT_FAILED",
    "CLOSE_PROXY_INTERNAL_ERROR"
};

enum close_reason reason = CLOSE_CLIENT_EOF;

struct backend_appliance backend_server;

struct heap
{
  struct backend_appliance data[HEAP_MAX_SIZE];
  int size;
};

  struct backend_appliance backend_servers[NUM_OF_SERVERS] = { {"127.0.0.1",8081,0,0,0,0,1,0}, {"127.0.0.1",8082,0,0,0,1,1,0}, {"127.0.0.1",8083,0,0,0,2,1,0}, {"127.0.0.1",8084,0,0,0,3,1,0}};
  int head = -1;
  struct heap h;
 

void swap(struct backend_appliance *h1, struct backend_appliance *h2)
{
  struct backend_appliance temp = *h1;
  *h1 = *h2;
  *h2 = temp;
}

void heapify_up(struct heap *h,int i)
{
  while(i > 0)
    {
      int parent = (i-1)/2;

      if(h->data[parent].active_connections > h->data[i].active_connections)
	{
	  swap(&h->data[parent],&h->data[i]);
	  i = parent;
	}
      else
	{
	  break;
	}
    }
  return;
}

void heapify_down(struct heap *h,int i)
{
  while(1)
    {
      int left = 2*i+1;
      int right = 2*i+2;
      int smallest = i;
      if(left < h->size && h->data[left].active_connections < h->data[smallest].active_connections )
	{
	  smallest = left;
	}
      if(right < h->size && h->data[right].active_connections < h->data[smallest].active_connections)
	{
	  smallest = right;
	}

      if(smallest == i)
	break;

      swap(&h->data[i], &h->data[smallest]);

      i = smallest;
    }
  return;
}

struct backend_appliance get_min_backend(struct heap *h)
{
  struct backend_appliance appliance = h->data[0];
  h->data[0] = h->data[h->size-1];
  h->size--;
  heapify_down(h,0);
  return appliance;
}

void insert(struct heap *h, struct backend_appliance * backend_server)
{
  if(h->size == HEAP_MAX_SIZE)
    {
      return;
    }
  h->data[h->size] = *backend_server;
  h->size++;
  heapify_up(h,h->size-1);
  
  return;
}

void update_heap(struct heap *h, int server_index,int increment)
{
  for(int i = 0; i < h->size; i++)
    {
      if(h->data[i].server_index == server_index)
        {
	  if(increment)
	    {
	      h->data[i].active_connections++;
	    }
	  else
	    {
	      h->data[i].active_connections--;
	    }
          heapify_up(h,i);
          heapify_down(h,i);
        }
    }

  return;
}


struct endpoint
{
  int fd;
  int isclient_fd;
  struct connection* conn; 
};

  size_t total_connections = 0;
  size_t active_connections = 0;
  size_t backend_failures = 0;
  size_t requests_served = 0;
  size_t timeouts = 0;
  size_t bytes_transferred = 0;
  size_t next_conn_id = 1;

struct connection
{
  int client_fd;
  int backend_fd;

  int retry_count;
  struct connection* next;
  time_t last_activity; 
  int closed;
  

  char client_ip[16];
  char backend_ip[16];
  uint16_t client_port;
  uint16_t backend_port;
  
  char client_buffer[8192];
  size_t client_buff_totallen;
  size_t client_buff_sentlen;
  
  char backend_buffer[8192];
  size_t backend_buff_totallen;
  size_t backend_buff_sentlen;

  struct endpoint* client_endpoint;
  struct endpoint* backend_endpoint;

  size_t conn_id;
  int firsttime;
  int backend_index;
  int keep_alive;
  
};

struct connection *conn_head = NULL;

void logging(char *level, char *msg, ...) 
{
  time_t now = time(NULL);
  char timestamp[30];
  strftime(timestamp, sizeof(timestamp),"%Y-%m-%d %H:%M:%S", localtime(&now));
  fprintf(stderr, "%s [%s]: ",timestamp, level);
  va_list args;
  va_start(args,msg);
  vfprintf(stderr,msg,args);
  va_end(args);
  fprintf(stderr,"\n");
}

int set_sock_nonblock(int fd)
{
  int flags = fcntl(fd,F_GETFL, 0);
  if(flags < 0)
  {
   return -1;
  }
  if(fcntl(fd, F_SETFL, flags | O_NONBLOCK) <0)
  {
   return -1;
  }
  return 0;
}

void rmv_conn_list(struct connection *ptr)
{
    
  if(!conn_head)
    {
      logging("INFO", "No nodes exist to remove from linked_list connections");
      return;
    }

  struct connection *temp = conn_head;

  if(conn_head->client_fd == ptr->client_fd)
    {
      conn_head = conn_head->next;
      //free(temp);
      return;
    }
  

  while(temp->next != NULL && temp->next->client_fd != ptr->client_fd)
    {
      temp = temp->next;
    }

  if(temp->next)
    {
      struct connection *temp1 = temp->next;
      temp->next = temp->next->next;
      //free(temp1);
      logging("INFO", "node with client id [%d] is removed from linked_list connections", temp1->client_fd);
    }
  else
    {
      logging("INFO", "node is not found in linked_list connections");
    }
  return;
}

void add_conn_list(struct connection *ptr)
{
  if(conn_head == NULL)
    {
      conn_head = ptr;
      return;
    }

  struct connection *temp = conn_head;
  
  while(temp->next != NULL)
    {
      temp = temp->next;
    }
  temp->next = ptr;
  logging("INFO", "node with client id [%d] is added to linked_list connections", ptr->client_fd);
  return;
}

 

int create_backend(struct connection *c, int epfd)
{
   struct sockaddr_in backend_address;
  logging("INFO","[conn=%zu] new backend is in creattion process",  c->conn_id);
   int backend_fd;
   backend_fd = socket(AF_INET, SOCK_STREAM, 0);
   if(backend_fd < 0)
   {
     logging("ERROR", strerror(errno));
     free(c->client_endpoint);
     free(c->backend_endpoint);
     epoll_ctl(epfd,EPOLL_CTL_DEL,c->client_fd,NULL);
                                       
     close(c->client_fd);
     c->closed = 1;
     rmv_conn_list(c);
     reason = CLOSE_PROXY_INTERNAL_ERROR;
      logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
     free(c);
     active_connections--;
     return -1;
    }
    logging("INFO","backend socket is created ");  
		  
    if(set_sock_nonblock(backend_fd) < 0)
    {
      logging("ERROR", strerror(errno));
      free(c->client_endpoint);
      free(c->backend_endpoint);
      epoll_ctl(epfd,EPOLL_CTL_DEL,c->client_fd,NULL);
                                       
      close(c->client_fd);
      close(backend_fd);
      c->closed = 1;
      rmv_conn_list(c);
      
      reason = CLOSE_PROXY_INTERNAL_ERROR;
      logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
      free(c);
      active_connections--;
				       
      return -1;
				
    }
   logging("INFO", "backend socket is set to non blocking mode ");
   
		  
   memset(&backend_address,0,sizeof(backend_address));
   backend_server = get_min_backend(&h);
   backend_servers[backend_server.server_index].active_connections++;
   insert(&h,&backend_server);
   update_heap(&h, backend_server.server_index, 1);
   backend_servers[backend_server.server_index].total_selected++;
   while(1)
   {
     if (!backend_servers[backend_server.server_index].is_healthy && (time(NULL)-backend_servers[backend_server.server_index].last_failed_time < 30))
     {
       backend_servers[backend_server.server_index].active_connections--;
       update_heap(&h, backend_server.server_index, 0);

       backend_server = get_min_backend(&h);
       backend_servers[backend_server.server_index].active_connections++;
       insert(&h,&backend_server);
       update_heap(&h, backend_server.server_index, 1);
       backend_servers[backend_server.server_index].total_selected++;

      }
      else
      {
	break;
      }
    }
    backend_servers[backend_server.server_index].is_healthy =1;
    backend_address.sin_port = htons(backend_server.port);
    backend_address.sin_family = AF_INET;
    c->backend_index = backend_server.server_index;
    if (inet_pton(AF_INET,backend_server.ip, &backend_address.sin_addr) <= 0)
    {
      logging("ERROR",strerror(errno));
      free(c->client_endpoint);
      free(c->backend_endpoint);
      epoll_ctl(epfd,EPOLL_CTL_DEL,c->client_fd,NULL);
                                       
      close(c->client_fd);
      close(backend_fd);
      c->closed = 1;
      rmv_conn_list(c);
      backend_servers[c->backend_index].active_connections--;
      update_heap(&h, c->backend_index, 0);
      reason = CLOSE_PROXY_INTERNAL_ERROR;
      logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
      free(c);
      active_connections--;
      return -1;
     }
     logging("INFO","backend socket is assigned with %s IP address  ",inet_ntoa(backend_address.sin_addr));

     c->backend_fd = backend_fd;
     inet_ntop(AF_INET,&backend_address.sin_addr,c->backend_ip,INET_ADDRSTRLEN);
     c->backend_port = backend_server.port;
     c->backend_endpoint->fd = c->backend_fd;
     c->firsttime =1;

     logging("INFO", "client[%s:%d] is successfully added to epoll polling",c->client_ip, c->client_port);
     struct epoll_event backend_event;
     backend_event.events = EPOLLIN;
     backend_event.data.ptr = c->backend_endpoint;

     if(epoll_ctl(epfd,EPOLL_CTL_ADD,c->backend_fd,&backend_event) < 0)
     {
       free(c->client_endpoint);
       free(c->backend_endpoint);
       epoll_ctl(epfd,EPOLL_CTL_DEL,c->client_fd,NULL);
       epoll_ctl(epfd,EPOLL_CTL_DEL,c->backend_fd,NULL);
       close(c->client_fd);
       close(c->backend_fd);
       c->closed = 1;
       rmv_conn_list(c);
       backend_servers[c->backend_index].active_connections--;
       update_heap(&h, c->backend_index, 0);
       reason = CLOSE_PROXY_INTERNAL_ERROR;
       logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
       free(c);
       active_connections--;
       return -1;
                                  
       }
      logging("INFO", "backend [%s:%d] is successfully added to epoll polling",c->backend_ip,c->backend_port);
      if(connect(c->backend_fd,(struct sockaddr *)&backend_address,sizeof(backend_address)) < 0)
      {
	if(errno == EINPROGRESS)
	{
	  struct epoll_event client_event_4;
          client_event_4.events = 0;
          client_event_4.data.ptr = c->client_endpoint;
          epoll_ctl(epfd,EPOLL_CTL_MOD,c->client_fd,&client_event_4);
	  
	  backend_event.events = EPOLLOUT;
	  backend_event.data.ptr = c->backend_endpoint;
	  epoll_ctl(epfd,EPOLL_CTL_MOD,c->backend_fd,&backend_event);
	  logging("INFO","retry connect");
	  return 1;
	 }

	  epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
	  epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
	  close(c->client_fd);
	  close(c->backend_fd);
	  free(c->client_endpoint);
	  free(c->backend_endpoint);
	  c->closed = 1;
	  rmv_conn_list(c);
	  backend_servers[c->backend_index].active_connections--;
	  update_heap(&h, c->backend_index, 0);
	  backend_servers[c->backend_index].failure_count++;
	  backend_servers[c->backend_index].is_healthy = 0;
	  backend_servers[c->backend_index].last_failed_time = time(NULL);
	  reason = CLOSE_BACKEND_ERROR;
          logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
	  free(c);
	  active_connections--;
	  return -1;;
       }
}

int main()
{
  int server_fd, client_fd,backend_fd;
  struct sockaddr_in server_address, client_address;
   struct sockaddr_in backend_address;
  socklen_t client_len = sizeof(client_address);
  int n;
  h.size = 0;
          
  server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if(server_fd < 0)
    {
      logging("ERROR", strerror(errno));
      return 1;
    }
  logging("INFO", "Server socket is created successfully");

  memset(&server_address, 0, sizeof(server_address));
  server_address.sin_addr.s_addr = INADDR_ANY;
  server_address.sin_port = htons(9999);
  server_address.sin_family = AF_INET;

  if(set_sock_nonblock(server_fd) == -1)
    {
      close(server_fd);
      logging("ERROR","setting socket to non blocking failed");
      return 1;
    }
  logging("INFO","server socket is set to non blocking succeeded");

  int opt =1;
  setsockopt(server_fd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt));

  if(bind(server_fd, (struct sockaddr *)&server_address, sizeof(server_address)) < 0)
    {
      logging("ERROR", strerror(errno));
      close(server_fd);
      return 1;
    }
  logging("INFO", "Server is bind to IP address %s and port %d", inet_ntoa(server_address.sin_addr), PORT);

  if(listen(server_fd,5) < 0)
    {
      logging("ERROR", strerror(errno));
      close(server_fd);
      return 1;
    }
  logging("INFO", "SEREVR IS LISTENING");

  int epfd = epoll_create1(0);
  if(epfd < 0)
    {
      logging("ERROR",strerror(errno));
      close(server_fd);
      return 1;
    }
  logging("INFO", "epoll is created");


  
  struct epoll_event server_event;
  server_event.events = EPOLLIN;

  struct endpoint server_var = { -1, -1, NULL };
  server_var.fd = server_fd;
  server_event.data.ptr = &server_var;

  if(epoll_ctl(epfd,EPOLL_CTL_ADD,server_fd, &server_event) < 0)
    {
      logging("ERROR",strerror(errno));
      close(server_fd);
      return 1;
    }
  
  struct epoll_event event_all[100];



  for(int i = 0; i < NUM_OF_SERVERS; i++)
    {
      insert(&h, &backend_servers[i]);
    }
  
  while(1)
    {
      int nfds = epoll_wait(epfd,event_all, 100, 10000);

      if(nfds == 0)
      {
	  if(conn_head == NULL)
	    {
	      continue;
	    }
	  struct connection *temp = conn_head, *temp1;
	  

	  while(temp != NULL)
	    {
	      struct connection *next = temp->next;
	      if(time(NULL)-temp->last_activity > 30)
		{
		  rmv_conn_list(temp);
		  epoll_ctl(epfd, EPOLL_CTL_DEL, temp->client_fd, NULL);
		  epoll_ctl(epfd, EPOLL_CTL_DEL, temp->backend_fd, NULL);
		  close(temp->client_fd);
		  close(temp->backend_fd);
		  free(temp->client_endpoint);
		  free(temp->backend_endpoint);
		  active_connections--;
		  update_heap(&h, temp->backend_index, 0);
                  backend_servers[temp->backend_index].active_connections--;
		  timeouts++;
		  temp->closed = 1;
		  logging("INFO", "Connection [conn = %zu] is inactive for more than 30 seconds, connection is timeout out", temp->conn_id);
		  reason = CLOSE_TIMEOUT;
		  logging("INFO", "[conn=%zu] closed reason=%s", temp->conn_id, close_reason_names[reason]);
		  free(temp);
		  
		}
	      temp = next;
	    }
	  
	  }
      
       
      for(int i=0 ; i<nfds; i++)
	{
	  if(event_all[i].data.ptr == &server_var)
	    {
	      while(1)
		{
		  memset(&client_address, 0, sizeof(client_address));
		  
		  client_fd = accept(server_fd, (struct sockaddr *)&client_address,&client_len);
	      
		  if(client_fd < 0)
		    {
		      if(errno == EAGAIN || errno == EWOULDBLOCK)
			{
			  break;
			}
		      else
			{
			  logging("ERROR", strerror(errno));
			  
			  continue;
			}
		    }

		  total_connections++;
                  active_connections++;
                  
		  
		  logging ("INFO", "TOTAL NO OF CONNECTIONS : [%ld]",total_connections);
		  logging ("INFO", "TOTAL NO OF ACTIVE CONNECTIONS : [%ld]",active_connections);
		  logging ("INFO", "TOTAL NO OF BACKEND FAILURES : [%ld]",backend_failures);
		  logging ("INFO", "TOTAL NO OF REQUESTS SERVED : [%ld]",requests_served);
		  logging ("INFO", "TOTAL NO OF CONNECTION TIMEOUTS : [%ld]",timeouts);
		  logging ("INFO", "TOTAL NO OF BYTES TRANSFERRED : [%ld]",bytes_transferred);

		  
		  logging("INFO","client with IP address %s connected ",inet_ntoa(client_address.sin_addr));
		   
		  backend_fd = socket(AF_INET, SOCK_STREAM, 0);
		  
		  if(backend_fd < 0)
		    {
		      logging("ERROR", strerror(errno));
		      close(client_fd);
		      active_connections--;
		      continue;
		    }
		  logging("INFO","backend socket is created ");

		  if(set_sock_nonblock(client_fd) < 0)
		    {
		      logging("ERROR", strerror(errno));
		      close(client_fd);
		      close(backend_fd);
		      active_connections--;
		      continue;
		    }

		  logging("INFO", "Client socket is set to non blocking mode ");

		    if(set_sock_nonblock(backend_fd) < 0)
                    {
                      logging("ERROR", strerror(errno));
                      close(client_fd);
                      close(backend_fd);
		      active_connections--;
                      continue;
                    }
		  logging("INFO", "backend socket is set to non blocking mode ");

                  memset(&backend_address,0,sizeof(backend_address));
		  //head = (head+1)%NUM_OF_SERVERS;

		  
		  backend_server = get_min_backend(&h);
                  backend_servers[backend_server.server_index].active_connections++;
		  insert(&h,&backend_server);
		  update_heap(&h, backend_server.server_index, 1);
		  backend_servers[backend_server.server_index].total_selected++;

		      while(!backend_servers[backend_server.server_index].is_healthy && (time(NULL)-backend_servers[backend_server.server_index].last_failed_time < 30))
		     {
		        backend_servers[backend_server.server_index].active_connections--;
			update_heap(&h, backend_server.server_index, 0);
   
		        backend_server = get_min_backend(&h);
			backend_servers[backend_server.server_index].active_connections++;
			insert(&h,&backend_server);
			update_heap(&h, backend_server.server_index, 1);
			backend_servers[backend_server.server_index].total_selected++;
		    }
		  backend_servers[backend_server.server_index].is_healthy =1;
		  backend_address.sin_port = htons(backend_server.port);
		  backend_address.sin_family = AF_INET;
		  
		  if (inet_pton(AF_INET,backend_servers[backend_server.server_index].ip, &backend_address.sin_addr) <= 0)
		    {
		      logging("ERROR",strerror(errno));
		      close(client_fd);
		      close(backend_fd);
		      active_connections--;
		      update_heap(&h, backend_server.server_index, 0);
		      backend_servers[backend_server.server_index].active_connections--;
		      continue;
		    }

		  logging("INFO","backend socket is assigned with %s IP address  ",inet_ntoa(backend_address.sin_addr));
		 		  
	       	  struct connection *connection_ptr = malloc(sizeof(struct connection));
		  if(!connection_ptr)
		    {
		      logging("ERROR", "connection pointer alloc allocation failed");
		      close(client_fd);
		      close(backend_fd);
		      active_connections--;
		      update_heap(&h, backend_server.server_index, 0);
		      backend_servers[backend_server.server_index].active_connections--;
		      continue;
		      
		    }
                  
		  connection_ptr->client_fd = client_fd;
		  connection_ptr->backend_fd = backend_fd;
		  connection_ptr->client_buff_totallen = 0;
		  connection_ptr->client_buff_sentlen = 0;
		  connection_ptr->client_buffer[connection_ptr->client_buff_totallen] = '\0';
		  connection_ptr->backend_buff_totallen = 0;
		  connection_ptr->backend_buff_sentlen = 0;
                  connection_ptr->backend_buffer[connection_ptr->backend_buff_totallen] = '\0';
		  connection_ptr->firsttime = 1;
		  inet_ntop(AF_INET,&client_address.sin_addr,connection_ptr->client_ip,INET_ADDRSTRLEN);
		  connection_ptr->client_port = ntohs(client_address.sin_port) ;
		  inet_ntop(AF_INET,&backend_address.sin_addr,connection_ptr->backend_ip,INET_ADDRSTRLEN);
                  connection_ptr->backend_port = backend_server.port;
		  connection_ptr->closed = 0;
		  connection_ptr->backend_index = backend_server.server_index;
		  connection_ptr->next = NULL;
		  connection_ptr->conn_id = next_conn_id++;
		  
		  connection_ptr->last_activity = time(NULL);
		  add_conn_list(connection_ptr);
		  
		  connection_ptr->client_endpoint = (struct endpoint *)malloc(sizeof(struct endpoint));
		  connection_ptr->retry_count = 3;

		  logging("INFO", "[conn=%zu] accepted client %s:%d",connection_ptr->conn_id, connection_ptr->client_ip, connection_ptr->client_port);

		  logging("INFO", "[conn=%zu] selected backend %s:%d",connection_ptr->conn_id, connection_ptr->backend_ip, connection_ptr->backend_port);

		  

		  if(connection_ptr->client_endpoint == NULL)
		    {
		      logging("ERROR", "Connection [conn=%zu] client_endpoint malloc allocation failed for client [%s:%d] and backend server [%s:%d]",connection_ptr-> conn_id, connection_ptr->client_ip, connection_ptr->client_port, connection_ptr->backend_ip,connection_ptr->backend_port);
		      close(client_fd);
		      close(backend_fd);
		      connection_ptr->closed = 1;
		      rmv_conn_list(connection_ptr);
		      update_heap(&h, connection_ptr->backend_index, 0);
		      backend_servers[connection_ptr->backend_index].active_connections--;
		      reason = CLOSE_PROXY_INTERNAL_ERROR;
                      logging("INFO", "[conn=%zu] closed reason=%s", connection_ptr->conn_id, close_reason_names[reason]);
		      free(connection_ptr);
		      active_connections--;
		      
		      continue;
		      
		    }

		  connection_ptr->backend_endpoint = (struct endpoint *)malloc(sizeof(struct endpoint));
                  if(connection_ptr->backend_endpoint == NULL)
                    {
		      logging("ERROR", "Connection [conn=%zu] backend_endpoint malloc allocation failed  for client [%s:%d] and backend server [%s:%d]",connection_ptr->conn_id,connection_ptr->client_ip, connection_ptr->client_port, connection_ptr->backend_ip,connection_ptr->backend_port);
                      close(client_fd);
                      close(backend_fd);
		      free(connection_ptr->client_endpoint);
		      connection_ptr->closed = 1;
		      rmv_conn_list(connection_ptr);
		      update_heap(&h, connection_ptr->backend_index, 0);
		      backend_servers[connection_ptr->backend_index].active_connections--;
		      reason = CLOSE_PROXY_INTERNAL_ERROR;
                      logging("INFO", "[conn=%zu] closed reason=%s", connection_ptr->conn_id, close_reason_names[reason]);
                      free(connection_ptr);
		      active_connections--;
		      
                      continue;

                    }
		  connection_ptr->client_endpoint->fd = client_fd;
		  connection_ptr->client_endpoint->isclient_fd = 1;
		  connection_ptr->client_endpoint->conn = connection_ptr;

		  connection_ptr->backend_endpoint->fd = backend_fd;
                  connection_ptr->backend_endpoint->isclient_fd = 0;
                  connection_ptr->backend_endpoint->conn = connection_ptr;
		  
                  
		  struct epoll_event client_event;
		  client_event.events = EPOLLIN;
		  client_event.data.ptr = connection_ptr->client_endpoint;

		  if(epoll_ctl(epfd,EPOLL_CTL_ADD,client_fd,&client_event) < 0)
		    {
		     
		      free(connection_ptr->client_endpoint);
		      free(connection_ptr->backend_endpoint);
		      connection_ptr->closed = 1;
		      close(connection_ptr->client_fd);
                      close(connection_ptr->backend_fd);
		      rmv_conn_list(connection_ptr);
		      update_heap(&h, connection_ptr->backend_index, 0);
		      backend_servers[connection_ptr->backend_index].active_connections--;
		      reason = CLOSE_PROXY_INTERNAL_ERROR;
                      logging("INFO", "[conn=%zu] closed reason=%s", connection_ptr->conn_id, close_reason_names[reason]);
		      free(connection_ptr);
		      logging("ERROR", strerror(errno));
		      active_connections--;
		      
		      continue;
		    }
		  
		  logging("INFO", "client[%s:%d] is successfully added to epoll polling",connection_ptr->client_ip, connection_ptr->client_port);
                  struct epoll_event backend_event;
                  backend_event.events = EPOLLIN;
                  backend_event.data.ptr = connection_ptr->backend_endpoint;
		  
		   if(epoll_ctl(epfd,EPOLL_CTL_ADD,backend_fd,&backend_event) < 0)
                    {
		      free(connection_ptr->client_endpoint);
                      free(connection_ptr->backend_endpoint);
		      connection_ptr->closed = 1;
		      epoll_ctl(epfd,EPOLL_CTL_DEL,client_fd,NULL);
                      close(client_fd);
                      close(backend_fd);
		      rmv_conn_list(connection_ptr);
		      update_heap(&h, connection_ptr->backend_index, 0);
		      backend_servers[connection_ptr->backend_index].active_connections--;
		      reason = CLOSE_PROXY_INTERNAL_ERROR;
                      logging("INFO", "[conn=%zu] closed reason=%s", connection_ptr->conn_id, close_reason_names[reason]);
                      free(connection_ptr);
                      logging("ERROR", strerror(errno));
		      active_connections--;
		      
		      continue;
                    }
		   logging("INFO", "backend [%s:%d] is successfully added to epoll polling",connection_ptr->backend_ip,connection_ptr->backend_port);
                   if(connect(backend_fd,(struct sockaddr *)&backend_address,sizeof(backend_address)) < 0)
		   {
		     if(errno == EINPROGRESS)
                      {
			  struct epoll_event client_event_4;
                          client_event_4.events = 0;
                          client_event_4.data.ptr = connection_ptr->client_endpoint;
                          epoll_ctl(epfd,EPOLL_CTL_MOD,connection_ptr->client_fd,&client_event_4);

			  
			  backend_event.events = EPOLLOUT;
                          backend_event.data.ptr = connection_ptr->backend_endpoint;
                          epoll_ctl(epfd,EPOLL_CTL_MOD,backend_fd,&backend_event);
                          continue;  		    
		      }
		               
		     logging("INFO","CONNECT IMMEDIATE FAILURE");
		              epoll_ctl(epfd, EPOLL_CTL_DEL, connection_ptr->client_fd, NULL);
                              epoll_ctl(epfd, EPOLL_CTL_DEL, connection_ptr->backend_fd, NULL);
                              close(connection_ptr->client_fd);
                              close(connection_ptr->backend_fd);
                              free(connection_ptr->client_endpoint);
                              free(connection_ptr->backend_endpoint);
                              connection_ptr->closed = 1;
			      rmv_conn_list(connection_ptr);
			      update_heap(&h, connection_ptr->backend_index, 0);
			      backend_servers[connection_ptr->backend_index].active_connections--;
			      backend_servers[connection_ptr->backend_index].failure_count++;
			      backend_servers[connection_ptr->backend_index].is_healthy = 0;
			      backend_servers[connection_ptr->backend_index].last_failed_time = time(NULL);
			      reason = CLOSE_BACKEND_ERROR;
                              logging("INFO", "[conn=%zu] closed reason=%s", connection_ptr->conn_id, close_reason_names[reason]);
                              free(connection_ptr);
			      active_connections--;
			      continue;
		   
		   }
		}
	    }
	    else
	    {

	      struct endpoint *end_ptr = event_all[i].data.ptr;
	      if(!end_ptr)
	      {
		continue;
	      }
	      printf("DEBUG: Processing event. Pointer address: %p\n", (void*)end_ptr);
	      if (end_ptr == NULL) {
		fprintf(stderr, "Error: end_ptr is NULL!\n");
		continue;
	      }

	      struct connection *c = end_ptr->conn;
	      if(c == NULL || c->closed)
              {
                continue;
              }
	      	              	      
	      if(end_ptr->isclient_fd)
		{       
		  if(event_all[i].events & EPOLLIN)
		    {
		      
		      
		      if(c->client_buff_totallen == 0)
		      {
			  logging("INFO","EPOLLIN event triggered for client socket [%s:%u] ",c->client_ip,c->client_port);
			  memset(c->client_buffer,0,sizeof(c->client_buffer));
		      }
			  ssize_t bytes_read = recv(c->client_fd,c->client_buffer+c->client_buff_totallen,sizeof(c->client_buffer)-c->client_buff_totallen-1,0);
                          c->last_activity = time(NULL);
			  if(bytes_read < 0)
			    {
			      if(errno == EAGAIN || errno == EWOULDBLOCK)
				{
				  continue;
				}
			      else
				{
				   epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
                                   epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
                                   close(c->client_fd);
                                   close(c->backend_fd);
                                   free(c->client_endpoint);
                                   free(c->backend_endpoint);
				   c->closed = 1;
				   rmv_conn_list(c);
				   active_connections--;
				   update_heap(&h, c->backend_index, 0);
				   backend_servers[c->backend_index].active_connections--;
				   backend_servers[c->backend_index].failure_count++;
				   backend_servers[c->backend_index].is_healthy = 0;
                                   backend_servers[c->backend_index].last_failed_time = time(NULL);
				   reason = CLOSE_CLIENT_ERROR;
                                   logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
                                   free(c);
				  logging("ERROR", "Receive failed , client socket closed");
				  continue;
				}
			    }
			  else if (bytes_read == 0)
			    {
			      // peer closed connection
			      logging("INFO", "connection closed by peer");

			      epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
			      epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
                              close(c->client_fd);
			      close(c->backend_fd);
			      free(c->client_endpoint);
                              free(c->backend_endpoint);
			      c->closed = 1;
			      active_connections--;
			      update_heap(&h, c->backend_index, 0);
			      backend_servers[c->backend_index].active_connections--;
			      rmv_conn_list(c);
			      reason = CLOSE_CLIENT_EOF;
                              logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
			      free(c);
			      continue;
			    }
			  else     
			    {
			      
			      bytes_transferred += bytes_read;
			      logging("INFO", "client socket [%s:%d] is receiving data",c->client_ip,c->client_port);
                              c->client_buff_totallen += bytes_read;
		      	      c->client_buffer[c->client_buff_totallen] = '\0';

			      logging("INFO","client buffer total length is %d",c->client_buff_totallen);
                              logging("INFO","client buffer sent length is %d",c->client_buff_sentlen);
                              logging("INFO", "client buffer is %s",c->client_buffer);
			      logging("INFO", "backend buffer is %s",c->backend_buffer);

			      if(strstr(c->client_buffer, "\r\n\r\n") == NULL)
				{
				  continue;
				}
			      logging("INFO","[conn=%zu] Received complete request",  c->conn_id);

			      if(strstr(c->client_buffer, "Connection: close") != NULL || strstr(c->client_buffer, "HTTP/1.0") != NULL)
				{
				  c->keep_alive = 0;
				  logging("INFO","[conn=%zu] is close after request",  c->conn_id);
				}
			      else
				{
				  c->keep_alive = 1;
				  logging("INFO","[conn=%zu] is keep-alive",  c->conn_id);
		      		}

			     

			      if(c->backend_fd == -1)
				{
				  logging("INFO", "EHLO");
				  if(create_backend(c,epfd) == -1)
				    continue;
				}
                              
			      requests_served++;
                              			       
			      struct epoll_event client_pause;
                              client_pause.events = 0;
                              client_pause.data.ptr = c->client_endpoint;
                              epoll_ctl(epfd,EPOLL_CTL_MOD,c->client_fd,&client_pause);
			       
			      struct epoll_event backend_event_1;
                              backend_event_1.events = EPOLLOUT;
			      backend_event_1.data.ptr = c->backend_endpoint;
                              epoll_ctl(epfd,EPOLL_CTL_MOD,c->backend_fd,&backend_event_1);
			      continue;
			    }
			  //}
		      }
		  
		    if(event_all[i].events & EPOLLOUT)
                    {
		      
                      if(c->backend_buff_totallen > 0)
                        {
                          logging("INFO","EPOLLOUT event triggered for client socket [%s:%d] ",c->client_ip,c->client_port);    
                          n = send(c->client_fd, c->backend_buffer+c->backend_buff_sentlen, c->backend_buff_totallen-c->backend_buff_sentlen,0);
                          c->last_activity = time(NULL);
                          if(n < 0)
                            {
                              if(errno == EAGAIN || errno == EWOULDBLOCK)
                                {
                                  continue;
                                }
                              else
                                {
                                  epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
				  epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
				  close(c->client_fd);
				  close(c->backend_fd);
				  free(c->client_endpoint);
				  free(c->backend_endpoint);
				  c->closed = 1;
				  active_connections--;
				  update_heap(&h, c->backend_index, 0);
				  backend_servers[c->backend_index].active_connections--;
				  backend_servers[c->backend_index].failure_count++;
				  backend_servers[c->backend_index].is_healthy = 0;
                                  backend_servers[c->backend_index].last_failed_time = time(NULL);
				  rmv_conn_list(c);
				  reason = CLOSE_CLIENT_ERROR;
                                  logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
				  free(c);
                                  logging("ERROR", "header send failed , client socket closed");
                                  continue;
                                }
                            }
			    else if (n == 0)
                            {
                              // peer closed connection                                                                                                                                                                                                                       
			      
                              logging("INFO", "connection closed by peer");
                              epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
                              epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
                              close(c->client_fd);
                              close(c->backend_fd);
                              free(c->client_endpoint);
                              free(c->backend_endpoint);
			      c->closed = 1;
			      active_connections--;
			      update_heap(&h, c->backend_index, 0);
			      backend_servers[c->backend_index].active_connections--;
			      rmv_conn_list(c);
			      reason = CLOSE_CLIENT_EOF;
                              logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
                              free(c);
			      
                              continue;
                            }
                          else
                            {
			       logging("INFO", " data is sent on client socket [%s:%d] ",c->client_ip,c->client_port);
			       bytes_transferred += n;
                              c->backend_buff_sentlen += n;
                              char *ptr = c->backend_buffer+c->backend_buff_sentlen;
                              int remaining = c->backend_buff_totallen-c->backend_buff_sentlen;
                              memmove(c->backend_buffer,ptr,remaining);
                              c->backend_buff_totallen -= c->backend_buff_sentlen;
                              c->backend_buffer[c->backend_buff_totallen] = '\0';
                              c->backend_buff_sentlen = 0;
			      logging("INFO","backend buffer total length is %d",c->backend_buff_totallen);
                              logging("INFO","backend buffer sent length is %d",c->backend_buff_sentlen);
                              logging("INFO", "backend buffer is %s",c->backend_buffer);
			      logging("INFO", "client buffer is %s",c->client_buffer);
                              
                            }
                        }
		      if(c->backend_buff_totallen == 0)
			{
			   
			  struct epoll_event client_event_2;
                          client_event_2.events = EPOLLIN;
			  client_event_2.data.ptr = c->client_endpoint;
                          epoll_ctl(epfd,EPOLL_CTL_MOD,c->client_fd,&client_event_2);

			  struct epoll_event backend_event_re;
			  backend_event_re.events = EPOLLIN;
			  backend_event_re.data.ptr = c->backend_endpoint;
			  epoll_ctl(epfd,EPOLL_CTL_MOD,c->backend_fd,&backend_event_re);
			  
			  
			}
		    }
                      		   
		}
		else if(!end_ptr->isclient_fd)
		{  
		  if(event_all[i].events & EPOLLIN)
		    {
		      logging("INFO","EPOLLIN event triggered for backend socket [%s:%u]",c->backend_ip,c->backend_port);
		      
		      if(c->backend_buff_totallen == 0)
			{
			  memset(c->backend_buffer,0,sizeof(c->backend_buffer));
			  ssize_t bytes_read = recv(c->backend_fd,c->backend_buffer+c->backend_buff_totallen,sizeof(c->backend_buffer)-c->backend_buff_totallen-1,0);
                          c->last_activity = time(NULL);
			  if(bytes_read < 0)
			    {
			      if(errno == EAGAIN || errno == EWOULDBLOCK)
				{
				  continue;
				}
			      else
				{
				  epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
				  epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
				  close(c->client_fd);
				  close(c->backend_fd);
				  free(c->client_endpoint);
				  free(c->backend_endpoint);
				  c->closed = 1;
				  active_connections--;
				  backend_servers[c->backend_index].failure_count++;
				  update_heap(&h, c->backend_index, 0);
				  backend_servers[c->backend_index].active_connections--;
				  backend_servers[c->backend_index].is_healthy = 0;
                                  backend_servers[c->backend_index].last_failed_time = time(NULL);
				  rmv_conn_list(c);
				  reason = CLOSE_BACKEND_ERROR;
                                  logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
				  free(c);
				  
				  logging("ERROR", "Receive failed , client socket closed");
				  continue;
				}
			    }
			  else if (bytes_read == 0)
                            {

			      
                              // peer closed connection
                              logging("INFO", "connection closed by peer");

			      //epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
                              epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
                              //close(c->client_fd);
                              close(c->backend_fd);
                              //free(c->client_endpoint);
                              //free(c->backend_endpoint);
			      //c->closed = 1;
			      active_connections--;
			      update_heap(&h, c->backend_index, 0);
			      backend_servers[c->backend_index].active_connections--;
			      //rmv_conn_list(c);
			      reason = CLOSE_BACKEND_EOF;
                              logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
			      if(c->keep_alive)
				{
				  c->backend_fd = -1;
				  logging("INFO", "[conn=%zu] backend is closed but client connection is alive", c->conn_id);
				}
			      else
				{
				  epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL); 
				  close(c->client_fd); 
				  free(c->client_endpoint);                                                                                                                               
                                  free(c->backend_endpoint);                                                                                                                              
                                  c->closed = 1; 
				  rmv_conn_list(c);
				  logging("INFO", "[conn=%zu] backend is closed and client connection is closed", c->conn_id);
				  free(c);
				 
				}
			      struct epoll_event client_event_2;                                                                                                                                                                                                                 
                              client_event_2.events = EPOLLIN;                                                                                                                                                                                                                   
                              client_event_2.data.ptr = c->client_endpoint;                                                                                                                                                                                                      
                              epoll_ctl(epfd,EPOLL_CTL_MOD,c->client_fd,&client_event_2); 
                              
			      
                              continue;
                            }

			  else      
			    {
			      logging("INFO", "backend socket [%s:%d] is receiving data ",c->backend_ip,c->backend_port);
			      bytes_transferred += bytes_read;
                              c->backend_buff_totallen += bytes_read;
		      	      c->backend_buffer[c->backend_buff_totallen] = '\0';
			      logging("INFO","backend buffer total length is %d",c->backend_buff_totallen);
                              logging("INFO","backend buffer sent length is %d",c->backend_buff_sentlen);
                              logging("INFO", "backend buffer is %s",c->backend_buffer);
			      logging("INFO", "client buffer is %s",c->client_buffer);

			      struct epoll_event backend_pause;
                              backend_pause.events = 0;
                              backend_pause.data.ptr = c->backend_endpoint;
                              epoll_ctl(epfd,EPOLL_CTL_MOD,c->backend_fd,&backend_pause);
			      
			      struct epoll_event client_event_1;
                              client_event_1.events = EPOLLOUT;
			      client_event_1.data.ptr = c->client_endpoint;
                              epoll_ctl(epfd,EPOLL_CTL_MOD,c->client_fd,&client_event_1);
			      continue;
			    }
			}	
		      
		    }
		

		   if(event_all[i].events & EPOLLOUT)
                    {
		      logging("INFO","EPOLLOUT event triggered on backend socket [%s:%d]",c->backend_ip,c->backend_port);
                      
                        
			  if(c->firsttime)
			    {
			      int err = 0;
			      socklen_t len = sizeof(err);
			      if(getsockopt(c->backend_fd, SOL_SOCKET, SO_ERROR, &err , &len) < 0)
				{
			          epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
				  epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
				  close(c->client_fd);
				  close(c->backend_fd);
				  free(c->client_endpoint);
				  free(c->backend_endpoint);
				  c->closed = 1;
				  active_connections--;
				   backend_servers[c->backend_index].failure_count++;
				   backend_servers[c->backend_index].active_connections--;
				  update_heap(&h, c->backend_index, 0);
				  backend_servers[c->backend_index].is_healthy = 0;
                                  backend_servers[c->backend_index].last_failed_time = time(NULL);
				  rmv_conn_list(c);
				  reason = CLOSE_BACKEND_ERROR;
                                  logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
				  free(c);
                                  logging("ERROR", "getsockopt has failed");
                                  continue;
			      	}

			      if(err != 0)
				{
		                  
				  if(!c->retry_count)
				    {
				       
				       logging("INFO",strerror(errno));
                                       logging("INFO","connection between client [%s:%d] and backend [%s:%d] failed",c->client_ip, c->client_port, c->backend_ip,c->backend_port);
                                       free(c->client_endpoint);
                                       free(c->backend_endpoint);
                                       epoll_ctl(epfd,EPOLL_CTL_DEL,c->client_fd,NULL);
                                       epoll_ctl(epfd,EPOLL_CTL_DEL,c->backend_fd,NULL);
                                       close(c->client_fd);
                                       close(c->backend_fd);
                                       c->closed = 1;
				       rmv_conn_list(c);
				       active_connections--;
				        backend_servers[c->backend_index].active_connections--;
				       update_heap(&h, c->backend_index, 0);
				       reason = CLOSE_BACKEND_ERROR;
                                  logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
                                       free(c);
                                       continue;
				    }
				  logging("INFO", "[conn=%zu] backend failed, retrying",c->conn_id);
				   backend_failures++;
				    backend_servers[c->backend_index].failure_count++;
				   logging("INFO","Retrying for another backend server");
				    logging("INFO","connection between client [%s:%d] and backend [%s:%d] failed",c->client_ip, c->client_port, c->backend_ip,c->backend_port);
				    c->retry_count--;
                                    epoll_ctl(epfd,EPOLL_CTL_DEL,c->backend_fd,NULL);
			            close(c->backend_fd);
		
				    backend_fd = socket(AF_INET, SOCK_STREAM, 0);
				    if(backend_fd < 0)
				      {
				       logging("ERROR", strerror(errno));
				       free(c->client_endpoint);
                                       free(c->backend_endpoint);
                                       epoll_ctl(epfd,EPOLL_CTL_DEL,c->client_fd,NULL);
                                       
                                       close(c->client_fd);
                                       
                                       c->closed = 1;
				       rmv_conn_list(c);
				       backend_servers[c->backend_index].active_connections--;
				       update_heap(&h, c->backend_index, 0);
				       reason = CLOSE_PROXY_INTERNAL_ERROR;
                                  logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
                                       free(c);
				       
				       active_connections--;
				       
                                       continue;
				      }
				    logging("INFO","backend socket is created ");  
		  
				    if(set_sock_nonblock(backend_fd) < 0)
				      {
				       logging("ERROR", strerror(errno));
				       free(c->client_endpoint);
                                       free(c->backend_endpoint);
                                       epoll_ctl(epfd,EPOLL_CTL_DEL,c->client_fd,NULL);
                                       
                                       close(c->client_fd);
                                       close(backend_fd);
                                       c->closed = 1;
				       rmv_conn_list(c);
				       update_heap(&h, c->backend_index, 0);
				       backend_servers[c->backend_index].active_connections--;
				       reason = CLOSE_PROXY_INTERNAL_ERROR;
                                  logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
                                       free(c);
				       active_connections--;
				       
                                       continue;
				
				      }
				    logging("INFO", "backend socket is set to non blocking mode ");

		  
				    memset(&backend_address,0,sizeof(backend_address));
			            backend_server = get_min_backend(&h);
				    backend_servers[backend_server.server_index].active_connections++;
				    insert(&h,&backend_server);
				    update_heap(&h, backend_server.server_index, 1);
				    backend_servers[backend_server.server_index].total_selected++;
				    while(1)
				      {
					if (!backend_servers[backend_server.server_index].is_healthy && (time(NULL)-backend_servers[backend_server.server_index].last_failed_time < 30))
					  {
					    backend_servers[backend_server.server_index].active_connections--;
					    update_heap(&h, backend_server.server_index, 0);

					    backend_server = get_min_backend(&h);
					    backend_servers[backend_server.server_index].active_connections++;
					    insert(&h,&backend_server);
					    update_heap(&h, backend_server.server_index, 1);
					    backend_servers[backend_server.server_index].total_selected++;

					  }
					else
					  {
					    break;
					  }
				      }
				    backend_servers[backend_server.server_index].is_healthy =1;
				    backend_address.sin_port = htons(backend_server.port);
				    backend_address.sin_family = AF_INET;
                                        c->backend_index = backend_server.server_index;
				    if (inet_pton(AF_INET,backend_server.ip, &backend_address.sin_addr) <= 0)
				      {
				       logging("ERROR",strerror(errno));
				       free(c->client_endpoint);
                                       free(c->backend_endpoint);
                                       epoll_ctl(epfd,EPOLL_CTL_DEL,c->client_fd,NULL);
                                       
                                       close(c->client_fd);
                                       close(backend_fd);
                                       c->closed = 1;
				       rmv_conn_list(c);
				       backend_servers[c->backend_index].active_connections--;
				       update_heap(&h, c->backend_index, 0);
				       reason = CLOSE_PROXY_INTERNAL_ERROR;
                                  logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
                                       free(c);
				       active_connections--;
                                       continue;
				      }
				    logging("INFO","backend socket is assigned with %s IP address  ",inet_ntoa(backend_address.sin_addr));

				    c->backend_fd = backend_fd;
				    inet_ntop(AF_INET,&backend_address.sin_addr,c->backend_ip,INET_ADDRSTRLEN);
				    c->backend_port = backend_server.port;
				    c->backend_endpoint->fd = c->backend_fd;
				    c->firsttime =1;

				    logging("INFO", "client[%s:%d] is successfully added to epoll polling",c->client_ip, c->client_port);
				    struct epoll_event backend_event;
				    backend_event.events = EPOLLIN;
				    backend_event.data.ptr = c->backend_endpoint;

				    if(epoll_ctl(epfd,EPOLL_CTL_ADD,c->backend_fd,&backend_event) < 0)
				      {
					free(c->client_endpoint);
                                       free(c->backend_endpoint);
                                       epoll_ctl(epfd,EPOLL_CTL_DEL,c->client_fd,NULL);
                                       epoll_ctl(epfd,EPOLL_CTL_DEL,c->backend_fd,NULL);
                                       close(c->client_fd);
                                       close(c->backend_fd);
                                       c->closed = 1;
				       rmv_conn_list(c);
				       backend_servers[c->backend_index].active_connections--;
				       update_heap(&h, c->backend_index, 0);
				       reason = CLOSE_PROXY_INTERNAL_ERROR;
                                  logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
                                       free(c);
				       active_connections--;
				       continue;
                                  
				      }
				    logging("INFO", "backend [%s:%d] is successfully added to epoll polling",c->backend_ip,c->backend_port);
				    if(connect(c->backend_fd,(struct sockaddr *)&backend_address,sizeof(backend_address)) < 0)
				    {
				      if(errno == EINPROGRESS)
					{
					  backend_event.events = EPOLLOUT;
					  backend_event.data.ptr = c->backend_endpoint;
					  epoll_ctl(epfd,EPOLL_CTL_MOD,c->backend_fd,&backend_event);
					  logging("INFO","retry connect");
					  continue;
					}

				      epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
				      epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
				      close(c->client_fd);
				      close(c->backend_fd);
				      free(c->client_endpoint);
				      free(c->backend_endpoint);
				      c->closed = 1;
				      rmv_conn_list(c);
				      backend_servers[c->backend_index].active_connections--;
				      update_heap(&h, c->backend_index, 0);
				      backend_servers[c->backend_index].failure_count++;
				      backend_servers[c->backend_index].is_healthy = 0;
				      backend_servers[c->backend_index].last_failed_time = time(NULL);
				      reason = CLOSE_BACKEND_ERROR;
                                  logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
				      free(c);
				      active_connections--;
				      
				      continue;

				    }

				   		      		                     
				   }
			           c->firsttime = 0;
				   
				   c->last_activity = time(NULL);
			           logging("INFO","connection between client  %s %u and backend  %s %u is connected ",c->client_ip, c->client_port, c->backend_ip, c->backend_port);
				    struct epoll_event backend_event_3;
				   if(c->client_buff_totallen > 0)
				     {
				       backend_event_3.events = EPOLLOUT;
				     }
				   else
				     {
				       backend_event_3.events = EPOLLIN;
				     }
			           backend_event_3.data.ptr = c->backend_endpoint;
			           epoll_ctl(epfd,EPOLL_CTL_MOD,c->backend_fd,&backend_event_3);

				   struct epoll_event client_event_5;
                                   client_event_5.events = EPOLLIN;
                                   client_event_5.data.ptr = c->client_endpoint;
                                   epoll_ctl(epfd,EPOLL_CTL_MOD,c->client_fd,&client_event_5);
		              	      

			       }
			  if(c->client_buff_totallen > 0)
			    {
                           
			  logging("INFO", "client [%s:%d] is sending data to backend server",c->client_ip,c->client_port);
                          
                          n = send(c->backend_fd, c->client_buffer+c->client_buff_sentlen, c->client_buff_totallen-c->client_buff_sentlen,0);
                          c->last_activity = time(NULL);
                          if(n < 0)
                            {
                              if(errno == EAGAIN || errno == EWOULDBLOCK)
                                {
                                  continue;
                                }
                              else
                                {
                                  epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
                                  epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
                                  close(c->client_fd);
                                  close(c->backend_fd);
                                  free(c->client_endpoint);
                                  free(c->backend_endpoint);
				  c->closed = 1;
				  active_connections--;
				  backend_servers[c->backend_index].failure_count++;
				  backend_servers[c->backend_index].is_healthy = 0;
                                  backend_servers[c->backend_index].last_failed_time = time(NULL);
				  update_heap(&h, c->backend_index, 0);
				  backend_servers[c->backend_index].active_connections--;
				  reason = CLOSE_BACKEND_ERROR;
                                  logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
				  rmv_conn_list(c);
                                  free(c);
				  
                                  logging("ERROR", "header send failed , client socket closed");
                                  continue;
                                }
                            }
			  else if (n == 0)
                            {
                              // peer closed connection
			      
                              logging("INFO", "connection closed by peer");

                              epoll_ctl(epfd, EPOLL_CTL_DEL, c->client_fd, NULL);
                              epoll_ctl(epfd, EPOLL_CTL_DEL, c->backend_fd, NULL);
                              close(c->client_fd);
                              close(c->backend_fd);
                              free(c->client_endpoint);
                              free(c->backend_endpoint);
			      c->closed = 1;
			      update_heap(&h, c->backend_index, 0);
			      backend_servers[c->backend_index].active_connections--;
			      active_connections--;
			      reason = CLOSE_BACKEND_EOF;
                              logging("INFO", "[conn=%zu] closed reason=%s", c->conn_id, close_reason_names[reason]);
			      rmv_conn_list(c);
                              free(c);
			      continue;
                            }

                          else
                            {
			      logging("INFO", "data is sent on backend socket [%s:%d]",c->backend_ip,c->backend_port);
			      bytes_transferred +=n;
                              c->client_buff_sentlen += n;
                              char *ptr = c->client_buffer+c->client_buff_sentlen;
                              int remaining = c->client_buff_totallen-c->client_buff_sentlen;
                              memmove(c->client_buffer,ptr,remaining);
			      
                              c->client_buff_totallen -= c->client_buff_sentlen;
                              c->client_buffer[c->client_buff_totallen] = '\0';
			      c->client_buff_sentlen = 0;
			      logging("INFO","client buffer total length is %d",c->client_buff_totallen);
                              logging("INFO","client buffer sent length is %d",c->client_buff_sentlen);
			      logging("INFO", "client buffer is %s",c->client_buffer);
			      logging("INFO", "backend buffer is %s",c->backend_buffer);
                            }
                        
		           if(c->client_buff_totallen == 0)
			   {
			   
			   struct epoll_event backend_event_2;
                           backend_event_2.events = EPOLLIN;
			   backend_event_2.data.ptr = c->backend_endpoint;
                           epoll_ctl(epfd,EPOLL_CTL_MOD,c->backend_fd,&backend_event_2);
			  }
			}
		    }
		
		}

	    
	    }
	  
	}
    }

		
  close(epfd);
  close(server_fd);
  
  return 0;
}

