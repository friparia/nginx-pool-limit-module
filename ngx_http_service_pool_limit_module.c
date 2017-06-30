#include <ngx_config.h>
#include <ngx_core.h>
#include <ngx_http.h>
#include <time.h>

typedef struct{
  u_char color;
  u_char len;
  ngx_msec_t timer_key;
  ngx_uint_t ref_cnt;
  u_char *data;
} ngx_http_service_pool_limit_node_t;

typedef struct{
  ngx_uint_t limit;
  ngx_uint_t timeout;
  ngx_shm_zone_t *shm_zone;
} ngx_http_service_pool_limit_conf_t;

typedef struct{
  ngx_rbtree_node_t *node;
  ngx_shm_zone_t *shm_zone;
} ngx_http_service_pool_limit_event_data_t;

typedef struct {
  ngx_rbtree_t  *rbtree;
  ngx_uint_t *conn;
} ngx_http_service_pool_limit_ctx_t;

static char* ngx_http_service_pool_limit(ngx_conf_t* cf, ngx_command_t* cmd, void* conf);
static void* ngx_http_service_pool_limit_create_conf(ngx_conf_t* cf);
static ngx_int_t ngx_http_service_pool_limit_init(ngx_conf_t *cf);

static ngx_command_t ngx_http_service_pool_limit_commands[] = {
  {
    ngx_string("service_pool_limit"), //The command name
    NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
    ngx_http_service_pool_limit,
    NGX_HTTP_SRV_CONF_OFFSET,
    offsetof(ngx_http_service_pool_limit_conf_t, limit),
    NULL
  },
  {
    ngx_string("service_pool_limit_timeout"), //The command name
    NGX_HTTP_SRV_CONF | NGX_CONF_TAKE1,
    ngx_conf_set_num_slot,
    NGX_HTTP_SRV_CONF_OFFSET,
    offsetof(ngx_http_service_pool_limit_conf_t, timeout),
    NULL
  },
  ngx_null_command
};

static ngx_http_module_t ngx_http_service_pool_limit_module_ctx = {
  NULL,
  ngx_http_service_pool_limit_init,
  NULL,
  NULL,
  ngx_http_service_pool_limit_create_conf,
  NULL,
  NULL,
  NULL,
};

ngx_module_t ngx_http_service_pool_limit_module = {
  NGX_MODULE_V1,
  &ngx_http_service_pool_limit_module_ctx,
  ngx_http_service_pool_limit_commands,
  NGX_HTTP_MODULE,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  NGX_MODULE_V1_PADDING
};
static ngx_rbtree_node_t *
ngx_http_service_pool_limit_timer_rbtree_lookup(ngx_rbtree_t *rbtree, uint32_t hash, ngx_log_t *log){
  ngx_rbtree_node_t           *node, *sentinel;

  node = rbtree->root;
  sentinel = rbtree->sentinel;

  while (node != sentinel) {

    if (hash < (uint32_t)node->key) {
      node = node->left;
      continue;
    }

    if (hash > (uint32_t)node->key) {
      node = node->right;
      continue;
    }

    /* hash == node->key */
    return node;
  }

  return NULL;
}

static ngx_rbtree_node_t *
ngx_http_service_pool_limit_lookup(ngx_rbtree_t *rbtree, uint32_t hash, u_char *key, u_char len)
{
  ngx_int_t                    rc;
  ngx_rbtree_node_t           *node, *sentinel;
  ngx_http_service_pool_limit_node_t  *spl;

  node = rbtree->root;
  sentinel = rbtree->sentinel;

  while (node != sentinel) {

    if (hash < node->key) {
      node = node->left;
      continue;
    }

    if (hash > node->key) {
      node = node->right;
      continue;
    }

    /* hash == node->key */
    spl = (ngx_http_service_pool_limit_node_t *) &node->color;

    rc = ngx_memn2cmp(key, spl->data, len, (size_t) spl->len);

    if (rc == 0) {
      return node;
    }

    node = (rc < 0) ? node->left : node->right;

    return node;
  }

  return NULL;
}

static void
ngx_http_service_pool_limit_rbtree_insert_value(ngx_rbtree_node_t *temp,
                                        ngx_rbtree_node_t *node, ngx_rbtree_node_t *sentinel)
{
  ngx_rbtree_node_t           **p;
  ngx_http_service_pool_limit_node_t   *spln, *splnt;

  for ( ;; ) {

    if (node->key < temp->key) {

      p = &temp->left;

    } else if (node->key > temp->key) {

      p = &temp->right;

    } else { /* node->key == temp->key */

      spln = (ngx_http_service_pool_limit_node_t *) &node->color;
      splnt = (ngx_http_service_pool_limit_node_t *) &temp->color;

      p = (ngx_memn2cmp(spln->data, splnt->data, spln->len, splnt->len) < 0)
        ? &temp->left : &temp->right;
    }

    if (*p == sentinel) {
      break;
    }

    temp = *p;
  }

  *p = node;
  node->parent = temp;
  node->left = sentinel;
  node->right = sentinel;
  ngx_rbt_red(node);
}

static void ngx_service_pool_limit_event_expire_handler(ngx_event_t *ev){
  ngx_http_service_pool_limit_event_data_t *data;
  ngx_slab_pool_t *shpool;
  ngx_http_service_pool_limit_ctx_t *ctx;
  ngx_shm_zone_t *shm_zone;
  ngx_http_service_pool_limit_node_t *spl;
  ngx_rbtree_node_t *node;
  data = ev->data;
  shm_zone = data->shm_zone;
  node = data->node;
  ctx = shm_zone->data;
  spl = (ngx_http_service_pool_limit_node_t *) &node->color;
  shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;
  ngx_shmtx_lock(&shpool->mutex);
  if(spl->ref_cnt == 1){
    (*ctx->conn)--;
    ngx_rbtree_delete(ctx->rbtree, node);
    ngx_slab_free_locked(shpool, node);
    ngx_slab_free_locked(shpool, data);
    ngx_slab_free_locked(shpool, ev->log);
    ngx_slab_free_locked(shpool, ev);
  }else{
    spl->ref_cnt--;
  }
  ngx_shmtx_unlock(&shpool->mutex);
}


static ngx_int_t ngx_http_service_pool_limit_handler(ngx_http_request_t* r){
  ngx_http_service_pool_limit_conf_t *splcf;
  ngx_uint_t limit;
  ngx_uint_t timeout;
  ngx_http_service_pool_limit_ctx_t *ctx;
  u_char *key;
  u_char len = 0;
  struct sockaddr_in *sin;
  struct sockaddr_in6 *sin6;
  ngx_slab_pool_t *shpool;
  ngx_rbtree_node_t *node;
  ngx_rbtree_node_t *timer_node;
  uint32_t hash;
  size_t n;
  ngx_event_t *stop_service_event;
  ngx_event_t *ev;
  ngx_log_t *event_log;
  ngx_http_service_pool_limit_event_data_t *event_data;
  ngx_http_service_pool_limit_node_t * spl;
  ngx_msec_t      timer_key;
  ngx_msec_int_t  timer_diff;
  ngx_table_elt_t *h;
  ngx_str_t header_key = ngx_string("Connection-In-Pool");
  u_char connection_count[64] = {0};
  ngx_str_t header_value;

  splcf = ngx_http_get_module_srv_conf(r, ngx_http_service_pool_limit_module);
  if(splcl->shm_zone == NULL){
    return NGX_DECLINED;
  }

  limit = splcf->limit;
  timeout = splcf->timeout;

  if(timeout == (ngx_uint_t) NGX_CONF_UNSET){
    timeout = 60;
  }
  key = ngx_palloc(r->pool, 32);

  ctx = splcf->shm_zone->data;

  ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: pid-%d, conn-%d, limit-%d, timeout-%d", ngx_getpid() ,*ctx->conn, limit, timeout);

  switch (r->connection->sockaddr->sa_family){
  case AF_INET:
    sin = (struct sockaddr_in *) r->connection->sockaddr;
    key = (u_char *)&sin->sin_addr.s_addr;
    len = 4;
    break;
  case AF_INET6:
    sin6 = (struct sockaddr_in6 *) r->connection->sockaddr;
    len = 16;
    key = sin6->sin6_addr.s6_addr;
    break;
  }

  hash = ngx_crc32_short(key, len);

  shpool = (ngx_slab_pool_t *) splcf->shm_zone->shm.addr;

  ngx_shmtx_lock(&shpool->mutex);

  node = ngx_http_service_pool_limit_lookup(ctx->rbtree, hash, key, len);

  ngx_sprintf(connection_count, "%d", *ctx->conn);
  header_value.len = ngx_strlen(connection_count);
  header_value.data = connection_count;
  h = ngx_list_push(&r->headers_out.headers);
  if(h != NULL){
    h->hash = 1;
    h->key = header_key;
    h->value = header_value;
  }

  if(node == NULL){
    if(*ctx->conn < limit){
      n = offsetof(ngx_rbtree_node_t, color)
        + offsetof(ngx_http_service_pool_limit_node_t, data)
        + offsetof(ngx_http_service_pool_limit_node_t, ref_cnt)
        + offsetof(ngx_http_service_pool_limit_node_t, timer_key)
        + len;

      node = ngx_slab_alloc_locked(shpool, n);
      if (node == NULL){
        ngx_shmtx_unlock(&shpool->mutex);
        return NGX_ERROR;
      }


      spl = (ngx_http_service_pool_limit_node_t *) &node->color;
      node->key = hash;
      spl->data = key;
      spl->len = len;
      switch (r->connection->sockaddr->sa_family){
      case AF_INET:
        ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: create-node-%d.%d.%d.%d", key[0], key[1], key[2], key[3]);
        break;
      case AF_INET6:
        ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: create-node-%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d:%d", key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7], key[8], key[9], key[10], key[11], key[12], key[13], key[14], key[15]);
        break;
      }

      (*ctx->conn)++;
      event_data = ngx_slab_alloc_locked(shpool, sizeof(ngx_http_service_pool_limit_event_data_t));

      if (event_data == NULL){
        ngx_shmtx_unlock(&shpool->mutex);
        return NGX_ERROR;
      }

      event_log = ngx_slab_alloc_locked(shpool, sizeof(ngx_log_t));

      if (event_log == NULL){
        ngx_shmtx_unlock(&shpool->mutex);
        return NGX_ERROR;
      }

      stop_service_event = ngx_slab_alloc_locked(shpool, sizeof(ngx_event_t));
      if (stop_service_event == NULL){
        ngx_shmtx_unlock(&shpool->mutex);
        return NGX_ERROR;
      }

      event_data->node = node;
      event_data->shm_zone = splcf->shm_zone;

      timer_key = ngx_current_msec + 1000 * timeout;
      stop_service_event->timer.key = timer_key;
      stop_service_event->data = event_data;
      stop_service_event->log = event_log;
      stop_service_event->handler = ngx_service_pool_limit_event_expire_handler;

      spl->ref_cnt = 1;
      spl->timer_key = timer_key;
      ngx_rbtree_insert(ctx->rbtree, node);

      ngx_rbtree_insert(&ngx_event_timer_rbtree, &stop_service_event->timer);
      ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: insert key-%d pid-%d event-%d", timer_key, ngx_getpid(), stop_service_event);

      ngx_shmtx_unlock(&shpool->mutex);
      return NGX_DECLINED;
    }

    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: blocked! limit-%d, conn-%d", limit, *ctx->conn);
    ngx_shmtx_unlock(&shpool->mutex);
    return 503;
  }


  spl = (ngx_http_service_pool_limit_node_t *) &node->color;
  timer_key = ngx_current_msec + 1000 * timeout;
  timer_diff = (ngx_msec_int_t) (timer_key - spl->timer_key);
  if (ngx_abs(timer_diff) >= 500) {
    ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: search key-%d pid-%d root-%d sentinel-%d", spl->timer_key, ngx_getpid(), ngx_event_timer_rbtree.root->key, ngx_event_timer_rbtree.sentinel->key);
    timer_node = ngx_http_service_pool_limit_timer_rbtree_lookup(&ngx_event_timer_rbtree, spl->timer_key, r->connection->log);
    if(timer_node != NULL){
      ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: delete key-%d pid-%d", spl->timer_key, ngx_getpid());
      ngx_rbtree_delete(&ngx_event_timer_rbtree, timer_node);
      ev = (ngx_event_t *)((char*) timer_node - offsetof(ngx_event_t, timer));
      ngx_slab_free_locked(shpool, ev->data);
      ngx_slab_free_locked(shpool, ev->log);
      ngx_slab_free_locked(shpool, ev);
    }else{
      ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: not found key-%d set:%d pid-%d", spl->timer_key, spl->ref_cnt, ngx_getpid());
      spl->ref_cnt++;
      ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: add key-%d set:%d pid-%d", spl->timer_key, spl->ref_cnt, ngx_getpid());
    }
    event_data = ngx_slab_alloc_locked(shpool, sizeof(ngx_http_service_pool_limit_event_data_t));
    if (event_data == NULL){
      ngx_shmtx_unlock(&shpool->mutex);
      return NGX_ERROR;
    }
    event_log = ngx_slab_alloc_locked(shpool, sizeof(ngx_log_t));
    if (event_log == NULL){
      ngx_shmtx_unlock(&shpool->mutex);
      return NGX_ERROR;
    }
    stop_service_event = ngx_slab_alloc_locked(shpool, sizeof(ngx_event_t));
    if (stop_service_event == NULL){
      ngx_shmtx_unlock(&shpool->mutex);
      return NGX_ERROR;
    }


    event_data->node = node;
    event_data->shm_zone = splcf->shm_zone;

    stop_service_event->timer.key = timer_key;
    stop_service_event->data = event_data;
    stop_service_event->log = event_log;
    stop_service_event->handler = ngx_service_pool_limit_event_expire_handler;
    spl->timer_key = timer_key;
    ngx_rbtree_insert(&ngx_event_timer_rbtree, &stop_service_event->timer);

    /* ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: insert new key-%d pid-%d", timer_key, ngx_getpid()); */
  }
  ngx_shmtx_unlock(&shpool->mutex);
  /* ngx_log_error(NGX_LOG_DEBUG, r->connection->log, 0, "SERVICE POOL: renewing.... set:%d", spl->ref_cnt); */

  return NGX_DECLINED;
}




static ngx_int_t ngx_http_service_pool_limit_init_shm_zone(ngx_shm_zone_t *shm_zone, void *data){

  ngx_http_service_pool_limit_ctx_t *octx = data;
  ngx_http_service_pool_limit_ctx_t *ctx;
  ngx_slab_pool_t *shpool;
  ngx_rbtree_node_t *sentinel;

  ctx = shm_zone->data;

  if (octx != NULL) {
    ctx->rbtree = octx->rbtree;
    ctx->conn = octx->conn;
    return NGX_OK;
  }

  shpool = (ngx_slab_pool_t *) shm_zone->shm.addr;

  if (shm_zone->shm.exists){
    ctx->rbtree = shpool->data;
    return NGX_ERROR;
  }

  ctx->conn = ngx_slab_alloc(shpool, sizeof(ngx_int_t));
  if (ctx->conn == NULL){
    return NGX_ERROR;
  }

  ctx->rbtree = ngx_slab_alloc(shpool, sizeof(ngx_rbtree_t));
  if (ctx->rbtree == NULL){
    return NGX_ERROR;
  }

  shpool->data = ctx->rbtree;

  sentinel = ngx_slab_alloc(shpool, sizeof(ngx_rbtree_node_t));

  if(sentinel == NULL){
    return NGX_ERROR;
  }


  ngx_rbtree_init(ctx->rbtree, sentinel, ngx_http_service_pool_limit_rbtree_insert_value);
  *ctx->conn = 0;

  return NGX_OK;
}

static char* ngx_http_service_pool_limit(ngx_conf_t* cf, ngx_command_t* cmd, void* conf){
  char* rv = NULL;
  ngx_shm_zone_t *shm_zone;
  ngx_str_t *shm_name;
  ngx_http_service_pool_limit_ctx_t *ctx;
  ngx_http_service_pool_limit_conf_t *splcf = conf;
  rv = ngx_conf_set_num_slot(cf, cmd, conf);

  if (rv != NGX_OK){
    return rv;
  }

  ctx = ngx_pcalloc(cf->pool, sizeof(ngx_http_service_pool_limit_ctx_t));
  if (ctx == NULL) {
    return NGX_CONF_ERROR;
  }


  shm_name = ngx_palloc(cf->pool, sizeof *shm_name);
  shm_name->len = sizeof("service_pool") - 1;
  shm_name->data = (unsigned char *) "service_pool";
  shm_zone = ngx_shared_memory_add(cf, shm_name, 8 * ngx_pagesize, &ngx_http_service_pool_limit_module);

  if (shm_zone == NULL){
    return NGX_CONF_ERROR;
  }

  shm_zone->init = ngx_http_service_pool_limit_init_shm_zone;
  shm_zone->data = ctx;

  splcf->shm_zone = shm_zone;

  return NGX_CONF_OK;
}

static ngx_int_t ngx_http_service_pool_limit_init(ngx_conf_t *cf){
  ngx_http_handler_pt *h;
  ngx_http_core_main_conf_t *cmcf;

  cmcf = ngx_http_conf_get_module_main_conf(cf, ngx_http_core_module);

  h = ngx_array_push(&cmcf->phases[NGX_HTTP_ACCESS_PHASE].handlers);
  if (h == NULL) {
    return NGX_ERROR;
  }

  *h = ngx_http_service_pool_limit_handler;

  return NGX_OK;
}

static void* ngx_http_service_pool_limit_create_conf(ngx_conf_t* cf){
  ngx_http_service_pool_limit_conf_t* conf;

  conf = ngx_pcalloc(cf->pool, sizeof(ngx_http_service_pool_limit_conf_t));
  if(conf == NULL){
    return NGX_CONF_ERROR;
  }
  conf->limit = NGX_CONF_UNSET;
  conf->timeout = NGX_CONF_UNSET;
  return conf;
}

