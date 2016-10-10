package com.changtu.util.redis

import com.changtu.util.host.Configuration
import com.redis.RedisClient

/**
  * Created by lubinsu on 9/9/2016.
  * Redis通用工具类
  */
class RedisUtils(addr: String) {

  val address = Configuration("platform.properties").getString(addr)
  val host = address.split(":")(0)
  val port = address.split(":")(1).toInt
  val redisClient = new RedisClient(host, port)

  def client = redisClient

  def del(key: Any, keys: Any*) = redisClient.del(key, keys)

  def get(key: Any) = redisClient.get(key)

  def hset(key: Any, field: Any, value: Any) = redisClient.hset(key, field, value)

  def hget(key: Any, field: Any) = redisClient.hget(key, field)

  def hmset(key: Any, map: Iterable[Product2[Any, Any]]) = redisClient.hmset(key, map)
}
