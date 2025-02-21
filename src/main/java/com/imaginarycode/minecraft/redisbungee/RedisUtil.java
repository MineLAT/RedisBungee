package com.imaginarycode.minecraft.redisbungee;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import net.md_5.bungee.api.connection.PendingConnection;
import net.md_5.bungee.api.connection.ProxiedPlayer;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@VisibleForTesting
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RedisUtil {
    protected static void createPlayer(ProxiedPlayer player, Pipeline pipeline, boolean fireEvent) {
        createPlayer(player.getPendingConnection(), pipeline, fireEvent);
        if (player.getServer() != null)
            pipeline.hset("player:" + player.getUniqueId().toString(), "server", player.getServer().getInfo().getName());
    }

    protected static void createPlayer(PendingConnection connection, Pipeline pipeline, boolean fireEvent) {
        Map<String, String> playerData = new HashMap<>(4);
        playerData.put("online", "0");
        playerData.put("ip", connection.getAddress().getAddress().getHostAddress());
        playerData.put("proxy", RedisBungee.getConfiguration().getServerId());

        pipeline.sadd("proxy:" + RedisBungee.getApi().getServerId() + ":usersOnline", connection.getUniqueId().toString());
        pipeline.hmset("player:" + connection.getUniqueId().toString(), playerData);

        if (fireEvent) {
            pipeline.publish("redisbungee-data", RedisBungee.getGson().toJson(new DataManager.DataManagerMessage<>(
                    connection.getUniqueId(), DataManager.DataManagerMessage.Action.JOIN,
                    new DataManager.LoginPayload(connection.getAddress().getAddress()))));
        }
    }

    public static void cleanUpPlayer(String player, Jedis jedis) {
        jedis.srem("proxy:" + RedisBungee.getApi().getServerId() + ":usersOnline", player);
        jedis.hdel("player:" + player, "server", "ip", "proxy");
        long timestamp = System.currentTimeMillis();
        jedis.hset("player:" + player, "online", String.valueOf(timestamp));
        jedis.publish("redisbungee-data", RedisBungee.getGson().toJson(new DataManager.DataManagerMessage<>(
                UUID.fromString(player), DataManager.DataManagerMessage.Action.LEAVE,
                new DataManager.LogoutPayload(timestamp))));
    }

    public static void cleanUpPlayer(String player, Pipeline pipeline) {
        pipeline.srem("proxy:" + RedisBungee.getApi().getServerId() + ":usersOnline", player);
        pipeline.hdel("player:" + player, "server", "ip", "proxy");
        long timestamp = System.currentTimeMillis();
        pipeline.hset("player:" + player, "online", String.valueOf(timestamp));
        pipeline.publish("redisbungee-data", RedisBungee.getGson().toJson(new DataManager.DataManagerMessage<>(
                UUID.fromString(player), DataManager.DataManagerMessage.Action.LEAVE,
                new DataManager.LogoutPayload(timestamp))));
    }

    public static boolean isRedisVersionRight(String redisVersion) {
        // Need to use >=6.2 to use Lua optimizations.
        String[] args = redisVersion.split("\\.");
        if (args.length < 2) {
            return false;
        }
        int major = Integer.parseInt(args[0]);
        int minor = Integer.parseInt(args[1]);
        return major >= 6 && minor >= 0;
    }

    // Ham1255: i am keeping this if some plugin uses this *IF*
    @Deprecated
    public static boolean canUseLua(String redisVersion) {
        return isRedisVersionRight(redisVersion);
    }
}
