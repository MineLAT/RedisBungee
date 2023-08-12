package com.imaginarycode.minecraft.redisbungee;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.gson.Gson;
import com.imaginarycode.minecraft.redisbungee.events.PubSubMessageEvent;
import com.imaginarycode.minecraft.redisbungee.util.*;
import com.imaginarycode.minecraft.redisbungee.util.uuid.NameFetcher;
import com.imaginarycode.minecraft.redisbungee.util.uuid.UUIDFetcher;
import com.imaginarycode.minecraft.redisbungee.util.uuid.UUIDTranslator;
import com.squareup.okhttp.Dispatcher;
import com.squareup.okhttp.OkHttpClient;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import net.md_5.bungee.api.ProxyServer;
import net.md_5.bungee.api.connection.ProxiedPlayer;
import net.md_5.bungee.api.plugin.Plugin;
import net.md_5.bungee.config.Configuration;
import net.md_5.bungee.config.ConfigurationProvider;
import net.md_5.bungee.config.YamlConfiguration;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.*;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * The RedisBungee plugin.
 * <p>
 * The only function of interest is {@link #getApi()}, which deprecated now,
 * Please check {@link RedisBungeeAPI#getRedisBungeeApi()},
 *
 * which exposes some functions in this class.
 * but if you want old version support,
 * then you can use old method {@link #getApi()}
 *
 */
public final class RedisBungee extends Plugin {

    // --- Static objects
    @Getter
    private static Gson gson = new Gson();
    private static final Object SERVER_TO_PLAYERS_KEY = new Object();
    // API
    private static RedisBungeeAPI api;

    // --- Jedis related objects
    @Getter
    private JedisPool pool;
    @Getter(AccessLevel.PACKAGE)
    private static RedisBungeeConfiguration configuration;
    @Getter(AccessLevel.PACKAGE)
    private static PubSubListener psl = null;

    // HTTP client
    @Getter
    private static OkHttpClient httpClient;

    // --- Instance objects
    private volatile List<String> serverIds;
    @Getter
    private UUIDTranslator uuidTranslator;
    @Getter
    private DataManager dataManager;

    private final AtomicInteger nagAboutServers = new AtomicInteger();
    private final AtomicInteger globalPlayerCount = new AtomicInteger();

    private Future<?> integrityCheck;
    private Future<?> heartbeatTask;

    private LuaManager.Script serverToPlayersScript;
    private LuaManager.Script getPlayerCountScript;

    private final Cache<Object, Multimap<String, UUID>> serverToPlayersCache = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .build();

    @Override
    public void onEnable() {
        // Replace plugin thread pool
        final ThreadFactory factory = ((ThreadPoolExecutor) getExecutorService()).getThreadFactory();
        final ScheduledExecutorService service = Executors.newScheduledThreadPool(24, factory);
        try {
            final Field field = Plugin.class.getDeclaredField("service");
            field.setAccessible(true);

            final ExecutorService builtinService = (ExecutorService) field.get(this);
            field.set(this, service);

            builtinService.shutdownNow();
        } catch (IllegalAccessException | NoSuchFieldException e) {
            getLogger().log(Level.WARNING, "Can't replace BungeeCord thread pool with our own");
            getLogger().log(Level.INFO, "skipping replacement.....");
        }

        // Load plugin config and config objects
        try {
            loadConfig();
        } catch (IOException e) {
            throw new RuntimeException("Unable to load/save config", e);
        } catch (JedisConnectionException e) {
            throw new RuntimeException("Unable to connect to your Redis server!", e);
        }

        if (pool == null) {
            // Register proxy channels
            getProxy().registerChannel("legacy:redisbungee");
            getProxy().registerChannel("RedisBungee");
            return;
        }

        // Update redis information
        try (Jedis jedis = pool.getResource()) {
            // Check redis version
            // This is more portable than INFO <section>
            final String info = jedis.info();
            for (String s : info.split("\r\n")) {
                if (s.startsWith("redis_version:")) {
                    String version = s.split(":")[1];
                    getLogger().info(version + " <- redis version");

                    if (!RedisUtil.isRedisVersionRight(version)) {
                        getLogger().warning("Your version of Redis (" + version + ") is not at least version 6.0 RedisBungee requires a newer version of Redis.");
                        throw new RuntimeException("Unsupported Redis version detected");
                    }
                    break;
                }
            }

            // Load scripts
            final LuaManager manager = new LuaManager(this);
            serverToPlayersScript = manager.createScript(IOUtil.readInputStreamAsString(getResourceAsStream("lua/server_to_players.lua")));
            getPlayerCountScript = manager.createScript(IOUtil.readInputStreamAsString(getResourceAsStream("lua/get_player_count.lua")));

            // Update heartbeats
            jedis.hset("heartbeats", configuration.getServerId(), jedis.time().get(0));

            // Check cache size
            final long uuidCacheSize = jedis.hlen("uuid-cache");
            if (uuidCacheSize > 750000) {
                getLogger().info("Looks like you have a really big UUID cache! Run https://www.spigotmc.org/resources/redisbungeecleaner.8505/ as soon as possible.");
            }
        }
        serverIds = getCurrentServerIds(true, false);

        // Create UUID translator
        uuidTranslator = new UUIDTranslator(this);

        // Create heartbeat task that update constantly the redis information
        heartbeatTask = service.scheduleAtFixedRate(() -> {
            // Update heartbeats
            try (Jedis rsc = pool.getResource()) {
                final long redisTime = getRedisTime(rsc.time());
                rsc.hset("heartbeats", configuration.getServerId(), String.valueOf(redisTime));
            } catch (JedisConnectionException e) {
                // Redis server has disappeared!
                getLogger().log(Level.SEVERE, "Unable to update heartbeat - did your Redis server go away?", e);
                return;
            }

            // Update server ids and global count
            try {
                serverIds = getCurrentServerIds(true, false);
                globalPlayerCount.set(getCurrentCount());
            } catch (Throwable e) {
                getLogger().log(Level.SEVERE, "Unable to update data - did your Redis server go away?", e);
            }
        }, 0, 3, TimeUnit.SECONDS);

        // Create data manager that update and get player data via proxy events
        dataManager = new DataManager(this);

        // Override bungeecord commands
        if (configuration.isRegisterBungeeCommands()) {
            getProxy().getPluginManager().registerCommand(this, new RedisBungeeCommands.GlistCommand(this));
            getProxy().getPluginManager().registerCommand(this, new RedisBungeeCommands.FindCommand(this));
            getProxy().getPluginManager().registerCommand(this, new RedisBungeeCommands.LastSeenCommand(this));
            getProxy().getPluginManager().registerCommand(this, new RedisBungeeCommands.IpCommand(this));
        }

        // Register own commands
        getProxy().getPluginManager().registerCommand(this, new RedisBungeeCommands.SendToAll(this));
        getProxy().getPluginManager().registerCommand(this, new RedisBungeeCommands.ServerId(this));
        getProxy().getPluginManager().registerCommand(this, new RedisBungeeCommands.ServerIds());
        getProxy().getPluginManager().registerCommand(this, new RedisBungeeCommands.PlayerProxyCommand(this));
        getProxy().getPluginManager().registerCommand(this, new RedisBungeeCommands.PlistCommand(this));
        getProxy().getPluginManager().registerCommand(this, new RedisBungeeCommands.DebugCommand(this));

        // Create API
        api = new RedisBungeeAPI(this);

        // Register event listeners
        getProxy().getPluginManager().registerListener(this, new RedisBungeeListener(this, configuration.getExemptAddresses()));
        getProxy().getPluginManager().registerListener(this, dataManager);

        // Create and register PubSubListener
        psl = new PubSubListener();
        getProxy().getScheduler().runAsync(this, psl);

        // Create and register integrity check that runs every minute to clean invalid data
        integrityCheck = service.scheduleAtFixedRate(() -> {
            try (Jedis jedis = pool.getResource()) {
                final Set<String> players = getLocalPlayersAsUuidStrings();
                final Set<String> playersInRedis = jedis.smembers("proxy:" + configuration.getServerId() + ":usersOnline");
                final List<String> lagged = getCurrentServerIds(false, true);

                // Clean up lagged players.
                for (String s : lagged) {
                    Set<String> laggedPlayers = jedis.smembers("proxy:" + s + ":usersOnline");
                    jedis.del("proxy:" + s + ":usersOnline");
                    if (!laggedPlayers.isEmpty()) {
                        getLogger().info("Cleaning up lagged proxy " + s + " (" + laggedPlayers.size() + " players)...");
                        for (String laggedPlayer : laggedPlayers) {
                            RedisUtil.cleanUpPlayer(laggedPlayer, jedis);
                        }
                    }
                }

                final Set<String> absentLocally = new HashSet<>(playersInRedis);
                absentLocally.removeAll(players);

                final Set<String> absentInRedis = new HashSet<>(players);
                absentInRedis.removeAll(playersInRedis);

                // Clean up players that are not online in the current proxy but in redis appears they are
                for (String member : absentLocally) {
                    boolean found = false;
                    for (String proxyId : getServerIds()) {
                        if (proxyId.equals(configuration.getServerId())) continue;
                        if (jedis.sismember("proxy:" + proxyId + ":usersOnline", member)) {
                            // Just clean up the set.
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        RedisUtil.cleanUpPlayer(member, jedis);
                        getLogger().warning("Player found in set that was not found locally and globally: " + member);
                    } else {
                        jedis.srem("proxy:" + configuration.getServerId() + ":usersOnline", member);
                        getLogger().warning("Player found in set that was not found locally, but is on another proxy: " + member);
                    }
                }

                Pipeline pipeline = jedis.pipelined();

                // Add online players to redis
                for (String player : absentInRedis) {
                    // Player not online according to Redis but not BungeeCord.
                    getLogger().warning("Player " + player + " is on the proxy but not in Redis.");

                    ProxiedPlayer proxiedPlayer = ProxyServer.getInstance().getPlayer(UUID.fromString(player));
                    if (proxiedPlayer == null)
                        continue; // We'll deal with it later.

                    RedisUtil.createPlayer(proxiedPlayer, pipeline, true);
                }

                pipeline.sync();
            } catch (Throwable e) {
                getLogger().log(Level.SEVERE, "Unable to fix up stored player data", e);
            }
        }, 0, 1, TimeUnit.MINUTES);

        // Register proxy channels
        getProxy().registerChannel("legacy:redisbungee");
        getProxy().registerChannel("RedisBungee");
    }

    @Override
    public void onDisable() {
        if (pool == null) {
            return;
        }

        // Poison the PubSub listener
        psl.poison();
        // Cancel tasks
        integrityCheck.cancel(true);
        heartbeatTask.cancel(true);
        // Unregister listeners
        getProxy().getPluginManager().unregisterListeners(this);

        // Remove current proxy from redis
        try (Jedis jedis = pool.getResource()) {
            jedis.hdel("heartbeats", configuration.getServerId());
            if (jedis.scard("proxy:" + configuration.getServerId() + ":usersOnline") > 0) {
                Set<String> players = jedis.smembers("proxy:" + configuration.getServerId() + ":usersOnline");
                for (String member : players)
                    RedisUtil.cleanUpPlayer(member, jedis);
            }
        }

        // Jedis pool: GOKU AAAAAAHHHHH!
        pool.destroy();
    }

    private void loadConfig() throws IOException, JedisConnectionException {
        if (!getDataFolder().exists()) {
            getDataFolder().mkdirs();
        }

        // Save default config
        final File file = new File(getDataFolder(), "config.yml");
        if (!file.exists()) {
            file.createNewFile();
            try (InputStream in = getResourceAsStream("example_config.yml");
                 OutputStream out = new FileOutputStream(file)) {
                ByteStreams.copy(in, out);
            }
        }

        // Load config
        final Configuration configuration = ConfigurationProvider.getProvider(YamlConfiguration.class).load(file);

        final String redisServer = configuration.getString("redis-server", "localhost");
        final int redisPort = configuration.getInt("redis-port", 6379);
        final boolean useSSL = configuration.getBoolean("useSSL");

        String redisPassword = configuration.getString("redis-password");
        if (redisPassword != null && (redisPassword.isEmpty() || redisPassword.equals("none"))) {
            redisPassword = null;
        }

        String serverId = configuration.getString("server-id");
        final String randomUUID = UUID.randomUUID().toString();
        // Configuration sanity checks.
        if (serverId == null || serverId.isEmpty()) {
            /*
            *  this check causes the config comments to disappear somehow
            *  I think due snake yaml limitations so as todo: write our own yaml parser?
            */
            String genId = UUID.randomUUID().toString();
            getLogger().info("Generated server id " + genId + " and saving it to config.");
            configuration.set("server-id", genId);
            ConfigurationProvider.getProvider(YamlConfiguration.class).save(configuration, new File(getDataFolder(), "config.yml"));
        } else {
            getLogger().info("Loaded server id " + serverId + '.');
        }

        if (configuration.getBoolean("use-random-id-string", false)) {
            serverId = configuration.getString("server-id") + "-" + randomUUID;
        }

        if (redisServer == null || redisServer.isBlank()) {
            throw new RuntimeException("No redis server specified!");
        }

        // Create JedisPool using configuration
        final String finalRedisPassword = redisPassword;
        FutureTask<JedisPool> task = new FutureTask<>(() -> {
            // Create the pool...
            JedisPoolConfig config = new JedisPoolConfig();
            config.setMaxTotal(configuration.getInt("max-redis-connections", 8));
            return new JedisPool(config, redisServer, redisPort, 0, finalRedisPassword, useSSL);
        });

        getProxy().getScheduler().runAsync(this, task);

        try {
            pool = task.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Unable to create Redis pool", e);
        }

        // Test the connection
        try (Jedis jedis = pool.getResource()) {
            jedis.ping();

            // If that worked, now we can check for an existing, alive Bungee:
            File crashFile = new File(getDataFolder(), "restarted_from_crash.txt");
            if (crashFile.exists()) {
                crashFile.delete();
            } else if (jedis.hexists("heartbeats", serverId)) {
                try {
                    long value = Long.parseLong(jedis.hget("heartbeats", serverId));
                    long redisTime = getRedisTime(jedis.time());
                    if (redisTime < value + 20) {
                        getLogger().severe("You have launched a possible impostor BungeeCord instance. Another instance is already running.");
                        getLogger().severe("For data consistency reasons, RedisBungee will now disable itself.");
                        getLogger().severe("If this instance is coming up from a crash, create a file in your RedisBungee plugins directory with the name 'restarted_from_crash.txt' and RedisBungee will not perform this check.");
                        throw new RuntimeException("Possible impostor instance!");
                    }
                } catch (NumberFormatException ignored) {
                }
            }

            getLogger().log(Level.INFO, "Successfully connected to Redis.");
        } catch (JedisConnectionException e) {
            pool.destroy();
            pool = null;
            throw e;
        }

        // Create HTTP client
        FutureTask<Void> task2 = new FutureTask<>(() -> {
            httpClient = new OkHttpClient();
            Dispatcher dispatcher = new Dispatcher(getExecutorService());
            httpClient.setDispatcher(dispatcher);
            NameFetcher.setHttpClient(httpClient);
            UUIDFetcher.setHttpClient(httpClient);
            RedisBungee.configuration = new RedisBungeeConfiguration(RedisBungee.this.getPool(), configuration, randomUUID);
            return null;
        });

        getProxy().getScheduler().runAsync(this, task2);

        try {
            task2.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException("Unable to create HTTP client", e);
        }
    }

    Multimap<String, UUID> serversToPlayers() {
        try {
            return serverToPlayersCache.get(SERVER_TO_PLAYERS_KEY, () -> {
                Collection<String> data = (Collection<String>) serverToPlayersScript.eval(ImmutableList.<String>of(), getServerIds());

                ImmutableMultimap.Builder<String, UUID> builder = ImmutableMultimap.builder();
                String key = null;
                for (String s : data) {
                    if (key == null) {
                        key = s;
                        continue;
                    }

                    builder.put(key, UUID.fromString(s));
                    key = null;
                }

                return builder.build();
            });
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    void sendProxyCommand(@NonNull String proxyId, @NonNull String command) {
        checkArgument(getServerIds().contains(proxyId) || proxyId.equals("allservers"), "proxyId is invalid");
        sendChannelMessage("redisbungee-" + proxyId, command);
    }

    void sendChannelMessage(String channel, String message) {
        try (Jedis jedis = pool.getResource()) {
            jedis.publish(channel, message);
        } catch (JedisConnectionException e) {
            // Redis server has disappeared!
            getLogger().log(Level.SEVERE, "Unable to get connection from pool - did your Redis server go away?", e);
            throw new RuntimeException("Unable to publish channel message", e);
        }
    }

    /**
     * Fetch the {@link RedisBungeeAPI} object created on plugin start.
     *
     * @deprecated Please use {@link RedisBungeeAPI#getRedisBungeeApi()}
     *
     * @return the {@link RedisBungeeAPI} object instance.
     */
    @Deprecated
    public static RedisBungeeAPI getApi() {
        return api;
    }

    static PubSubListener getPubSubListener() {
        return psl;
    }

    List<String> getServerIds() {
        return serverIds;
    }

    int getCount() {
        return globalPlayerCount.get();
    }

    private Set<String> getLocalPlayersAsUuidStrings() {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (ProxiedPlayer player : getProxy().getPlayers()) {
            builder.add(player.getUniqueId().toString());
        }
        return builder.build();
    }

    private long getRedisTime(List<String> timeRes) {
        return Long.parseLong(timeRes.get(0));
    }

    private List<String> getCurrentServerIds(boolean nag, boolean lagged) {
        try (Jedis jedis = pool.getResource()) {
            long time = getRedisTime(jedis.time());
            int nagTime = 0;
            if (nag) {
                nagTime = nagAboutServers.decrementAndGet();
                if (nagTime <= 0) {
                    nagAboutServers.set(10);
                }
            }
            ImmutableList.Builder<String> servers = ImmutableList.builder();
            Map<String, String> heartbeats = jedis.hgetAll("heartbeats");
            for (Map.Entry<String, String> entry : heartbeats.entrySet()) {
                try {
                    long stamp = Long.parseLong(entry.getValue());
                    if (lagged ? time >= stamp + 30 : time <= stamp + 30)
                        servers.add(entry.getKey());
                    else if (nag && nagTime <= 0) {
                        getLogger().warning(entry.getKey() + " is " + (time - stamp) + " seconds behind! (Time not synchronized or server down?) and was removed from heartbeat.");
                        jedis.hdel("heartbeats", entry.getKey());
                    }
                } catch (NumberFormatException ignored) {
                }
            }
            return servers.build();
        } catch (JedisConnectionException e) {
            getLogger().log(Level.SEVERE, "Unable to fetch server IDs", e);
            return Collections.singletonList(configuration.getServerId());
        }
    }

    int getCurrentCount() {
        Long count = (Long) getPlayerCountScript.eval(ImmutableList.<String>of(), ImmutableList.<String>of());
        return count.intValue();
    }

    Set<UUID> getPlayers() {
        ImmutableSet.Builder<UUID> setBuilder = ImmutableSet.builder();
        if (pool != null) {
            try (Jedis rsc = pool.getResource()) {
                List<String> keys = new ArrayList<>();
                for (String i : getServerIds()) {
                    keys.add("proxy:" + i + ":usersOnline");
                }
                if (!keys.isEmpty()) {
                    Set<String> users = rsc.sunion(keys.toArray(new String[keys.size()]));
                    if (users != null && !users.isEmpty()) {
                        for (String user : users) {
                            try {
                                setBuilder = setBuilder.add(UUID.fromString(user));
                            } catch (IllegalArgumentException ignored) {
                            }
                        }
                    }
                }
            } catch (JedisConnectionException e) {
                // Redis server has disappeared!
                getLogger().log(Level.SEVERE, "Unable to get connection from pool - did your Redis server go away?", e);
                throw new RuntimeException("Unable to get all players online", e);
            }
        }
        return setBuilder.build();
    }

    public Set<UUID> getPlayersOnProxy(String server) {
        checkArgument(getServerIds().contains(server), server + " is not a valid proxy ID");
        try (Jedis jedis = pool.getResource()) {
            Set<String> users = jedis.smembers("proxy:" + server + ":usersOnline");
            ImmutableSet.Builder<UUID> builder = ImmutableSet.builder();
            for (String user : users) {
                builder.add(UUID.fromString(user));
            }
            return builder.build();
        }
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    class PubSubListener implements Runnable {
        private JedisPubSubHandler jpsh = new JedisPubSubHandler();

        private Set<String> addedChannels = new HashSet<>();

        private boolean enabled = false;
        private Thread thread = null;

        @Override
        public void run() {
            if (enabled) {
                poison();
            }
            addedChannels.add("redisbungee-" + configuration.getServerId());
            addedChannels.add("redisbungee-allservers");
            addedChannels.add("redisbungee-data");
            enabled = true;
            thread = new Thread(this::alive);
            thread.start();
        }

        private void alive() {
            boolean reconnected = false;
            while (enabled && !Thread.interrupted() && pool != null && !pool.isClosed()) {
                try (Jedis jedis = pool.getResource()) {
                    if (reconnected) {
                        RedisBungee.this.getLogger().info("Redis connection is alive again");
                    }
                    // Subscribe channels and lock the thread
                    jedis.subscribe(jpsh, addedChannels.toArray(new String[0]));
                } catch (Throwable t) {
                    // Thread was unlocked due error
                    if (enabled) {
                        if (reconnected) {
                            RedisBungee.this.getLogger().warning("Redis connection dropped, automatic reconnection in 8 seconds...\n" + t.getMessage());
                        }
                        try {
                            jpsh.unsubscribe();
                        } catch (Throwable ignored) { }

                        // Make an instant subscribe if occurs any error on initialization
                        if (!reconnected) {
                            reconnected = true;
                        } else {
                            try {
                                Thread.sleep(8000);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    } else {
                        return;
                    }
                }
            }
        }

        public void addChannel(String... channels) {
            addedChannels.addAll(Arrays.asList(channels));
            try {
                jpsh.unsubscribe();
            } catch (Throwable ignored) { }
            if (thread != null) {
                thread.interrupt();
                thread.start();
            }
        }

        public void removeChannel(String... channels) {
            addedChannels.removeAll(Arrays.asList(channels));
            try {
                jpsh.unsubscribe();
            } catch (Throwable ignored) { }
            if (thread != null) {
                thread.interrupt();
                thread.start();
            }
        }

        public void poison() {
            addedChannels.remove("redisbungee-" + configuration.getServerId());
            addedChannels.remove("redisbungee-allservers");
            addedChannels.remove("redisbungee-data");
            enabled = false;
            try {
                jpsh.unsubscribe();
            } catch (Throwable ignored) { }
            if (thread != null) {
                thread.interrupt();
                thread = null;
            }
        }
    }

    private class JedisPubSubHandler extends JedisPubSub {

        @Override
        public void onMessage(final String channel, final String message) {
            if (channel == null || message.trim().length() == 0) return;
            getProxy().getScheduler().runAsync(RedisBungee.this, () -> getProxy().getPluginManager().callEvent(new PubSubMessageEvent(channel, message)));
        }

        @Override
        public void onSubscribe(String channel, int subscribedChannels) {
            RedisBungee.this.getLogger().info("RedisBungee subscribed to channel '" + channel + "'");
        }

        @Override
        public void onUnsubscribe(String channel, int subscribedChannels) {
            RedisBungee.this.getLogger().info("RedisBungee unsubscribed from channel '" + channel + "'");
        }
    }
}
