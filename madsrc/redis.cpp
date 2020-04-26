#include "redis.h"

namespace T {

    RedisError::RedisError(int code, const char message[])
        : _code(code)
    {
        static const char *ERROR_MESSAGE[] = {
            "NO_ERROR",
            "[IO]",
            "[OTHER]",
            "[EOF]",
            "[PROTOCOL]",
            "[OOM]"};

        if (_code <= 5)
        {
            _message = ERROR_MESSAGE[code];
        }
        else
        {
            _message = ERROR_MESSAGE[REDIS_ERR_OTHER];
        }
        _message += message;
    }

    RedisClient::~RedisClient()
    {
        redisFree(_connection);
    }

    void RedisClient::connect()
    {
        if (_config.port == UNIX_SOCKET_PORT)
        {
            _connection = redisConnectUnix(_config.szIP);
        }
        else
        {
            _connection = redisConnect(_config.szIP, _config.port);
        }

        if (_connection == NULL)
        {
            throw RedisError(REDIS_ERR_OTHER, "Can't allocate redis context");
        }
        if (_connection && _connection->err)
        {
            throw RedisError(_connection->err, _connection->errstr);
        }

        char buf[256];
        try
        {
            if (_config.password)
            {
                sprintf(buf, "AUTH %s", _config.password);
                command(buf);
            }

            sprintf(buf, "SELECT %d", _config.db);
            command(buf);
        }
        catch (...)
        {
            redisFree(_connection);
            throw;
        }
    }

    void RedisClient::raise_error() const
    {
        throw RedisError(_connection->err, _connection->errstr);
    }

    std::unique_ptr<RedisReply> RedisClient::parse_reply(void *p)
    {
        if (p == NULL)
        {
            RedisError error(_connection->err, _connection->errstr);
            connect(); // Reconnect
            throw error;
        }
        else
        {
            redisReply *reply = (redisReply *)p;
            return std::unique_ptr<RedisReply>(new RedisReply(reply));
        }
    }

}