#include <cassert>
#include <cstdint>
#include <memory>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <hiredis/hiredis.h>

#include "misc.h"

#ifndef _MADLEE_REDIS_H_
#define _MADLEE_REDIS_H_

namespace madlee {

    enum
    {
        UNIX_SOCKET_PORT = uint32_t(-1)
    };

    struct RedisConnection
    {
        const char *szIP;
        uint32_t port;
        uint32_t db;
        const char *password;
    };


    class RedisError
        : public std::exception
    {
    public:
        explicit RedisError(int code, const char message[]);

        const char *what() const throw()
        {
            return _message.c_str();
        }

    private:
        int code() const
        {
            return _code;
        }

    private:
        int _code;
        std::string _message;
    };

    class RedisReply
    {
    public:
        explicit RedisReply(void *p)
            : _p((redisReply *)p)
        {
        }

        ~RedisReply()
        {
            freeReplyObject(_p);
        }

        std::string get_string() const
        {
            return std::string(_p->str, _p->len);
        }

        std::vector<String> get_array() const
        {
            std::vector<String> result;
            if (_p->type == REDIS_REPLY_ARRAY)
            {
                result.reserve(_p->elements);
                for (size_t i = 0; i < _p->elements; ++i)
                {
                    redisReply *sub = _p->element[i];
                    result.push_back(String(sub->str, sub->len));
                }
            }
            return result;
        }

        template <typename POD>
        size_t get_array(POD output[]) const
        {
            for (size_t i = 0; i < _p->elements; ++i)
            {
                redisReply *sub = _p->element[i];
                if (sub->type == REDIS_REPLY_INTEGER)
                {
                    assert(sizeof(POD) == sizeof(int));
                    memcpy(output + i, &(sub->integer), sizeof(POD));
                }
                else if (sub->type == REDIS_REPLY_STRING)
                {
                    memcpy(output + i, sub->str, std::min(sizeof(POD), size_t(sub->len)));
                }
                else
                {
                    memset(output + i, 0, sizeof(POD));
                }
            }
            return _p->elements;
        }

        long long get_size() const
        {
            return _p->integer;
        }

    protected:
        redisReply *_p;
    };

    typedef bool (*SubscribeHandle)(const char *, const char *, size_t, void *);

    class RedisClient
    {
    public:
        explicit RedisClient(const RedisConnection &config)
            : _config(config)
        {
            connect();
        }

        virtual ~RedisClient();

    public:
        std::unique_ptr<RedisReply> command(const char command[], ...)
        {
            va_list parms;
            va_start(parms, command);
            void *result = redisCommand(_connection, command, parms);
            va_end(parms);
            return parse_reply(result);
        }

        std::unique_ptr<RedisReply> command(const char *command, const char *value)
        {
            void *result = redisCommand(_connection, command, value);
            return parse_reply(result);
        }

        std::unique_ptr<RedisReply> command(const char *command, const char *value, size_t n)
        {
            void *result = redisCommand(_connection, command, value, n);
            return parse_reply(result);
        }

        std::unique_ptr<RedisReply> rpush(const char *key, const char *value, size_t n)
        {
            void *result = redisCommand(_connection, "RPUSH %s %b", key, value, n);
            return parse_reply(result);
        }

        std::unique_ptr<RedisReply> rpush(const char *key, const char *value)
        {
            void *result = redisCommand(_connection, "RPUSH %s %s", key, value);
            return parse_reply(result);
        }

        std::unique_ptr<RedisReply> lpush(const char *key, const char *value, size_t n)
        {
            void *result = redisCommand(_connection, "LPUSH %s %b", key, value, n);
            return parse_reply(result);
        }

        std::unique_ptr<RedisReply> lpush(const char *key, const char *value)
        {
            void *result = redisCommand(_connection, "LPUSH %s %s", key, value);
            return parse_reply(result);
        }

        std::unique_ptr<RedisReply> hset(const char *key1, const char *key2, const char *value, size_t n)
        {
            void *result = redisCommand(_connection, "HSET %s %s %b", key1, key2, value, n);
            return parse_reply(result);
        }

        std::unique_ptr<RedisReply> hset(const char *key1, const char *key2, const char *value)
        {
            void *result = redisCommand(_connection, "HSET %s %s %s", key1, key2, value);
            return parse_reply(result);
        }

        std::string hget(const char *key1, const char *key2)
        {
            void *result = redisCommand(_connection, "HGET %s %s", key1, key2);
            return parse_reply(result)->get_string();
        }

        std::unordered_map<std::string, std::string> hgetall(const char *key1)
        {
            void *result = redisCommand(_connection, "HGETALL %s", key1);
            auto reply = parse_reply(result)->get_array();
            assert(reply.size() % 2 == 0);
            std::unordered_map<std::string, std::string> map;
            for (size_t i = 0; i < reply.size(); i += 2)
            {
                map.insert(std::make_pair(reply[i], reply[i + 1]));
            }
            return map;
        }

        std::vector<std::string> hmget(const char *key1, const std::vector<std::string> &codes)
        {
            std::ostringstream buffer;
            buffer << "HMGET %s ";
            for (const auto &v : codes)
            {
                buffer << v << " ";
            }
            return command(buffer.str().c_str(), key1)->get_array();
        }

        size_t llen(const char *key)
        {
            void *result = (redisReply *)redisCommand(_connection, "LLEN %s", key);
            return parse_reply(result)->get_size();
        }

        std::vector<std::string> lrange(const char *key, int start, int end)
        {
            void *reply = (redisReply *)redisCommand(_connection, "LRANGE %s %d %d", key, start, end);
            return parse_reply(reply)->get_array();
        }

        std::string load_script(const char script[])
        {
            void *reply = (redisReply *)redisCommand(_connection, "SCRIPT LOAD %s", script);
            return parse_reply(reply)->get_string();
        }

        std::unique_ptr<RedisReply> evalsha(const char sha[], const char key[], const char data1[], size_t n, const char data2[])
        {
            void *reply = (redisReply *)redisCommand(_connection, "EVALSHA %s 1 %s %b %s", sha, key, data1, n, data2);
            return parse_reply(reply);
        }

        std::unique_ptr<RedisReply> evalsha(const char sha[], const char key[], size_t v1, size_t v2)
        {
            char vv1[64], vv2[64];
            sprintf(vv1, "%lu", v1);
            sprintf(vv2, "%lu", v2);
            void *reply = (redisReply *)redisCommand(_connection, "EVALSHA %s 1 %s %s %s", sha, key, vv1, vv2);
            return parse_reply(reply);
        }

        template <typename POD>
        size_t lrange(const char *key, int start, int end, POD output[])
        {
            void *reply = (redisReply *)redisCommand(_connection, "LRANGE %s %d %d", key, start, end);
            return parse_reply(reply)->get_array(output);
        }

        void publish(const char key[], const char message[])
        {
            void *result = redisCommand(_connection, "PUBLISH %s %s", key, message);
            parse_reply(result);
        }

        void publish(const char key[], const char message[], size_t n)
        {
            void *result = redisCommand(_connection, "PUBLISH %s %b", key, message, n);
            parse_reply(result);
        }

        void subscribe(const char key[])
        {
            void *result = redisCommand(_connection, "SUBSCRIBE %s", key);
            parse_reply(result);
        }

        void psubscribe(const char key[], SubscribeHandle handle, void *parm)
        {
            redisReply *result = (redisReply *)redisCommand(_connection, "PSUBSCRIBE %s", key);
            parse_reply(result);

            while (redisGetReply(_connection, (void **)&result) == REDIS_OK)
            {
                // consume message
                bool stop = handle(result->element[2]->str,
                                result->element[3]->str, result->element[3]->len,
                                parm);
                freeReplyObject(result);
                if (stop)
                {
                    break;
                }
            }
        }

        void raise_error() const;

    private:
        RedisConnection _config;
        redisContext *_connection;

    private:
        void connect();
        std::unique_ptr<RedisReply> parse_reply(void *);
    };

} // namespace madlee


#endif
