#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <time.h>
#include <string.h>
#include <inttypes.h>

#include <hiredis/hiredis.h>

#define MSG_SIZE    0x1000

typedef struct TEST {
    uint32_t ui;
    int32_t i;
    uint64_t ul;
    int64_t l;
    uint8_t msg[MSG_SIZE];
} DATA;

const char alphabet[26] = {'a', 'b', 'c', 'd', 'e', 'f', 'g',
    'h', 'i', 'j', 'k', 'l', 'm', 'n',
    'o', 'p', 'q', 'r', 's', 't', 'u',
    'v', 'w', 'x', 'y', 'z'};

void set(DATA *data) {
    //const char *commands[3];
    //size_t lens[3];
    redisContext *rc = NULL;
    redisReply *rr = NULL;

    rc = redisConnect("127.0.0.1", 6379);
    if (rc == NULL || rc->err) {
        if (rc) {
            printf("Redis Error: %s. \n", rc->errstr);
            /* handle error here */
        } else {
            printf("Can't allocate Redis context! \n");
        }
        return;
    }

//    commands[0] = "set";
//    lens[0] = strlen("set");
//
//    commands[1] = "data";
//    lens[1] = strlen("data");
//
//    commands[2] = (const char *) data;
//    lens[2] = sizeof (DATA);
//
//    rr = (redisReply *) redisCommandArgv(rc, sizeof (commands) / sizeof (commands[0]), commands, lens);
//    if (rr == NULL) {
//        printf("empty reply\n");
//        goto EXIT;
//
//    }
    rr = redisCommand(rc, "SET data %b", data, (size_t) sizeof (DATA));
    printf("reply: %s\n", rr->str);

EXIT:
    if (rr) {
        freeReplyObject(rr);
        rr = NULL;
    }
    if (rc) {
        redisFree(rc);
        rc = NULL;
    }

}

void get(void) {
    int i = 0;
    DATA *data = NULL;
    redisContext *rc = NULL;
    redisReply *rr = NULL;

    rc = redisConnect("127.0.0.1", 6379);
    if (rc == NULL || rc->err) {
        if (rc) {
            printf("Redis Error: %s. \n", rc->errstr);
        } else {
            printf("Can't allocate Redis context! \n");
        }
        return;
    }

    rr = (redisReply *) redisCommand(rc, "get data");
    if (rr == NULL) {
        printf("empty reply!!!\n");
        goto EXIT;
    }

    printf("reply length: %lu. \n", rr->len);
    if (rr->len != sizeof (DATA)) {
        printf("reply length is wrong!!! \n");
        goto EXIT;
    }

    data = (DATA *) malloc(sizeof (DATA));
    if (data == NULL) {
        printf("Can't allocate...\n");
    } else {
        memcpy(data, rr->str, rr->len);
        printf("Get data: %d, %u, %" PRIu64 ", %" PRId64 ". \n", data->i, data->ui, data->ul, data->l);
        printf("Message is :");
        for (i = 0; i < MSG_SIZE; i++) {
            printf("%c", data->msg[i]);
        }
        printf("\n");
    }

    if (data) {
        free(data);
        data = NULL;
    }

EXIT:
    if (rr) {
        freeReplyObject(rr);
        rr = NULL;
    }
    if (rc) {
        redisFree(rc);
        rc = NULL;
    }

}

void zadd(DATA *data) {
    const char *commands[4];
    size_t lens[4];
    redisContext *rc = NULL;
    redisReply *rr = NULL;
    char *score = NULL;
    
    time_t now = time(NULL);
    asprintf( &score, "%lu", now);
    
    rc = redisConnect("127.0.0.1", 6379);
    if (rc == NULL || rc->err) {
        if (rc) {
            printf("Redis Error: %s. \n", rc->errstr);
            /* handle error here */
        } else {
            printf("Can't allocate Redis context! \n");
        }
        return;
    }

    commands[0] = "zadd";
    lens[0] = strlen("zadd");

    commands[1] = "msg";
    lens[1] = strlen("msg");

    commands[2] = score;
    lens[2] = strlen(score);
    
    commands[3] = (const char *) data;
    lens[3] = sizeof (DATA);

    rr = (redisReply *) redisCommandArgv(rc, sizeof (commands) / sizeof (commands[0]), commands, lens);

EXIT:
    if (score) {
        free(score);
        score = NULL;
    }
    if (rr) {
        freeReplyObject(rr);
        rr = NULL;
    }
    if (rc) {
        redisFree(rc);
        rc = NULL;
    }
}

void zget(void) {
    int i = 0;
    size_t sz = 0;
    DATA *data = NULL;
    redisContext *rc = NULL;
    redisReply *rr = NULL;

    rc = redisConnect("127.0.0.1", 6379);
    if (rc == NULL || rc->err) {
        if (rc) {
            printf("Redis Error: %s. \n", rc->errstr);
        } else {
            printf("Can't allocate Redis context! \n");
        }
        return;
    }

    rr = (redisReply *) redisCommand(rc, "zrange msg 0 -1 WITHSCORES");
    if (rr == NULL) {
        printf("empty reply!!!\n");
        goto EXIT;
    }

    printf("reply elements: %lu. \n", rr->elements);
    
    data = (DATA *) malloc(sizeof (DATA));
    if (data == NULL) {
        printf("Can't allocate...\n");
    } else {
        for (sz = 0; sz < rr->elements; sz++) {
            if (rr->element[sz]->len != sizeof (DATA)) {
                printf("score is: %s. \n", rr->element[sz]->str);
                continue;
            }
            memcpy(data, rr->element[sz]->str, rr->element[sz]->len);
            printf("Get data: %d, %u, %" PRIu64 ", %" PRId64 ". \n", data->i, data->ui, data->ul, data->l);
            printf("Message is :");
            for (i = 0; i < MSG_SIZE; i++) {
                printf("%c", data->msg[i]);
            }
            printf("\n");
        }
    }
    
EXIT:
    if (data) {
        free(data);
        data = NULL;
    }
    if (rr) {
        freeReplyObject(rr);
        rr = NULL;
    }
    if (rc) {
        redisFree(rc);
        rc = NULL;
    }
}

int main(int argc, char** argv) {
    int i = 0;
    DATA data = {0};
    data.i = INT_MAX;
    data.l = LONG_MAX;
    data.ui = UINT_MAX;
    data.ul = ULONG_MAX;
    for (i = 0; i < MSG_SIZE; i++) {
        data.msg[i] = alphabet[rand() % 26];
    }

    set(&data);
    get();
    
    zadd(&data);
    sleep(1);
    
    for (i = 0; i < MSG_SIZE; i++) {
        data.msg[i] = alphabet[rand() % 26];
    }
    zadd(&data);
    sleep(1);
    
    for (i = 0; i < MSG_SIZE; i++) {
        data.msg[i] = alphabet[rand() % 26];
    }
    zadd(&data);
    zget();

    return (EXIT_SUCCESS);
}
// EOF
