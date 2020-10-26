#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <limits.h>
#include <string.h>
#include <inttypes.h>

#include <hiredis/hiredis.h>

#define MSG_SIZE    4096

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
    const char *commands[3];
    size_t lens[3];
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

    commands[0] = "set";
    lens[0] = strlen("set");

    commands[1] = "data";
    lens[1] = strlen("data");

    commands[2] = (const char *) data;
    lens[2] = sizeof (DATA);

    rr = (redisReply *) redisCommandArgv(rc, sizeof (commands) / sizeof (commands[0]), commands, lens);
    if (rr == NULL) {
        printf("empty reply\n");
        goto EXIT;

    }
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

    printf("reply length: %d. \n", (int) rr->len);
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

    return (EXIT_SUCCESS);
}
// EOF
