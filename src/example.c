#define _DEFAULT_SOURCE

#include <stddef.h>
#include <syslog.h>
#include <string.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <stdint.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>

#define EXAMPLE_SENTINEL 8090
#define EXAMPLE_AGG_F_SENTINEL 8091

// just want to have an upper bound on the sqlite3_step for loop to avoid
// infinite loops; set this to a value which is absolutely going to be higher
// then any realistic max number of results on a select query
#define EXAMPLE_MAX_QUERY_LOOP_STEPS 1048576


struct example_s {
    int sentinel;
    sqlite3 * db;
};

struct example_agg_f_s {
    int sentinel;
    int aggregate;
};



// Basic database schema for the persistent database
static const char example_schema_full[] =
    "begin;"

    "create table devices ("
        "deviceid text not null check (length(deviceid)==12),"
        "primary key (deviceid)"
    ") without rowid;"

    // not adding the length constraint here since it's implied by the foreign key
    "create table outputs ("
        "deviceid text not null,"
        "outputid int not null check (0 <= outputid),"
        "foreign key (deviceid) references devices(deviceid),"
        "primary key (deviceid, outputid)"
    ") without rowid;"
    
    "create table groups ("
        "deviceid text not null,"
        "outputid int not null,"
        "groupid int not null,"
        "foreign key (deviceid, outputid) references outputs(deviceid, outputid),"
        "primary key (deviceid, outputid, groupid)"
    ") without rowid;"

    "pragma user_version = 1;"

    "commit;";


static const char example_memory_schema[] =
    "begin;"

    // drawback: it's not possible to add cross-database foreign keys - so it's
    // not possible to add a foreign key to the persistent database.
    "create table state.measured ("
        "deviceid text not null check (length(deviceid)==12),"
        "outputid int not null check (0 <= outputid),"
        "timestamp int not null default (now_monotonic()),"
        "state bool not null,"
        "level int,"
        "primary key (deviceid, outputid)"
    ") without rowid;"

    "create table state.setpoint ("
        "deviceid text not null check (length(deviceid)==12),"
        "outputid int not null check (0 <= outputid),"
        "setstate bool not null,"
        "setlevel int,"
        "primary key (deviceid, outputid)"
    ") without rowid;"

    "commit;";




// custom aggregate function example
void example_agg_f_step (
    sqlite3_context * ctx,
    int argc,
    sqlite3_value ** argv
)
{
    int ret = 0;

    if (argc != 3) {
        syslog(LOG_ERR, "%s:%d:%s: example_agg_f_step takes 3 arguments, but given %d",
                __FILE__, __LINE__, __func__, argc);
        sqlite3_result_error(ctx, "wrong number of arguments", strlen("wrong number of arguments"));
        return;
    }

    // fetch the values from the database
    const int nodeid = sqlite3_value_int(argv[0]);
    const int outputid = sqlite3_value_int(argv[1]);
    const int groupid = sqlite3_value_int(argv[2]);


    // fetch the result struct
    struct example_agg_f_s * agg_f = sqlite3_aggregate_context(ctx, sizeof(struct example_agg_f_s));

    // if the sentinel isn't set, then we called it the first time; use this to
    // set initial values on the aggregate.
    if (0 == agg_f->sentinel) {
        agg_f->aggregate = 80;
        agg_f->sentinel = EXAMPLE_AGG_F_SENTINEL;
    }

    // if sentinel is other than EXAMPLE_AGG_F_SENTINEL, then something bad happened.
    if (EXAMPLE_AGG_F_SENTINEL != agg_f->sentinel) {
        syslog(LOG_ERR, "%s:%d:%s: aggregate structure sentinel is wrong! memory corrupt?", __FILE__, __LINE__, __func__);
        sqlite3_result_error(ctx, "sentinel value is wrong", strlen("sentinel value is wrong"));
        return;
    }


    // aggregate logic
    agg_f->aggregate += groupid;
    return;

}


void example_agg_f_final (
    sqlite3_context * ctx
)
{
    struct example_agg_f_s * agg = sqlite3_aggregate_context(ctx, sizeof(struct example_agg_f_s));
    sqlite3_result_blob(ctx, agg, sizeof(struct example_agg_f_s), SQLITE_TRANSIENT);
    return;
}


// custom ordinary function
void example_now_monotonic (
    sqlite3_context * ctx,
    int argc,
    sqlite3_value ** argv
)
{

    int ret = 0;

    struct timespec tp = {0};

    ret = clock_gettime(CLOCK_MONOTONIC, &tp);
    if (-1 == ret) {
        syslog(LOG_ERR, "%s:%d:%s: clock_gettime: %s", __FILE__, __LINE__, __func__, strerror(errno));
        sqlite3_result_error(ctx, "clock_gettime returned -1", strlen("clock_gettime returned -1"));
        return;
    }

    // This value contains 16 bits of second precision (rolls over every 65535
    // seconds), and 16 bits of sub-second precision (more then millisecond
    // precision, less then microsecond).
    uint64_t time = (tp.tv_sec & 0xffffffff);
    time <<= 32;
    time |= (tp.tv_nsec & 0xffffffff);

    sqlite3_result_int64(
        /* context = */ ctx,
        /* int = */ time
    );

    return;
    (void)argc;
    (void)argv;
}




int example_init_schema_migration_full (
    struct example_s * example
)
{

    int ret = 0;
    char * err = NULL;

    ret = sqlite3_exec(
        /* db = */ example->db,
        /* sql = */ example_schema_full,
        /* cb = */ NULL,
        /* user_data = */ NULL,
        /* err = */ &err
    );
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_exec returned %d: %s",
            __FILE__, __LINE__, __func__, ret, err);
        return -1;
    }

    return 0;
}


int example_init_schema_migration (
    struct example_s * example
)
{

    int ret = 0;
    sqlite3_stmt * stmt = NULL;

    // time to do schema migrations; find out which version we're running right now.
    ret = sqlite3_prepare_v3(
        /* db = */ example->db,
        /* sql = */ "pragma user_version;",
        /* sql_len = */ strlen("pragma user_version;"),
        /* flags = */ SQLITE_PREPARE_NORMALIZE,
        /* &stmt = */ &stmt,
        /* &sql_end = */ NULL
    );
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_prepare_v2 returned %d: %s",
            __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }


    ret = sqlite3_step(stmt);
    if (SQLITE_ROW != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_step returned %d: %s",
                __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }


    const int sqlite3_user_version = sqlite3_column_int(stmt, 0);


    // Ok, we have the version, let's destroy the sqlite3 stmt and work on the
    // schema resolution
    ret = sqlite3_finalize(stmt);
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_finalize returned %d: %s",
            __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }


    // here you can add a switch-statement or something and patch the schema up
    // to the most recent version.


    // full database schema
    if (0 == sqlite3_user_version) {
        syslog(LOG_INFO, "%s:%d:%s: doing full schema migration", __FILE__, __LINE__, __func__);

        ret = example_init_schema_migration_full(example);
        if (-1 == ret) {
            syslog(LOG_ERR, "%s:%d:%s: example_init_schema_migration_full returned -1", __FILE__, __LINE__, __func__);
            return -1;
        }

        return 0;
    }

    // current schema version
    if (1 == sqlite3_user_version) {
        return 0;
    }


    // if we reach this point, the sqlite3 schema version is too new for us to
    // handle; don't touch it!
    syslog(LOG_ERR, "%s:%d:%s: sqlite3 schema version is too new; giving up", __FILE__, __LINE__, __func__);
    return -1;
}


int example_init_schema_memory (
    struct example_s * example
)
{

    int ret = 0;
    char * err = NULL;


    // attach in-memory database
    ret = sqlite3_exec(
        /* db = */ example->db,
        /* sql = */ "attach ':memory:' as state;",
        /* cb = */ NULL,
        /* user_data = */ NULL,
        /* err = */ &err
    );
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_exec returned %d: %s",
            __FILE__, __LINE__, __func__, ret, err);
        return -1;
    }


    // create schema
    err = NULL;
    ret = sqlite3_exec(
        /* db = */ example->db,
        /* sql = */ example_memory_schema,
        /* cb = */ NULL,
        /* user_data = */ NULL,
        /* err = */ &err
    );
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_exec returned %d: %s",
            __FILE__, __LINE__, __func__, ret, err);
        return -1;
    }
    

    return 0;
}


int example_init_custom_agg_function (
    struct example_s * example
)
{

    int ret = 0;

    ret = sqlite3_create_function_v2(
        /* db = */ example->db,
        /* function_name = */ "example_agg_f",
        /* num_args = */ 3,
        /* flags = */ SQLITE_UTF8 | SQLITE_DETERMINISTIC,
        /* user_data = */ example,
        /* func = */ NULL,
        /* step = */ example_agg_f_step,
        /* final = */ example_agg_f_final,
        /* destroy = */ NULL
    );
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_create_function_v2 returned %d: %s"
                , __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }

    return 0;
}


int example_init_custom_now_monotonic_function (
    struct example_s * example
)
{

    int ret = 0;

    ret = sqlite3_create_function_v2(
        /* db = */ example->db,
        /* function_name = */ "now_monotonic",
        /* num_args = */ 0,
        /* flags = */ SQLITE_UTF8 | SQLITE_DETERMINISTIC,
        /* user_data = */ example,
        /* func = */ example_now_monotonic,
        /* step = */ NULL,
        /* final = */ NULL,
        /* destroy = */ NULL
    );
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_create_function_v2 returned %d: %s"
                , __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }

    return 0;
}


int example_init (
    struct example_s * example
)
{

    int ret = 0;
    char * err = NULL;

    ret = sqlite3_initialize();
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_initialize returned %d", __FILE__, __LINE__, __func__, ret);
        return -1;
    }


    // open database, create it if it doesn't exist
    // useful flags: SQLITE_OPEN_READONLY, SQLITE_OPEN_READWRITE, SQLITE_OPEN_CREATE, SQLITE_OPEN_MEMORY
    ret = sqlite3_open_v2(
        /* path = */ "db.sqlite",
        /* db = */ &example->db,
        /* flags = */ SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_MEMORY,
        /* vfs = */ NULL
    );
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_open_v2 returned %d: %s",
            __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }


    // enable foreign keys
    ret = sqlite3_exec(
        /* db = */ example->db,
        /* sql = */ "pragma foreign_keys=1;",
        /* cb = */ NULL,
        /* user_data = */ NULL,
        /* err = */ &err
    );
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_exec returned %d: %s",
            __FILE__, __LINE__, __func__, ret, err);
        return -1;
    }


    // migrate schema to current schema version
    ret = example_init_schema_migration(example);
    if (-1 == ret) {
        syslog(LOG_ERR, "%s:%d:%s: example_init_schema_migration returned -1", __FILE__, __LINE__, __func__);
        return -1;
    }


    // attach in-memory database on top and set up schemas
    ret = example_init_schema_memory(example);
    if (-1 == ret) {
        syslog(LOG_ERR, "%s:%d:%s: example_init_schema_memory returned -1", __FILE__, __LINE__, __func__);
        return -1;
    }



    // create custom aggregate function
    ret = example_init_custom_agg_function(example);
    if (-1 == ret) {
        syslog(LOG_ERR, "%s:%d:%s: example_init_custom_agg_function returned -1", __FILE__, __LINE__, __func__);
        return -1;
    }


    return 0;
}



int example_device_new (
    struct example_s * example,
    const char * const deviceid,
    const uint32_t deviceid_len
)
{

    int ret = 0;
    sqlite3_stmt * stmt = NULL;


    // prepare statement
    ret = sqlite3_prepare_v3(
        /* db = */ example->db,
        /* sql = */ "insert into devices(deviceid) values (?);",
        /* sql_len = */ strlen("insert into devices(deviceid) values (?);"),
        /* flags = */ SQLITE_PREPARE_NORMALIZE,
        /* &stmt = */ &stmt,
        /* &sql_end = */ NULL
    );
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_prepare_v2 returned %d: %s",
            __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }


    // bind deviceid
    ret = sqlite3_bind_text(
        /* stmt = */ stmt,
        /* index = */ 1,
        /* text = */ deviceid,
        /* text_len = */ deviceid_len,
        /* destructor = */ SQLITE_STATIC
    );
    if (-1 == ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_bind_text returned %d: %s",
            __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;

    }


    // execute query
    ret = sqlite3_step(stmt);
    if (SQLITE_DONE != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_step returned %d: %s",
                __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }


    // cleanup
    ret = sqlite3_finalize(stmt);
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_finalize returned %d: %s",
            __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }


    return 0;
}


int example_custom_aggregate_query (
    struct example_s * example
)
{

    int ret = 0;
    sqlite3_stmt * stmt;
    
    ret = sqlite3_prepare_v3(
        /* db = */ example->db,
        /* sql = */ "select example_agg_f(deviceid, outputid, groupid) from groups group by groups.groupid",
        /* sql_len = */ strlen("select example_agg_f(deviceid, outputid, groupid) from groups group by groups.groupid"),
        /* flags = */ SQLITE_PREPARE_NORMALIZE,
        /* &stmt = */ &stmt,
        /* &sql_end = */ NULL
    );
    if (SQLITE_OK != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_prepare_v3 returned %d: %s",
                __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }


    for (int i = 0; i < EXAMPLE_MAX_QUERY_LOOP_STEPS; i++) {
        ret = sqlite3_step(stmt);
        if (SQLITE_DONE == ret) {
            break;
        }
        if (SQLITE_ROW != ret) {
            syslog(LOG_ERR, "%s:%d:%s: sqlite3_step returned %d: %s",
                    __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
            return -1;
        }

        const struct example_agg_f_s * res = sqlite3_column_blob(stmt, 0);
        syslog(LOG_INFO, "%s:%d:%s: res->aggregate=%d", __FILE__, __LINE__, __func__, res->aggregate);
    }
    if (SQLITE_DONE != ret) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_step returned %d: %s",
                __FILE__, __LINE__, __func__, ret, sqlite3_errmsg(example->db));
        return -1;
    }

    return 0;
}


int example_serialize (
    struct example_s * example
)
{

    int ret = 0;

    uint8_t * db;
    sqlite3_int64 db_len = 0;

    db = sqlite3_serialize(
        /* db = */ example->db,
        /* schema = */ "main",
        /* db_len = */ &db_len,
        /* flags = */ 0
    );
    if (-1 == db_len) {
        syslog(LOG_ERR, "%s:%d:%s: sqlite3_serialize returned -1", __FILE__, __LINE__, __func__);
        return -1;
    }

    printf("db_len=%lld\n", db_len);

    // compress (db, db_len) using something

    // send (db, db_len) somewhere

    return 0;
}


int main (
    int argc,
    char const* argv[]
)
{
    int ret = 0;
    struct example_s example = {0};

    openlog("example", LOG_CONS | LOG_PID, LOG_USER);

    ret = example_init(&example);
    if (-1 == ret) {
        syslog(LOG_ERR, "%s:%d:%s: example_init returned -1", __FILE__, __LINE__, __func__);
        return -1;
    }

    ret = example_device_new(
        /* app struct = */ &example,
        /* deviceid = */ "0123456789012",
        /* deviceid_len = */ 12
    );
    if (-1 == ret) {
        syslog(LOG_ERR, "%s:%d:%s: example_device_new returned -1", __FILE__, __LINE__, __func__);
        return -1;
    }

    ret = example_custom_aggregate_query(&example);
    if (-1 == ret) {
        syslog(LOG_ERR, "%s:%d:%s: example_custom_aggregate_query returned -1", __FILE__, __LINE__, __func__);
        return -1;
    }


    ret = example_serialize(&example);
    if (-1 == ret) {
        syslog(LOG_ERR, "%s:%d:%s: example_serialize returned -1", __FILE__, __LINE__, __func__);
        return -1;
    }

    syslog(LOG_INFO, "%s:%d:%s: ok", __FILE__, __LINE__, __func__);

    return 0;
    (void)argc;
    (void)argv;
}
