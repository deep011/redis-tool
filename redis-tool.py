import getopt
import sys

separator = " "

host="127.0.0.1"
port=6379
password=""

max_time_complexity=10

output_file_name = ""
output_file = None

supported_commands=["export-keys"]
command=supported_commands[0]
supported_commands_str = ""
for supported_command in supported_commands:
    supported_commands_str += supported_command + " "

output_type_screen = 0  #Out put the status to the screen.
output_type_file = 1  #Out put the status to the files.
output_type = output_type_screen #default output is the screen.
def output(content):
    if output_type == output_type_screen:
        print repr(content)
    elif output_type == output_type_file:
        output_file.write(repr(content) + '\n')
    else:
        return

    return

def redis_connection_create():
    import redis.connection
    redis_conn = redis.StrictRedis(
        host=host,
        port=port,
        password=password)

    return redis_conn

def redis_connection_destroy(conn):
    conn.connection_pool.disconnect()
    return

# "value_1 value_2 ... value_n"
def convert_list_value_to_string(list_value):
    result = ""
    for value in list_value:
        result += value + separator

    return result

# "field_1 svalue_1 field_2 value_2 ... field_n value_n"
def convert_hash_value_to_string(hash_value):
    result = ""
    if hash_value == None:
        return result

    for field,value in hash_value.items():
        result += field + separator + value + separator

    return result

# "member_1 member_2 ... member_n"
def convert_set_value_to_string(set_value):
    result = ""
    for member in set_value:
        result += member + separator

    return result

# "member_1 score_1 member_2 score_2 ... member_n score_n"
def convert_zset_value_to_string(zset_value):
    result = ""
    for member,score in zset_value:
        result += member + separator + str(score) + separator

    return result

def fetch_string_value_safely(redis_conn, key):
    return redis_conn.get(key)

def fetch_hash_value_repeatedly(redis_conn, key, value_convert_to_string=False):
    hash = {}
    hash_string = ""
    for field,value in redis_conn.hscan_iter(key):
        if value_convert_to_string == True:
            hash_string += field + separator + str(value) + separator
        else:
            hash[field] = value

    if value_convert_to_string == True:
        return hash_string
    else:
        return hash

def fetch_hash_value_safely(redis_conn, key, value_convert_to_string=False):
    hash_len = redis_conn.hlen(key)
    if hash_len <= max_time_complexity / 2 + 1:
        hash = redis_conn.hgetall(key)
        if value_convert_to_string == True:
            return convert_hash_value_to_string(hash)
        else:
            return hash
    else:
        return fetch_hash_value_repeatedly(redis_conn, key, value_convert_to_string)

def fetch_list_value_repeatedly(redis_conn, key, list_len, interval, value_convert_to_string=False):
    begin = 0
    end = list_len
    list = []
    list_string = ""
    while begin < end:
        result = redis_conn.lrange(key, begin, begin+interval)
        begin += interval + 1
        if value_convert_to_string == True:
            list_string += convert_list_value_to_string(result)
        else:
            list.extend(result)

    if value_convert_to_string == True:
        return list_string
    else:
        return list

def fetch_list_value_safely(redis_conn, key, value_convert_to_string=False):
    list_len = redis_conn.llen(key)
    if list_len <= max_time_complexity:
        list = redis_conn.lrange(key, 0, -1)
        if value_convert_to_string == True:
            return convert_list_value_to_string(list)
        else:
            return list
    else:
        return fetch_list_value_repeatedly(redis_conn, key, list_len, max_time_complexity, value_convert_to_string)

def fetch_set_value_repeatedly(redis_conn, key, value_convert_to_string=False):
    set = []
    set_string = ""
    for mem in redis_conn.sscan_iter(key):
        if value_convert_to_string == True:
            set_string += mem + separator
        else:
            set.append(mem)

    if value_convert_to_string == True:
        return set_string
    else:
        return set

def fetch_set_value_safely(redis_conn, key, value_convert_to_string=False):
    set_len = redis_conn.scard(key)
    if set_len <= max_time_complexity:
        set = redis_conn.smembers(key)
        if value_convert_to_string == True:
            return convert_set_value_to_string(set)
        else:
            return set
    else:
        return fetch_set_value_repeatedly(redis_conn, key, value_convert_to_string)

def fetch_zset_value_repeatedly(redis_conn, key, value_convert_to_string=False):
    zset = []
    zset_string = ""
    for mem,score in redis_conn.zscan_iter(key):
        if value_convert_to_string == True:
            zset_string += mem + separator + str(score) + separator
        else:
            zset.append((mem,str(score)))

    if value_convert_to_string == True:
        return zset_string
    else:
        return zset

def fetch_zset_value_safely(redis_conn, key, value_convert_to_string=False):
    zset_len = redis_conn.zcard(key)
    if zset_len <= max_time_complexity/2 + 1:
        zset = redis_conn.zrange(key, 0, -1, withscores=True)
        if value_convert_to_string == True:
            return convert_zset_value_to_string(zset)
        else:
            return zset
    else:
        return fetch_zset_value_repeatedly(redis_conn, key, value_convert_to_string)

def fetch_value_safely(redis_conn, key, value_type, value_convert_to_string=False):
    if value_type == "string":
        return fetch_string_value_safely(redis_conn, key)
    elif value_type == "hash":
        return fetch_hash_value_safely(redis_conn, key, value_convert_to_string)
    elif value_type == "set":
        return fetch_set_value_safely(redis_conn, key, value_convert_to_string)
    elif value_type == "zset":
        return fetch_zset_value_safely(redis_conn, key, value_convert_to_string)
    elif value_type == "list":
        return fetch_list_value_safely(redis_conn, key, value_convert_to_string)

    return None

def fetch_value_with_type(redis_conn, key, value_convert_to_string=False):
    value_type = redis_conn.type(key)
    return [value_type, fetch_value_safely(redis_conn, key, value_type,value_convert_to_string)]

def export_redis_keys(with_value=False, value_convert_to_string=False):


    conn = redis_connection_create()

    for key in conn.scan_iter():
        type_value = fetch_value_with_type(conn, key, value_convert_to_string)
        output(type_value[0] + separator + key + separator + str(type_value[1]))

    redis_connection_destroy(conn)

def usage():
    print 'python redis-tool.py [options]'
    print ''
    print 'options:'
    print '-h,--help: show this help message'
    print '-v,--version: show the version'
    print '-H: target host'
    print '-P: target port'
    print '-p: target user password'
    print '-o: output the status to this file'
    print '-e: output error message to this file'
    print '-i: interval'
    print '-C: the command to be executed, default is \'' + supported_commands[0] + "\'"
    print '--socket: the socket file to use for connection'
    print '\r\n'
    print 'Supported commands: ' + supported_commands_str
    print ''

def version():
    return '0.1.0'

def print_version():
    print "redis-tool version " + version()
    return

if __name__ == "__main__":
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'hvH:P:p:o:e:i:C:', ['help', 'version', 'socket='])
    except getopt.GetoptError, err:
        print str(err)
        usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-h', '--help'):
            usage()
            sys.exit(1)
        elif opt in ('-v', '--version'):
            print_version()
            sys.exit(1)
        elif opt in ('-H'):
            host = arg
        elif opt in ('-P'):
            port = int(arg)
        elif opt in ('-p'):
            password = arg
        elif opt in ('-o'):
            output_type = output_type_file
            output_file_name = arg
        elif opt in ('-e'):
            errlog_file_name = arg
        elif opt in ('-i'):
            interval = int(arg)
        elif opt in ('-C'):
            command = arg
            find = 0
            for supported_command in supported_commands:
                if supported_command == command:
                    find = 1

            if find == 0:
                print 'ERROR! Not suppoted command: ' + command
                print 'Supported commands: ' + supported_commands_str
                sys.exit(3)
        elif opt in ('--socket'):
            socket_file = arg
        else:
            print 'Unhandled option'
            sys.exit(3)

    if output_type == output_type_file:
        output_file = open(output_file_name, 'wb', 0)

    export_redis_keys(with_value=True, value_convert_to_string=True)