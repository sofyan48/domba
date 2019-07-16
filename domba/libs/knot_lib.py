from domba.libs.libcommand.parse import parser
from domba.libs import command_lib
from domba.libs import utils
from domba.libs import kafka_lib

def libknot_json(data):
    initialiaze_command = parser.initialiaze(data)
    try:
        knot_data = parser.execute_command(initialiaze_command)
    except Exception as e:
        print("ERROR READ REST: ", e)
        response={
            "result": False,
            "error": str(e),
            "status": "Command Not Execute"
        }
        return response
    else:
        response={
            "result": True,
            "description": initialiaze_command,
            "status": "Command Execute",
            "data": knot_data
        }
        return response

def begin(zone=None):
    json_data = {
        "command-begin": {
                "sendblock": {
                "cmd": "conf-begin",
                "item": "domain", 
                "section":"zone",
                "data": zone
            },
                "receive": {
                "type": "block"
            }
        }
    }
    return libknot_json(json_data)

def zone_begin(zone=None):
    json_data = {
        "zone-begin": {
                "sendblock": {
                "cmd": "zone-begin",
                "zone": zone,
                "data": zone
            },
                "receive": {
                "type": "block"
            }
        }
    }
    return libknot_json(json_data)

def commit(zone=None):
    json_data = {
        "command-commit": {
                "sendblock": {
                "cmd": "conf-commit",
                "item": "domain", 
                "section":"zone",
                "data": zone
            },
                "receive": {
                "type": "block"
            }
        }
    }
    return libknot_json(json_data)

def zone_commit(zone=None):
    json_data = {
        "zone-commit": {
                "sendblock": {
                "cmd": "zone-commit",
                "zone": zone,
                "data": zone
            },
                "receive": {
                "type": "block"
            }
        }
    }
    return libknot_json(json_data)

def parsing_data_general(data, broker):
    id_zone = None
    json_data = None
    for i in data:
        id_zone = data[i]['id_zone']
        json_data = data[i]['general']
    data = {
        "command": json_data
    }
    result_command = initialiaze_command_general(data, id_zone)
    kafka_lib.producer_send(broker, "api_general", result_command)
    print(result_command['status'])

def initialiaze_command_general(data, id_zone):
    report_command = libknot_json(data)
    return report_command

def parsing_data_cluster(data, broker, flags=None):
    zone = None
    id_zone = None
    json_data = None
    for i in data:
        zone = i
        id_zone = data[i]['id_zone']
        json_data = data[i]['cluster'][flags]
    respons = initialiaze_command_cluster(json_data, zone, id_zone, flags)
    kafka_lib.producer_send(broker, "api_cluster", respons)


def initialiaze_command_cluster(data, zone, id_zone, flags):
    if flags == "slave":
        slave_response = list()
        begin(zone)
        insert_config = command_lib.insert_config_zone(zone)
        insert_exe = libknot_json(insert_config)
        slave_response.append(insert_exe)
        file_config = command_lib.set_file(zone, id_zone)
        file_exec = libknot_json(file_config)
        slave_response.append(file_exec)
        for i in data['master']:
            master_config = command_lib.master_create_json(zone, i)
            slave_response.append(libknot_json(master_config))
        for i in data['acl']:
            acl_config = command_lib.create_json_acl(zone, i)
            slave_response.append(libknot_json(acl_config))
        module_config = command_lib.set_mods_stats(zone, "mod-stats/default")
        slave_response.append(libknot_json(module_config))
        serial_config = command_lib.set_serial_policy(zone, "dateserial")
        slave_response.append(libknot_json(serial_config))
        commit(zone)
        return slave_response
    else:
        master_response = list()
        begin(zone)
        insert_config = command_lib.insert_config_zone(zone)
        insert_exe = libknot_json(insert_config)
        master_response.append(insert_exe)
        file_config = command_lib.set_file(zone, id_zone)
        file_exec = libknot_json(file_config)
        master_response.append(file_exec)
        if data['master']:
            for i in data['master']:
                master_config = command_lib.master_create_json(zone, i)
                master_response.append(libknot_json(master_config))
        for i in data['notify']:
            notify_config = command_lib.create_json_notify(zone, i)
            master_response.append(libknot_json(notify_config))
        for i in data['acl']:
            acl_config = command_lib.create_json_acl(zone, i)
            master_response.append(libknot_json(acl_config))
        module_config = command_lib.set_mods_stats(zone, "mod-stats/default")
        master_response.append(libknot_json(module_config))
        serial_config = command_lib.set_serial_policy(zone, "dateserial")
        master_response.append(libknot_json(serial_config))
        commit(zone)
        return master_response

