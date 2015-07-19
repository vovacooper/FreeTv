from classes.utils import Utils

current_ip = "109.64.186.249"
web_servers = [
    {
        "hostname": "s1.impospace.com",
        "user": "root",
        "pass": "Vc@141592"
    }]

command = ["iptables -I INPUT 8 -s {0} -p tcp -j ACCEPT".format(current_ip)]

for x in web_servers:
    Utils.execute_remote_commands(x["hostname"], x["user"], x["pass"], command)
