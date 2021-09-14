#!/usr/bin/env python

import json, sys, subprocess


def discover_riak_nodes(tussle_name):
    network = "%s_net0" % (tussle_name)
    args = ["docker", "network", "inspect", network]
    p = subprocess.run(args,
                       capture_output = True,
                       encoding = "utf8")
    if p.returncode != 0:
        sys.exit("Failed to discover riak nodes in %s_net0: %s\n%s" % (tussle_name, p.stdout, p.stderr))
    res = [{"ip": e["IPv4Address"].split("/")[0],
            "container": e["Name"]}
           for e in json.loads(p.stdout)[0]["Containers"].values()
           if tussle_name + "_riak." in e["Name"]]
    return res

def join_nodes(nodes):
    first = nodes[0]
    rest = nodes[1:]
    print("Joining nodes %s to %s" % ([n["ip"] for n in rest], first["ip"]))
    for n in rest:
        p = subprocess.run(args = ["docker", "exec", "-it", first["container"],
                                   "riak", "admin", "cluster", "join", "riak@" + first["ip"]],
                           capture_output = True,
                           encoding = "utf8")
        if p.returncode != 0:
            sys.exit("Failed to execute a join command on node %s (%s): %s%s" % (n["container"], n["ip"], p.stdout, p.stderr))
        print(p.stdout)

def main():
    riak_nodes = discover_riak_nodes(sys.argv[1])
    join_nodes(riak_nodes)


if __name__ == "__main__":
    main()
