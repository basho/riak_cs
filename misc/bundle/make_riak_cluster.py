#!/usr/bin/env python

import json, sys, time, subprocess


def discover_nodes(tussle_name, pattern, required_nodes):
    network = "%s_net0" % (tussle_name)
    args = ["docker", "network", "inspect", network]
    while True:
        p = subprocess.run(args,
                           capture_output = True,
                           encoding = "utf8")
        if p.returncode != 0:
            sys.exit("Failed to discover riak nodes in %s_net0: %s\n%s" % (tussle_name, p.stdout, p.stderr))
        res = [{"ip": e["IPv4Address"].split("/")[0],
                "container": e["Name"]}
               for e in json.loads(p.stdout)[0]["Containers"].values()
               if tussle_name + "_" + pattern + "." in e["Name"]]
        if len(res) != required_nodes:
            time.sleep(1)
        else:
            print("Discovered these", pattern, "nodes:", [n["ip"] for n in res])
            return res

def start_riak_nodes(nodes):
    for n in nodes:
        print("Starting Riak at node", n["ip"])
        p = docker_exec_proc(n, ["riak", "start"])
        if p.returncode != 0:
            sys.exit("Failed to start riak node at %s: %s%s" % (n["ip"], p.stdout, p.stderr))
    for n in nodes:
        nodename = "riak@" + n["ip"]
        print("Waiting for service riak_kv on node", n["ip"])
        repeat = 10
        while repeat > 0:
            p = docker_exec_proc(n, ["riak", "admin", "wait-for-service", "riak_kv"])
            if p.stdout == "riak_kv is up\n":
                break
            else:
                time.sleep(1)
                repeat = repeat-1
        repeat = 10
        while repeat > 0:
            p = docker_exec_proc(n, ["riak", "admin", "ringready"])
            if p.returncode == 0:
                break
            else:
                time.sleep(1)
                repeat = repeat-1

def configure_riak_nodes(nodes):
    for n in nodes:
        nodename = "riak@" + n["ip"]
        print("Setting riak nodename at node", n["ip"], "to", nodename)
        p = docker_exec_proc(n, ["sed", "-i", "-E",
                                 "-e", "s|nodename = riak@127.0.0.1|nodename = %s|" % nodename,
                                 "-e", "s|listener.http.internal = .+|listener.http.internal = 0.0.0.0:8098|",
                                 "-e", "s|listener.protobuf.internal = .+|listener.protobuf.internal = 0.0.0.0:8087|",
                                 "/etc/riak/riak.conf"])
        if p.returncode != 0:
            sys.exit("Failed to set nodename on riak node at %s: %s%s" % (n["ip"], p.stdout, p.stderr))


def join_riak_nodes(nodes):
    first = nodes[0]
    rest = nodes[1:]
    print("Joining nodes %s to %s" % ([n["ip"] for n in rest], first["ip"]))
    for n in rest:
        p = docker_exec_proc(n, ["riak", "admin", "cluster", "join", "riak@" + first["ip"]])
        if p.returncode != 0:
            sys.exit("Failed to execute a join command on node %s (%s): %s%s" %
                     (n["container"], n["ip"], p.stdout, p.stderr))
        print(p.stdout)
    print("Below are the cluster changes to be committed:")
    for n in nodes:
        p = docker_exec_proc(n, ["riak", "admin", "cluster", "plan"])
        if p.returncode != 0:
            sys.exit("Failed to execute a join command on node %s (%s): %s%s" % (n["container"], n["ip"], p.stdout, p.stderr))
        print(p.stdout)
    print("Committing changes now")
    for n in rest:
        p = docker_exec_proc(n, ["riak", "admin", "cluster", "commit"])
        if p.returncode != 0:
            sys.exit("Failed to execute a join command on node %s (%s): %s%s" % (n["container"], n["ip"], p.stdout, p.stderr))
        print(p.stdout)


def configure_rcs_nodes(rcs_nodes, riak_nodes, stanchion_node):
    n = 0
    m = 0
    for rn in rcs_nodes:
        nodename = "riak_cs@" + rn["ip"]
        print("Setting riak_cs nodename at node", rn["ip"], "to", nodename)
        p = docker_exec_proc(rn, ["sed", "-i", "-E",
                                  "-e", "s|nodename = riak@127.0.0.1|nodename = %s|" % nodename,
                                  "-e", "s|riak_host = .+|riak_host = %s:8087|" % riak_nodes[m]["ip"],
                                  "-e", "s|stanchion_host = .+|stanchion_host = %s:8085|" % stanchion_node["ip"],
                                  "/opt/riak-cs/etc/riak-cs.conf"])
        if p.returncode != 0:
            sys.exit("Failed to modify riak-cs.conf node at %s: %s%s" % (rn["ip"], p.stdout, p.stderr))
        n = n + 1
        m = m + 1
        if m > len(riak_nodes):
            m = 0

def configure_stanchion_node(stanchion_node, riak_nodes):
    nodename = "stanchion@" + stanchion_node["ip"]
    print("Setting stanchion nodename at node", stanchion_node["ip"], "to", nodename)
    p = docker_exec_proc(stanchion_node, ["sed", "-i", "-E",
                                          "-e", "s|listener = 127.0.0.1:8085|listener = 0.0.0.0:8085|",
                                          "-e", "s|nodename = riak@127.0.0.1|nodename = %s|" % nodename,
                                          "-e", "s|riak_host = .+|riak_host = %s:8087|" % riak_nodes[0]["ip"],
                                          "/opt/stanchion/etc/stanchion.conf"])
    if p.returncode != 0:
        sys.exit("Failed to modify stanchion.conf node at %s: %s%s" % (stanchion_node["ip"], p.stdout, p.stderr))


def start_stanchion_node(node):
    print("Starting Stanchion at node", node["ip"])
    p = docker_exec_proc(node, ["/opt/stanchion/bin/stanchion", "start"])
    if p.returncode != 0:
        sys.exit("Failed to start Stanchion at %s: %s%s" % (node["ip"], p.stdout, p.stderr))

def start_rcs_nodes(nodes):
    for n in nodes:
        print("Starting Riak CS at node", n["ip"])
        p = docker_exec_proc(n, ["/opt/riak-cs/bin/riak-cs", "start"])
        if p.returncode != 0:
            sys.exit("Failed to start Riak CS at %s: %s%s" % (n["ip"], p.stdout, p.stderr))



def docker_exec_proc(n, cmd):
    return subprocess.run(args = ["docker", "exec", "-it", n["container"]] + cmd,
                          capture_output = True,
                          encoding = "utf8")

def main():
    tussle_name = sys.argv[1]
    required_riak_nodes = int(sys.argv[2])
    required_rcs_nodes = int(sys.argv[3])
    riak_nodes = discover_nodes(tussle_name, "riak", required_riak_nodes)
    configure_riak_nodes(riak_nodes)
    start_riak_nodes(riak_nodes)
    if len(riak_nodes) > 1:
        join_riak_nodes(riak_nodes)

    rcs_nodes = discover_nodes(tussle_name, "riak_cs", required_rcs_nodes)
    stanchion_nodes = discover_nodes(tussle_name, "stanchion", 1)
    configure_stanchion_node(stanchion_nodes[0], riak_nodes)
    configure_rcs_nodes(rcs_nodes, riak_nodes, stanchion_nodes[0])
    start_stanchion_node(stanchion_nodes[0])
    start_rcs_nodes(rcs_nodes)


if __name__ == "__main__":
    main()
