def is_running_in_docker():
    try:
        with open('/proc/1/cgroup', 'r') as f:
            cgroup = f.read()
        return 'docker' in cgroup
    except FileNotFoundError:
        return False