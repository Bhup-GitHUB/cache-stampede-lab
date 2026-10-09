import argparse
import concurrent.futures
import datetime
import json
import os
import pathlib
import platform
import shutil
import subprocess
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parents[1]
COMPOSE = ["docker-compose"] if shutil.which("docker-compose") else ["docker", "compose"]
BASE = "http://127.0.0.1:18080"
METRICS = ",".join(f"http://127.0.0.1:{p}/metrics" for p in (18081, 18082, 18083))


def command(args, env=None):
    return subprocess.check_output(args, cwd=ROOT, env=env, text=True).strip()


def compose(*args, env=None):
    return command(COMPOSE + list(args), env)


def redis(*args):
    return compose("exec", "-T", "redis", "redis-cli", "--raw", *args)


def get(url):
    with urllib.request.urlopen(url, timeout=6) as response:
        return response.read().decode()


def clear_data():
    return redis("EVAL", "local keys=redis.call('KEYS','data:*'); for _,k in ipairs(keys) do redis.call('DEL',k) end; return #keys", "0")


def ready():
    for attempt in range(100):
        try:
            for port in (18080, 18081, 18082, 18083):
                get(f"http://127.0.0.1:{port}/healthz")
            return
        except Exception:
            time.sleep(.2)
    raise RuntimeError("replicas did not become ready")


def setup(mode, **overrides):
    env = dict(os.environ, MODE=mode, CACHE_TTL="60s", LEASE_TTL="2s", QUERY_TIMEOUT="2s", REFRESH_TIMEOUT="3s", FOLLOWER_TIMEOUT="3s", SIMULATED_DELAY="200ms", MAX_DB_CONNS="16", MAX_CONCURRENT_DB="32")
    env.update(overrides)
    compose("up", "-d", "app1", "app2", "app3", "lb", env=env)
    compose("exec", "-T", "lb", "nginx", "-s", "reload", env=env)
    ready()
    return env


def run(args, directory, mode, scenario="hot", **overrides):
    settings = dict(SIMULATED_DELAY=args.simulated_delay, LEASE_TTL=args.lease_ttl, MAX_DB_CONNS=str(args.max_db_conns), MAX_CONCURRENT_DB=str(args.max_concurrent_db))
    env = setup(mode, **settings)
    directory.mkdir(parents=True, exist_ok=False)
    clear_data()
    keys = 2000 if scenario == "distinct" else 20 if scenario == "cold" else 1
    if scenario != "distinct":
        with concurrent.futures.ThreadPoolExecutor(max_workers=20) as pool:
            list(pool.map(lambda key: get(f"{BASE}/items/{key}"), range(1, keys + 1)))
    if overrides:
        settings.update(overrides)
        env = setup(mode, **settings)
    config = {k: env[k] for k in ("MODE", "CACHE_TTL", "LEASE_TTL", "QUERY_TIMEOUT", "REFRESH_TIMEOUT", "FOLLOWER_TIMEOUT", "SIMULATED_DELAY", "MAX_DB_CONNS", "MAX_CONCURRENT_DB")}
    (directory / "config.json").write_text(json.dumps(dict(config=config, scenario=scenario, replicas=3, offered_rps=args.rate), indent=2) + "\n")
    events = []
    start = time.time()
    with (directory / "loadgen.log").open("w") as log:
        load_command = [str(ROOT / "bin/loadgen")]
        url, metrics, output = BASE + "/items/{key}", METRICS, str(directory)
        if args.docker_loadgen:
            load_command = ["docker", "run", "--rm", "--name", "cache-stampede-lab-loadgen", "--network", "cache-stampede-lab_default", "--user", f"{os.getuid()}:{os.getgid()}", "-v", f"{directory}:/results", "--entrypoint", "/loadgen", "cache-stampede-lab:local"]
            url, metrics, output = "http://lb:8080/items/{key}", ",".join(f"http://app{i}:8080/metrics" for i in (1, 2, 3)), "/results"
        process = subprocess.Popen(load_command + ["-url", url, "-rate", str(args.rate), "-duration", f"{args.duration}s", "-keys", str(keys), "-output", output, "-metrics-urls", metrics], stdout=log, stderr=subprocess.STDOUT)
        try:
            time.sleep(args.delete_at)
            before = time.time()
            if scenario == "cold":
                redis("SET", "lease:sentinel", "preserve-me", "PX", "30000")
                deleted = clear_data()
                evidence = dict(deleted=deleted, lease_preserved=redis("GET", "lease:sentinel"))
                redis("DEL", "lease:sentinel")
            elif scenario == "redis-unavailable":
                compose("stop", "redis")
                evidence = dict(action="stop redis")
            elif scenario == "owner-killed":
                deleted = redis("DEL", "data:1")
                events.append(dict(event="cache_delete", before_unix=before, after_unix=time.time(), deleted=deleted))
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    owner_request = pool.submit(get, "http://127.0.0.1:18081/items/1")
                    deadline = time.monotonic() + 2
                    observed = ""
                    while time.monotonic() < deadline:
                        observed = compose("exec", "-T", "postgres", "psql", "-U", "stampede", "-Atc", "SELECT application_name FROM pg_stat_activity WHERE application_name IN ('app1','app2','app3') AND state='active' AND query LIKE 'SELECT id, payload%'")
                        if observed:
                            break
                        time.sleep(.01)
                    owner = observed.splitlines()[0] if observed else ""
                    if owner not in ("app1", "app2", "app3"):
                        raise RuntimeError("could not observe actual refresh owner query")
                    token = redis("GET", "lease:1")
                    before = time.time()
                    compose("kill", "-s", "SIGKILL", owner)
                    events.append(dict(event="owner_killed", before_unix=before, after_unix=time.time(), owner=owner))
                    evidence = dict(owner=owner, active_query_observed=True, lease_token_before_kill=token, action="SIGKILL owner during pg_sleep")
                    try:
                        owner_request.result(timeout=6)
                    except Exception as error:
                        evidence["direct_request_error"] = str(error)
            elif scenario == "distinct":
                evidence = dict(action="initial cold 2000 keys; no mid-run deletion")
            else:
                evidence = dict(deleted=redis("DEL", "data:1"))
            events.append(dict(event="cold_data_delete" if scenario == "cold" else "redis_stop" if scenario == "redis-unavailable" else "owner_kill_observation" if scenario == "owner-killed" else "cache_delete" if scenario != "distinct" else "observation", before_unix=before, after_unix=time.time(), **evidence))
            if process.wait(timeout=args.duration + 15) != 0:
                raise RuntimeError(f"loadgen failed: {directory}")
        finally:
            if process.poll() is None:
                if args.docker_loadgen:
                    subprocess.run(["docker", "stop", "cache-stampede-lab-loadgen"], check=False)
                process.terminate()
                process.wait()
            if scenario == "redis-unavailable":
                compose("start", "redis")
            if scenario == "owner-killed":
                compose("up", "-d", "app1", "app2", "app3", env=env)
                compose("exec", "-T", "lb", "nginx", "-s", "reload")
    summary = json.loads((directory / "summary.json").read_text())
    origin = summary.get("start_unix_seconds", start)
    for event in events:
        event["second"] = event["after_unix"] - origin
    (directory / "events.json").write_text(json.dumps(events, indent=2) + "\n")
    compose_logs = compose("logs", "--no-color", "--tail", "100", "app1", "app2", "app3")
    (directory / "services.log").write_text(compose_logs + "\n")
    chart = ["python3", "scripts/charts.py", "--input", str(directory), "--title", f"{mode}: {scenario}, simulated PostgreSQL delay {env['SIMULATED_DELAY']}"]
    if scenario != "distinct":
        chart += ["--deletion-second", str(events[0]["second"])]
    if scenario == "redis-unavailable":
        chart += ["--event-label", "Redis stopped"]
    if scenario == "owner-killed":
        chart += ["--secondary-event-second", str(events[1]["second"])]
    command(chart)
    print(directory.name, json.dumps(summary), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--duration", type=float, default=12)
    parser.add_argument("--delete-at", type=float, default=4)
    parser.add_argument("--repeats", type=int, default=2)
    parser.add_argument("--output", default="results/" + datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ"))
    parser.add_argument("--failures", action="store_true")
    parser.add_argument("--docker-loadgen", action="store_true")
    parser.add_argument("--simulated-delay", default="200ms")
    parser.add_argument("--lease-ttl", default="2s")
    parser.add_argument("--max-db-conns", type=int, default=16)
    parser.add_argument("--max-concurrent-db", type=int, default=32)
    args = parser.parse_args()
    if args.duration <= args.delete_at or args.delete_at < 0 or args.repeats < 1:
        parser.error("duration must exceed delete-at, and repeats must be positive")
    out = pathlib.Path(args.output).resolve()
    out.mkdir(parents=True, exist_ok=True)
    docker_info = json.loads(command(["docker", "info", "--format", "{{json .}} "]))
    docker_info = {key: docker_info[key] for key in ("NCPU", "MemTotal", "KernelVersion", "OperatingSystem", "Architecture", "ServerVersion", "ContainersRunning")}
    details = dict(platform=platform.platform(), cpu_count=os.cpu_count(), go=command(["go", "version"]), docker=docker_info, compose=compose("version"), args=vars(args), revision=command(["git", "rev-parse", "HEAD"]),  application_image=command(["docker", "image", "inspect", "cache-stampede-lab:local", "--format", "{{.Id}}"]), initial_container_images=compose("images", "--format", "json"))
    (out / "environment.json").write_text(json.dumps(details, indent=2) + "\n")
    (out / "compose.yaml").write_text(compose("config") + "\n")
    for mode in ("naive", "singleflight", "lease"):
        for repeat in range(1, args.repeats + 1):
            run(args, out / f"{mode}-{repeat}", mode)
    if args.failures:
        for mode in ("naive", "singleflight", "lease"):
            run(args, out / f"{mode}-distinct", mode, "distinct")
        run(args, out / "lease-cold", "lease", "cold")
        run(args, out / "lease-expiry", "lease", "hot", LEASE_TTL="75ms", SIMULATED_DELAY="200ms")
        run(args, out / "lease-owner-killed", "lease", "owner-killed", SIMULATED_DELAY="800ms")
        run(args, out / "lease-redis-unavailable", "lease", "redis-unavailable")


if __name__ == "__main__":
    main()
