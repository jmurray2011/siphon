# Running siphon in production

`siphon run` is the whole scheduler - it does not exit, and there is nothing to
put in cron. One process drives one config, so a host mirroring three APIs runs
three of them; that is deliberate, because the budget and the governor are
per-source and a shared process would let one API's backlog starve another's.

The binary is static, so the image is the binary plus a CA bundle:

```dockerfile
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY siphon /usr/local/bin/siphon
COPY configs/ /etc/siphon/
ENTRYPOINT ["/usr/local/bin/siphon"]
```

```sh
cargo build --release --target x86_64-unknown-linux-musl
docker run -d --name siphon-jira --restart unless-stopped \
  --network siphon-net --env-file ./env.jira \
  siphon:local run /etc/siphon/jira.yaml
```

Put the daemon and its Postgres on the same private network and point
`store.url` at the container name. The hop then never leaves the host, which is
what makes `sslmode=disable` defensible there and nowhere else - anything
reaching the store across a network wants a real certificate.

Credentials come from the environment, so they are whatever your host already
uses for secrets: an `--env-file`, a Docker secret, a systemd `EnvironmentFile`.
siphon resolves every one of them at startup, before the first request, so a
missing token is a container that fails immediately rather than a 401 discovered
three pages into a walk.

The mirror is worth putting on a volume and the daemons are worth
`--restart unless-stopped`. Nothing else is stateful: kill a daemon mid-walk and
the checkpoint it committed with its last page is where the next one resumes.

## Watching it

`siphon status` reports attainment over seven days, not just whether the last run
succeeded - the number an operator actually wants, and one that is only
computable because freshness is declared:

```
STREAM             FRESHNESS       LAG  ATTAIN(7d)   LAST OK  STATE
issues                   15m     4m12s       99.2%  4m12s ago  ok
issue_comments            6h     1h03m      100.0%  1h03m ago  ok
stargazers                 -         -           -    12s ago  backfilling (surplus)
releases                   1h     3h41m       71.4%  3h41m ago  BREACH: 2h41m past target

SOURCE                REMAINING     WINDOW      SPEND/S     SKEW  STATE
github              4211/5000      41m12s         1.36       2s  ok
```
