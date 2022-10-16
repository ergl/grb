# UniStore

For more information, consult our paper [UniStore: A fault-tolerant marriage of causal and strong consistency (ATC ‘21).](https://www.usenix.org/system/files/atc21-bravo.pdf)

## Build

UniStore is written in Erlang. It has been tested up to OTP 24.3.3. You can download it here: https://www.erlang-solutions.com/downloads/

```
make # Downloads and caches dependencies, compiles the code
make rel # Builds an release, which can be ran using the console
```

## Test

```
make test # Unit tests
make ct # Common Tests
```

## Run

```
make rel # If you haven't done it before
make start
```

To stop the server, run `make stop`. To attach to the running server, run `make attach`.

## Try

There are two ways of poking the system: using the Erlang shell, or by running a local deployment.

### Erlang Shell

If you haven't built a release, do it first by running `make rel`. Then, run and connect to a local test server by running `make console`. Check out the API on the [grb.erl](./src/grb.erl) file to know what you can do.

To quit, you can run the following:

```erlang
q().
```

### Local Cluster

You can perform a local deployment of four replicas by running the following:

```
# Creates a release for every server.
# You should run this _even_ if you have already run `make rel`
make devrel

# Start the servers
make devstart

# Verify that the servers are running
make devping

# Join the four servers as four different replicas
make devreplicas
```

To stop the servers, run `make devstop`.

## License

Licensed under Apache License, Version 2.0.
