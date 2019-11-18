#!/bin/bash

# rpcc is the client side component, also known as "all" and "all_wait" - all the same thing, just called with different arguments.
# rpcs is the server side component, should be running on every cluster node

go build rpcs.go
go build rpcc.go
