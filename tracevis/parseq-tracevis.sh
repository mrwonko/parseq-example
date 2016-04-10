#!/bin/sh

set -e

echo "Usage: $0 [port]"
echo "This script starts a Parseq tracevis server for visualizing Task traces, available on the given port (8080 by default)."

if which dot >> /dev/null ; then
    java -jar `dirname $0`/parseq-tracevis-server-2.2.0-jar-with-dependencies.jar `which dot` ${1:-8080}
else
    echo "Must have dot!"
    exit 1
fi
