#!/bin/sh
set -e

case "$ROLE" in
    producer)  exec python -m demo_service.producer_main ;;
    worker)    exec python -m demo_service.worker_main ;;
    reaper)    exec python -m taskqueue.reaper ;;
    cleanup)   exec python -m taskqueue.cleanup ;;
    migrate)   exec python -m taskqueue.migrate ;;
    *)         echo "Unknown ROLE: $ROLE" && exit 1 ;;
esac
