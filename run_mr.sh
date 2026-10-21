hadoop fs -rm -r "$2" 2>/dev/null || true
hadoop jar bigd-sk-projekt1.jar "$1" "$2"