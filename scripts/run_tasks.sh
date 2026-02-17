#!/bin/bash

# Get paths relative to the script location
SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
PROJECT_ROOT=$(dirname "$SCRIPT_DIR")

if [ -z "$1" ]; then
    echo "Error: Specify task 1, 2, or 3"
    exit 1
fi

TASK=$1
JAR="$PROJECT_ROOT/bin/Zad$TASK.jar"
INPUT="$PROJECT_ROOT/data/input$TASK"
OUTPUT="$PROJECT_ROOT/output$TASK"

case $TASK in
  1)
    CLASS="AfterLine"
    EXTRA_ARGS=""
    ;;
  2)
    CLASS="InvertedIndexPos"
    EXTRA_ARGS=""
    ;;
  3)
    CLASS="SimilarTweets"
    # If a second argument is provided, use it as the target tweet T
    if [ -z "$2" ]; then
        echo "Error: Task 3 requires a target tweet sentence."
        echo "Usage: ./run_tasks.sh 3 \"your target sentence here\""
        exit 1
    fi
    EXTRA_ARGS="$2"
    ;;
  *)
    echo "Unknown task: $TASK"
    exit 1
    ;;
esac

if [ ! -f "$JAR" ]; then
    echo "Error: $JAR not found in bin/ folder!"
    exit 1
fi

# Clean old output
rm -rf "$OUTPUT"

echo "Executing Task $TASK ($CLASS)..."
hadoop jar "$JAR" "$CLASS" "$INPUT" "$OUTPUT" "$EXTRA_ARGS"

if [ $? -eq 0 ]; then
    echo "Success. Results in $OUTPUT"
    echo "Top 5 lines of output:"
    head -n 5 "$OUTPUT/part-r-00000"
else
    echo "Job failed."
fi