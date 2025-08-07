#!/bin/bash

TOTAL=100
PASSED=0
FAILED=0

echo "Running test_2a $TOTAL times..."

for ((i=1; i<=$TOTAL; i++)); do
  LOG_FILE="test_run_${i}.log"
  echo -n "Run $i/$TOTAL: "
  if make test_2a > "$LOG_FILE" 2>&1; then
    echo "PASSED"
    PASSED=$((PASSED+1))
  else
    echo "FAILED"
    FAILED=$((FAILED+1))
    echo "Failed run $i - see test_output.log for last failure"
  fi
done

echo "--------------------"
echo "RESULTS:"
echo "Total runs: $TOTAL"
echo "Passed:     $PASSED"
echo "Failed:     $FAILED"
echo "Success rate: $(( 100 * PASSED / TOTAL ))%"
