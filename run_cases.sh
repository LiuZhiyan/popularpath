#!/usr/bin/env bash

set -o errexit -o nounset -o pipefail
SCRIPTPATH="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )";

if [ ! -d "$SCRIPTPATH/target" ]; then
  echo "No target directory found, use 'mvn clean compile assembly:assembly' build project first."
  exit 1
fi

PROGRAM="$SCRIPTPATH/target/lzy-popular-path-jar-with-dependencies.jar"

if [ ! -f "$PROGRAM" ]; then
  echo "No target program found, use 'mvn clean compile assembly:assembly' build project first."
  exit 1
fi

echo ">>>>>>>>>> CASE 1 <<<<<<<<<<"
echo "Normal case"
ACCESS_LOG_FILE=${SCRIPTPATH}/target/example_access_log.txt
echo ">>>>> Access log ($ACCESS_LOG_FILE):"
cat ${ACCESS_LOG_FILE}
echo
echo ">>>>> Program output:"
AOE_KIND="yes"
time java -jar ${PROGRAM} 1 1 ${AOE_KIND} ${ACCESS_LOG_FILE}

echo
echo ">>>>>>>>>> CASE 2 <<<<<<<<<<"
echo "Extend case"
ACCESS_LOG_FILE=${SCRIPTPATH}/target/example_access_log_with_cycle.txt
echo ">>>>> Access log ($ACCESS_LOG_FILE):"
cat ${ACCESS_LOG_FILE}
echo
echo ">>>>> Program output:"
AOE_KIND="no"
time java -jar ${PROGRAM} 1 1 ${AOE_KIND} ${ACCESS_LOG_FILE}
echo ">>>>> done."

exit 0
