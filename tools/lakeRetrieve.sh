#!/bin/bash

# Source for utility to store and retrieve data into/from Delta Lake:
#  https://github.com/bom4v/delta-lake-io
# Artefacts on Maven repositories:
#  https://repo1.maven.org/maven2/org/bom4v/ti/delta-lake-io_2.12/

#
TOOL_NAME="delta-lake-io"
TOOL_VERSION="0.0.1"
SCALA_MJR="2.12"
SPARK_MJR="2.4"
ORG_L1="org"
ORG_L2="bom4v"
ORG_L3="ti"
MVN_REPO="https://repo1.maven.org/maven2"
TGT_DIR="target/scala-${SCALA_MJR}"
TOOL_CLS="${ORG_L1}.${ORG_L2}.${ORG_L3}.DeltaLakeRetriever"
# Delta Lake
DLK_DIR="/tmp/delta-lake/table.dlk"
# Output CSV file
CSV_FILE="delta-extract.csv"
#
KRB_KEYTAB="${HOME}/${USER}.headless.keytab"
KRB_PRINCIPAL="${USER}/${HOSTNAME}"
#
has_changed_dir="0"

# Usage
if [ "$1" == "-h" -o "$1" == "--help" ]
then
    echo
    echo "Usage: $0 <path-to-delta-lake-table> <csv-output-data-file>"
    echo "  - Default Delta Lake location: ${DLK_DIR}"
    echo "  - Default output CSV file: ${CSV_FILE}"
    echo
    exit
fi

# Parameters
if [ ! -z "$1" ]
then
    DLK_DIR="$1"
fi
if [ ! -z "$2" ]
then
    CSV_FILE="$2"
fi

# Check for the directory where that script is launched from
if [ ! -x "tools/lakeRetrieve.sh" ]
then
    TOOL_DIR="$(dirname $0)"
    pushd ${TOOL_DIR} > /dev/null 2>&1
    pushd .. > /dev/null 2>&1
    has_changed_dir="1"
    NEW_DIR="${PWD}"
    #echo
    #echo "That script ($0) should be launched from ${NEW_DIR}; go there to see logs for instance"
    #echo "cd ${NEW_DIR} && ./tools/lakeRetrieve.sh"
    #echo
fi

# Remove potential previous output data files
echo
echo "Removing potential previous output data file (${CSV_FILE})..."
\rm -f ${CSV_FILE}
echo

# Test for wget
if [ ! -x "$(command -v wget)" ]
then
    echo
    echo "The wget utility cannot be found on that machine. It is needed to download Jar artefacts. Ask your administrator to install it"
    echo
    exit -1
fi

# Test for target directory (where Jar artefacts are stored)
if [ ! -d ${TGT_DIR} ]
then
    mkdir -p ${TGT_DIR}
fi

#
JAR_FILENAME="${TOOL_NAME}_${SCALA_MJR}-${TOOL_VERSION}-spark${SPARK_MJR}.jar"
JAR_URL="${MVN_REPO}/${ORG_L1}/${ORG_L2}/${ORG_L3}/${TOOL_NAME}_${SCALA_MJR}/${TOOL_VERSION}-spark${SPARK_MJR}/${JAR_FILENAME}"
if [ ! -f ${TGT_DIR}/${JAR_FILENAME} ]
then
    echo
    echo "Downloading ${JAR_FILENAME} from Maven repository (${MVN_REPO}) into ${TGT_DIR}..."
    wget ${JAR_URL} -O ${TGT_DIR}/${JAR_FILENAME}
    echo
fi

#
echo "Launching Spark job to extract the data from Delta Lake (${DLK_DIR})..."
spark-submit --master yarn --deploy-mode client --class ${TOOL_CLS} \
	     ${TGT_DIR}/${JAR_FILENAME} ${DLK_DIR} ${CSV_FILE}
echo "... done"
echo "Resulting data file (${CSV_FILE}):"
hdfs dfs -ls -h ${CSV_FILE}
echo

# Back to the initial directory
if [ "${has_changed_dir}" == "1" ]
then
    popd > /dev/null 2>&1
    popd > /dev/null 2>&1
fi

