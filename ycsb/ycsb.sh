#!/usr/bin/env bash
#set -e
SCRIPT_DIR=$(realpath "$(dirname "$0")")
if [[ -z "$HOSTS_PATH" ]]; then
  HOSTS_PATH="$SCRIPT_DIR/hosts"
  echo "Using default $HOSTS_PATH"
fi
DB_PREFIX="/dev/shm/rocks.db"
SERVER=$(head -n 1 "$HOSTS_PATH")
FIRST_CLIENT=$(head -n 2 "$HOSTS_PATH" | tail -n 1 | cut -d" " -f1,1)
NP=$(awk '{ sum += $1 } END { print sum }' <(tail -n +2 "$HOSTS_PATH" | cut -d"=" -f2,2))
MPI_LIB=$(realpath "$(which mpirun | xargs dirname)"/../lib)
ASYNC=false
THREAD=$(nproc)
OVERWRITE=false

if [[ -z "$WORKLOADS" ]]; then
  WORKLOADS="workloada workloadb workloadc workloadd workloade workloadf"
fi

if [[ ! -f "$YCSB_HOME/bin/ycsb" ]]; then
  echo "Invalid YCSB_HOME"
  exit 1
fi

if [[ ! -f "$KVSTORE_HOME/kv_store" ]]; then
  echo "Invalid KVSTORE_HOME"
  exit 1
fi

for i in "$@"; do
  case $i in
  -c=* | --cmd=*)
    CMD="${i#*=}"
    shift
    ;;
  -p=* | --profile=*)
    PROFILE_PATH="${i#*=}"
    shift
    ;;
  --async)
    ASYNC=true
    shift
    ;;
  --overwrite)
    OVERWRITE=true
    shift
    ;;
  -t=* | --thread=*)
    THREAD="${i#*=}"
    shift
    ;;
  --bp-timeout=*)
    export GRPC_BP_TIMEOUT="${i#*=}"
    shift
    ;;
  --* | -*)
    echo "Unknown option $i"
    exit 1
    ;;
  *) ;;
  esac
done

if [[ -f "$PROFILE_PATH" ]]; then
  PROFILE_PATH=$(realpath "$PROFILE_PATH")
else
  echo "Invalid profile $PROFILE_PATH"
  exit 1
fi

function get_logs_dir() {
    LOG_PATH=$(realpath "$SCRIPT_DIR/logs")
    LOG_PATH="$LOG_PATH/${GRPC_PLATFORM_TYPE}_$NP"
    if [[ $ASYNC == true ]]; then
      LOG_PATH="${LOG_PATH}_async"
    else
      LOG_PATH="${LOG_PATH}_sync"
    fi

    if [[ -n "$LOG_SUFFIX" ]]; then
      LOG_PATH="${LOG_PATH}_${LOG_SUFFIX}"
    fi
    mkdir -p "$LOG_PATH"
    echo "$LOG_PATH"
}


function kill_server() {
  ssh "$SERVER" 'if ps aux | pgrep kv_store >/dev/null; then pkill kv_store && while $(pkill -0 kv_store 2>/dev/null); do continue; done; fi'
  #  ssh "$SERVER" 'ps aux|pgrep kv_store|xargs kill -9 || true'
}

function get_db_file() {
  workload="$1"
  profile_name=$(basename "$PROFILE_PATH")
  db_file="${DB_PREFIX}/${workload}_$profile_name"
  echo "$db_file"
}

function start_server() {
  kill_server
  workload="$1"
  db_file="$(get_db_file "$workload")"
  # Launch server
  mpirun --bind-to none -x GRPC_PLATFORM_TYPE -x GRPC_BP_TIMEOUT \
    -n 1 -host "$SERVER" \
    "$KVSTORE_HOME"/kv_store -server -async=$ASYNC -db_file="$db_file" -thread="$THREAD" &
}

function load() {
  workload=$1
  db_file="$(get_db_file "$workload")"
  if ssh "$SERVER" "[ -d $db_file ]"; then
    echo "DB $workload exists, skip"
  else
    if ssh "$SERVER" mkdir -p "$db_file"; then
      start_server "$workload"
      echo "Loading $workload"
      mpirun --bind-to none -x GRPC_PLATFORM_TYPE -x GRPC_BP_TIMEOUT -n 1 -wdir "$YCSB_HOME" -host "$FIRST_CLIENT" \
        python2 "$YCSB_HOME"/bin/ycsb load grpcrocksdb \
        -jvm-args="-Djava.library.path=${KVSTORE_HOME}:${MPI_LIB}" \
        -P "$YCSB_HOME/workloads/$workload" \
        -P "$PROFILE_PATH" \
        -s -p grpc.addr="$SERVER:12345"
      kill_server
    else
      echo "Failed to create $db_file"
      exit 1
    fi
  fi
}

for workload in ${WORKLOADS}; do
  db_file="$(get_db_file "$workload")"

  if [[ $CMD == "load" ]]; then
    load "$workload"
  elif [[ $CMD == "clean" ]]; then
    echo "Remove $db_file on $SERVER"
    ssh "$SERVER" rm -rf "$db_file"
  elif [[ $CMD == "run" ]]; then
    LOG_DIR=$(get_logs_dir)
    curr_log_path="$LOG_DIR/${workload}.log"
    if [[ $OVERWRITE == true ]]; then
      rm -f "$curr_log_path"
    fi
    if [[ -f "$curr_log_path" ]]; then
      echo "$curr_log_path exists, skip"
    else
      while true; do
        if ! ssh "$SERVER" "[ -d $db_file ]"; then
          load "$workload"
        fi
        start_server "$workload"

        tmp_host="/tmp/clients.$RANDOM"
        if [[ -f $tmp_host ]]; then
          tmp_host="/tmp/clients.$RANDOM"
        fi
        tail -n +2 "$HOSTS_PATH" >"$tmp_host"
        echo "============================= Running $workload with $NP clients, mode: $GRPC_PLATFORM_TYPE"
        # Evaluate
        mpirun --bind-to none -x GRPC_PLATFORM_TYPE \
          -mca btl_tcp_if_include ib0 \
          -wdir "$YCSB_HOME" -np "$NP" -hostfile "$tmp_host" \
          python2 "$YCSB_HOME"/bin/ycsb run grpcrocksdb \
          -jvm-args="-Djava.library.path=${KVSTORE_HOME}:${MPI_LIB}" \
          -P "$YCSB_HOME/workloads/$workload" \
          -P "$PROFILE_PATH" \
          -s -p grpc.addr="$SERVER:12345" | tee "${curr_log_path}.tmp" 2>&1
        kill_server

        row_count=$(grep -c "\[OVERALL\], Throughput" <"${curr_log_path}.tmp")

        if [[ $row_count != "$NP" ]]; then
          echo "Found error when evaluate workload $workload, retrying"
        else
          mv "${curr_log_path}.tmp" "${curr_log_path}"
          break
        fi
      done
    fi
  else
    echo "Bad command: $CMD"
  fi
done
