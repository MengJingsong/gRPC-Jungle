set -e
export KVSTORE_HOME=/home/meng.479/gRPC-Jungle/build
export YCSB_HOME=/home/meng.479/YCSB

hostfile_template="ycsb/hosts"
hostfile_template=$(realpath "$hostfile_template")
if [[ ! -f "$hostfile_template" ]]; then
  echo "Bad hostfile $hostfile_template"
  exit 1
fi

function set_hostfile() {
  n_clients=$1
  name_prefix=$(basename "$hostfile_template")
  hostfile="/tmp/$name_prefix"
  ./gen_hostfile.py ycsb/hosts "$n_clients" >"$hostfile"
  export HOSTS_PATH="$hostfile"
  echo $hostfile
}

function clean() {
  profile="$1"
  ./ycsb/ycsb.sh -c=clean -p="$(realpath ycsb/"$profile")"
}

function load() {
  profile="$1"
  export GRPC_PLATFORM_TYPE=TCP
  set_hostfile 1
  ./ycsb/ycsb.sh -c=load -p="$(realpath ycsb/"$profile")" --async --thread=1
}

function scalability() {
  profile="$1"
  N_CLIENTS=(32)
  GRPC_MODES=(TCP RDMA_EVENT RDMA_BP RDMA_BPEV)
  GRPC_MODES=(RDMA_BP)
  export WORKLOADS="workloada workloadb workloadc workloadd workloadf"

  for grpc_mode in "${GRPC_MODES[@]}"; do
    export GRPC_PLATFORM_TYPE=$grpc_mode
    for n_cli in "${N_CLIENTS[@]}"; do
      set_hostfile "$n_cli"

      thread=28
      bp_to=0
      if [[ n_cli -lt 32 ]]; then
        if [[ "${grpc_mode}" == "RDMA_BPEV" ]]; then
          thread=27
        else
          thread=28
        fi
        bp_to=50
      elif [[ n_cli -eq 32 ]]; then
        thread=32
      elif [[ n_cli -eq 64 ]]; then
        thread=64
        bp_to=50
      elif [[ n_cli -eq 128 ]]; then
        thread=128
        bp_to=50
      fi

      export LOG_SUFFIX="th_${thread}"
      ./ycsb/ycsb.sh -c=run \
        -p="$(realpath ycsb/"$profile")" \
        --async \
        --thread="$thread" \
        --bp-timeout=$bp_to --overwrite
    done
  done
}

for i in "$@"; do
  case $i in
  --clean=*)
    clean "${i#*=}"
    shift
    ;;
  --load=*)
    load "${i#*=}"
    shift
    ;;
  --scalability=*)
    scalability "${i#*=}"
    shift
    ;;
  --* | -*)
    echo "Unknown option $i"
    exit 1
    ;;
  *)
    echo "--bench or --profile"
    exit 1
    ;;
  esac
done
