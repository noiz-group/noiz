#!/bin/bash
# =============================================================================
# Noiz Full Workflow Runner
# =============================================================================
# Usage: ./workflow_runner.sh [stage]
# Stages: deploy, load_data, process, status, interactive, all
#
# This script automates the full noiz seismology processing workflow.
# Based on debugging sessions and best practices for Docker deployment.
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

log_info() { echo -e "${BLUE}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_step() { echo -e "${CYAN}[STEP]${NC} $1"; }

# =============================================================================
# Configuration - Adjust these for your deployment
# =============================================================================
# Docker settings
CONTAINER_NAME="${CONTAINER_NAME:-noiz-deployment-noiz-1}"
POSTGRES_CONTAINER="${POSTGRES_CONTAINER:-noiz-deployment-postgres-1}"
COMPOSE_DIR="${COMPOSE_DIR:-$(dirname "$(realpath "$0")")/..}"

# Database settings
POSTGRES_USER="${POSTGRES_USER:-noiztest}"
POSTGRES_DB="${POSTGRES_DB:-noiztest}"

# Data paths (inside container)
SDS_PATH="${SDS_PATH:-/SDS}"
PARAM_DIR="${PARAM_DIR:-/SDS/param_toml}"
STATION_XML="${STATION_XML:-/SDS/station.xml}"
SEISMIC_DATA_DIR="${SEISMIC_DATA_DIR:-/SDS/seismic-data}"
SOH_DATA_DIR="${SOH_DATA_DIR:-/SDS/soh-data}"

# Processing date range
START_DATE="${START_DATE:-2019-10-01}"
END_DATE="${END_DATE:-2019-10-02}"

# Network code
NETWORK="${NETWORK:-EN}"

# Station configuration
# Format: "station:digitizer_type:gps_param"
# Digitizer types: centaur, taurus
# GPS params: gnsstime (centaur), gpstime (taurus)
STATIONS=(
    "ES03:centaur:gnsstime"
    "ES04:taurus:gpstime"
    "ES05:centaur:gnsstime"
    "ES11:centaur:gnsstime"
    "ES13:centaur:gnsstime"
    "ES203:taurus:gpstime"
    "ES23:taurus:gpstime"
    "ES26:taurus:gpstime"
    "ES29:centaur:gnsstime"
    "ES30:taurus:gpstime"
    "ES37:centaur:gnsstime"
)

# Config file names (adjust if different)
DATACHUNK_CONFIG="datachunk_params_p1.toml"
QCONE_CONFIG="QCOneConfig.toml"
PPSD_CONFIG="ppsd_params.toml"
PROCESSED_DATACHUNK_CONFIG="processed_datachunk_params.toml"
CROSSCORRELATION_CONFIG="crosscorrelation_cartesian_params.toml"
CROSSCORRELATION_CYLINDRICAL_CONFIG="crosscorrelation_cylindric_params.toml"
BEAMFORMING_CONFIG="beamforming_params.toml"
QCTWO_CONFIG="QCTwoConfig.toml"
STACKING_SCHEMA_CONFIG="my_stacking_schema.toml"

# Cylindrical CCF parameters
# Note: Cylindrical CCF requires cartesian CCFs to be computed first
# Batch size is number of timespans per batch (controls memory usage)
CYLINDRICAL_BATCH_SIZE="${CYLINDRICAL_BATCH_SIZE:-125}"

# Beamforming parameters
BEAMFORMING_FREQ_MIN="${BEAMFORMING_FREQ_MIN:-0.80}"
BEAMFORMING_FREQ_MAX="${BEAMFORMING_FREQ_MAX:-1.4}"
BEAMFORMING_FREQ_STEP="${BEAMFORMING_FREQ_STEP:-0.05}"
BEAMFORMING_FREQ_WIDTH="${BEAMFORMING_FREQ_WIDTH:-0.1}"
# Note: Beamforming runs on raw Datachunks (not ProcessedDatachunks).
# The -p flag for run_beamforming refers to beamforming_params_id, NOT processed_datachunk_params_id.
# Default beamforming params IDs to use (space-separated). Set to empty to auto-detect all IDs.
BEAMFORMING_PARAMS_IDS="${BEAMFORMING_PARAMS_IDS:-1 10}"

# Stacking parameters
STACKING_COMPONENT="${STACKING_COMPONENT:-ZZ}"
STACKING_COMPONENT_CYLINDRICAL="${STACKING_COMPONENT_CYLINDRICAL:-RR}"
STACKING_BATCH_SIZE="${STACKING_BATCH_SIZE:-200}"

# Timespan parameters
TIMESPAN_WINDOW_LENGTH="${TIMESPAN_WINDOW_LENGTH:-1860}"
TIMESPAN_WINDOW_OVERLAP="${TIMESPAN_WINDOW_OVERLAP:-60}"

# Processing options
USE_PARALLEL="${USE_PARALLEL:-true}"
BATCH_SIZE="${BATCH_SIZE:-1000}"
LOG_DIR="${LOG_DIR:-/processed-data-dir/logs}"

# =============================================================================
# Helper Functions
# =============================================================================
check_container_running() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log_error "Container $CONTAINER_NAME is not running"
        log_info "Run: docker compose up -d"
        exit 1
    fi
}

check_postgres_running() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${POSTGRES_CONTAINER}$"; then
        log_error "PostgreSQL container $POSTGRES_CONTAINER is not running"
        exit 1
    fi
}

run_noiz() {
    local cmd="$1"
    local description="$2"
    log_step "$description"
    # Use yes to auto-answer prompts, remove -it for non-interactive
    yes | docker exec -i "$CONTAINER_NAME" noiz $cmd || true
}

run_noiz_bg() {
    local cmd="$1"
    local description="$2"
    local logfile="$3"
    log_step "$description"
    log_info "Output being logged to: $logfile"
    docker exec "$CONTAINER_NAME" mkdir -p "$LOG_DIR"
    # Use yes to auto-answer prompts
    yes | docker exec "$CONTAINER_NAME" bash -c "noiz $cmd 2>&1 | tee $logfile" || true
}

get_db_count() {
    local table="$1"
    docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
        "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' '
}

# =============================================================================
# Deployment Functions
# =============================================================================
deploy_fresh() {
    log_info "Starting fresh deployment..."
    
    cd "$COMPOSE_DIR"
    
    log_step "Stopping existing containers..."
    docker compose down -v 2>/dev/null || true
    
    log_step "Removing existing database data..."
    sudo rm -rf noiz-postgres-data 2>/dev/null || rm -rf noiz-postgres-data 2>/dev/null || true
    mkdir -p noiz-postgres-data
    
    log_step "Starting containers..."
    docker compose up -d
    
    log_step "Waiting for containers to be ready..."
    sleep 30
    
    check_container_running
    check_postgres_running
    
    log_step "Running database migrations..."
    # db migrate may return non-zero if no new migration is needed, so we allow it to fail
    log_step "Creating migration (may be skipped if already up to date)"
    docker exec "$CONTAINER_NAME" noiz db migrate || log_warn "Migration creation returned non-zero (may be normal if no changes needed)"
    
    log_step "Applying migration"
    docker exec "$CONTAINER_NAME" noiz db upgrade
    
    log_success "Deployment complete!"
}

deploy_restart() {
    log_info "Restarting deployment (preserving data)..."
    
    cd "$COMPOSE_DIR"
    docker compose down
    docker compose up -d
    
    sleep 10
    check_container_running
    
    log_success "Deployment restarted!"
}

# =============================================================================
# Data Loading Functions
# =============================================================================
load_inventory() {
    log_info "Loading station inventory..."
    check_container_running
    
    run_noiz "data add_inventory $STATION_XML" "Adding inventory from $STATION_XML"
    
    local station_count=$(get_db_count "station")
    local component_count=$(get_db_count "component")
    local pair_count=$(get_db_count "component_pair")
    
    log_success "Inventory loaded: $station_count stations, $component_count components, $pair_count pairs"
}

load_seismic_data() {
    log_info "Loading seismic data..."
    check_container_running
    
    log_step "Adding seismic data from $SEISMIC_DATA_DIR/"
    yes | docker exec -i "$CONTAINER_NAME" noiz data add_seismic_data "$SEISMIC_DATA_DIR/" || true
    
    log_success "Seismic data loaded"
}

add_timespans() {
    log_info "Adding timespans..."
    check_container_running
    
    run_noiz "data add_timespans -sd $START_DATE -ed $END_DATE -wl $TIMESPAN_WINDOW_LENGTH -wo $TIMESPAN_WINDOW_OVERLAP --generate_over_midnight" \
        "Adding timespans from $START_DATE to $END_DATE"
    
    local count=$(get_db_count "timespan")
    log_success "Timespans added: $count"
}

load_soh_data() {
    log_info "Loading SOH data for all stations..."
    check_container_running
    
    for station_config in "${STATIONS[@]}"; do
        IFS=':' read -r station digitizer gps_param <<< "$station_config"
        log_step "Adding SOH data for $station (type=$digitizer, param=$gps_param)"
        
        # Find year directories
        local year_dir="$SOH_DATA_DIR/$station/2019"
        yes | docker exec -i "$CONTAINER_NAME" noiz data add_soh_dir \
            -t "$digitizer" -p "$gps_param" -n "$NETWORK" -s "$station" "$year_dir/" || {
            log_warn "SOH data for $station may not exist or failed to load"
        }
    done
    
    log_success "SOH data loading complete"
}

load_all_data() {
    load_inventory
    load_seismic_data
    add_timespans
    load_soh_data
}

# =============================================================================
# Processing Functions
# =============================================================================
prepare_datachunks() {
    log_info "Preparing datachunks..."
    check_container_running
    
    run_noiz "configs add_datachunk_params -f $PARAM_DIR/$DATACHUNK_CONFIG --add_to_db" \
        "Adding datachunk params"
    
    run_noiz_bg "processing prepare_datachunks -sd $START_DATE -ed $END_DATE" \
        "Preparing datachunks" "$LOG_DIR/prepare_datachunks.log"
    
    run_noiz_bg "processing average_soh_gps -sd $START_DATE -ed $END_DATE" \
        "Averaging SOH GPS" "$LOG_DIR/average_soh_gps.log"
    
    run_noiz_bg "processing calc_datachunk_stats -sd $START_DATE -ed $END_DATE -p 1" \
        "Calculating datachunk stats" "$LOG_DIR/calc_datachunk_stats.log"
    
    local count=$(get_db_count "datachunk")
    log_success "Datachunks prepared: $count"
}

run_qcone() {
    log_info "Running QC One..."
    check_container_running
    
    run_noiz "configs add_qcone_config -f $PARAM_DIR/$QCONE_CONFIG --add_to_db" \
        "Adding QC One config"
    
    run_noiz_bg "processing run_qcone -sd $START_DATE -ed $END_DATE" \
        "Running QC One" "$LOG_DIR/qcone.log"
    
    local count=$(get_db_count "qcone_results")
    log_success "QC One complete: $count results"
}

run_ppsd() {
    log_info "Running PPSD calculation..."
    check_container_running
    
    # Check if config already exists
    local ppsd_config_count=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
        "SELECT COUNT(*) FROM ppsd_params;" 2>/dev/null | tr -d ' ')
    
    if [ "$ppsd_config_count" = "0" ]; then
        run_noiz "configs add_ppsd_params -f $PARAM_DIR/$PPSD_CONFIG --add_to_db" \
            "Adding PPSD params"
    fi
    
    local parallel_flag="--parallel"
    if [ "$USE_PARALLEL" != "true" ]; then
        parallel_flag="--no_parallel"
    fi
    
    run_noiz_bg "processing run_ppsd -sd $START_DATE -ed $END_DATE -p 1 $parallel_flag" \
        "Running PPSD ($parallel_flag)" "$LOG_DIR/ppsd_run.log"
    
    local count=$(get_db_count "ppsd_result")
    log_success "PPSD complete: $count results"
}

run_ppsd_sequential() {
    log_info "Running PPSD calculation (sequential mode)..."
    USE_PARALLEL="false" run_ppsd
}

process_datachunks() {
    log_info "Processing datachunks..."
    check_container_running
    
    # Check if config already exists
    local config_count=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
        "SELECT COUNT(*) FROM processed_datachunk_params;" 2>/dev/null | tr -d ' ')
    
    if [ "$config_count" = "0" ]; then
        run_noiz "configs add_processed_datachunk_params -f $PARAM_DIR/$PROCESSED_DATACHUNK_CONFIG --add_to_db" \
            "Adding processed datachunk params"
    fi
    
    local parallel_flag="--parallel"
    if [ "$USE_PARALLEL" != "true" ]; then
        parallel_flag="--no_parallel"
    fi
    
    run_noiz_bg "processing process_datachunks -sd $START_DATE -ed $END_DATE -p 1 $parallel_flag" \
        "Processing datachunks ($parallel_flag)" "$LOG_DIR/process_datachunks.log"
    
    local count=$(get_db_count "processeddatachunk")
    log_success "Processed datachunks complete: $count"
}

run_crosscorrelations() {
    log_info "Running cross-correlations..."
    check_container_running
    
    # Check if config already exists
    local config_count=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
        "SELECT COUNT(*) FROM crosscorrelation_cartesian_params;" 2>/dev/null | tr -d ' ')
    
    if [ "$config_count" = "0" ]; then
        run_noiz "configs add_crosscorrelation_cartesian_params -f $PARAM_DIR/$CROSSCORRELATION_CONFIG --add_to_db" \
            "Adding cross-correlation params"
    fi
    
    local parallel_flag="--parallel"
    if [ "$USE_PARALLEL" != "true" ]; then
        parallel_flag="--no_parallel"
    fi
    
    run_noiz_bg "processing run_crosscorrelations_cartesian -sd $START_DATE -ed $END_DATE -p 1 $parallel_flag" \
        "Running cross-correlations ($parallel_flag)" "$LOG_DIR/crosscorrelations.log"
    
    local count=$(get_db_count "crosscorrelation_cartesian")
    log_success "Cross-correlations complete: $count"
}

add_cylindrical_componentpairs() {
    log_info "Adding cylindrical component pairs..."
    check_container_running
    
    # Check if cylindrical component pairs already exist
    local pair_count=$(get_db_count "componentpair_cylindrical")
    
    if [ "$pair_count" = "0" ]; then
        run_noiz "data add_cylindrical_componentpair --upsert" \
            "Adding cylindrical component pairs from existing cartesian pairs"
    else
        log_info "Cylindrical component pairs already exist ($pair_count pairs), skipping..."
    fi
    
    pair_count=$(get_db_count "componentpair_cylindrical")
    log_success "Cylindrical component pairs: $pair_count"
}

run_crosscorrelations_cylindrical() {
    log_info "Running cylindrical cross-correlations..."
    check_container_running
    
    # Cylindrical CCF requires cartesian CCFs to be computed first
    local cartesian_count=$(get_db_count "crosscorrelation_cartesian")
    if [ "$cartesian_count" = "0" ]; then
        log_error "No cartesian cross-correlations found! Run crosscorr first."
        log_info "Cylindrical CCF computation requires cartesian CCFs as input."
        return 1
    fi
    
    # Ensure cylindrical component pairs exist
    local cyl_pair_count=$(get_db_count "componentpair_cylindrical")
    if [ "$cyl_pair_count" = "0" ]; then
        log_info "No cylindrical component pairs found. Creating them first..."
        add_cylindrical_componentpairs
    fi
    
    # Check if config already exists
    local config_count=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
        "SELECT COUNT(*) FROM crosscorrelation_cylindrical_params;" 2>/dev/null | tr -d ' ')
    
    if [ "$config_count" = "0" ]; then
        run_noiz "configs add_crosscorrelation_cylindrical_params -f $PARAM_DIR/$CROSSCORRELATION_CYLINDRICAL_CONFIG --add_to_db" \
            "Adding cylindrical cross-correlation params"
    fi
    
    local parallel_flag="--parallel"
    if [ "$USE_PARALLEL" != "true" ]; then
        parallel_flag="--no_parallel"
    fi
    
    local overwrite_flag=""
    if [ "$USE_OVERWRITE" = "true" ]; then
        overwrite_flag="--overwrite"
    fi
    
    run_noiz_bg "processing run_crosscorrelations_cylindrical -sd $START_DATE -ed $END_DATE -p 1 -b $CYLINDRICAL_BATCH_SIZE $parallel_flag $overwrite_flag" \
        "Running cylindrical cross-correlations ($parallel_flag $overwrite_flag, batch=$CYLINDRICAL_BATCH_SIZE)" "$LOG_DIR/crosscorrelations_cylindrical.log"
    
    local count=$(get_db_count "crosscorrelation_cylindrical")
    log_success "Cylindrical cross-correlations complete: $count"
}

run_crosscorrelations_cylindrical_sequential() {
    log_info "Running cylindrical cross-correlations (sequential mode)..."
    USE_PARALLEL="false" run_crosscorrelations_cylindrical
}

run_beamforming() {
    log_info "Running beamforming..."
    check_container_running
    
    # Generate beamforming params config and add to DB
    run_noiz "configs generate_beamforming_params -f $PARAM_DIR/$BEAMFORMING_CONFIG -fn $BEAMFORMING_FREQ_MIN -fx $BEAMFORMING_FREQ_MAX -fp $BEAMFORMING_FREQ_STEP -fw $BEAMFORMING_FREQ_WIDTH" \
        "Generating beamforming params"
    
    # Use configured beamforming_params IDs or auto-detect from database
    local beamforming_ids
    if [ -n "$BEAMFORMING_PARAMS_IDS" ]; then
        beamforming_ids="$BEAMFORMING_PARAMS_IDS"
        log_info "Using configured beamforming_params IDs: $beamforming_ids"
    else
        log_step "Querying beamforming_params IDs from database..."
        beamforming_ids=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
            "SELECT id FROM beamforming_params ORDER BY id;" 2>/dev/null | tr -d ' ' | grep -v '^$')
        
        if [ -z "$beamforming_ids" ]; then
            log_error "No beamforming_params found in database. Make sure generate_beamforming_params was successful."
            return 1
        fi
        log_info "Auto-detected beamforming_params IDs from database"
    fi
    
    # Build beamforming params flags
    local beamforming_params_flags=""
    for p in $beamforming_ids; do
        beamforming_params_flags="$beamforming_params_flags -p $p"
    done
    log_info "Using beamforming_params IDs:$beamforming_params_flags"
    
    run_noiz_bg "processing run_beamforming -sd $START_DATE -ed $END_DATE $beamforming_params_flags" \
        "Running beamforming" "$LOG_DIR/beamforming.log"
    
    local count=$(get_db_count "beamforming_result")
    log_success "Beamforming complete: $count results"
}

run_qctwo() {
    log_info "Running QC Two..."
    check_container_running
    
    # Check if config already exists
    local config_count=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
        "SELECT COUNT(*) FROM qctwo_config;" 2>/dev/null | tr -d ' ')
    
    if [ "$config_count" = "0" ]; then
        run_noiz "configs add_qctwo_config -f $PARAM_DIR/$QCTWO_CONFIG" \
            "Adding QC Two config"
    fi
    
    run_noiz_bg "processing run_qctwo -p 1" \
        "Running QC Two" "$LOG_DIR/qctwo.log"
    
    local count=$(get_db_count "qctwo_results")
    log_success "QC Two complete: $count results"
}

run_stacking() {
    log_info "Running stacking..."
    check_container_running
    
    # Check if config already exists
    local config_count=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
        "SELECT COUNT(*) FROM stacking_schema;" 2>/dev/null | tr -d ' ')
    
    if [ "$config_count" = "0" ]; then
        run_noiz "configs add_stacking_schema -f $PARAM_DIR/$STACKING_SCHEMA_CONFIG" \
            "Adding stacking schema"
    fi
    
    run_noiz_bg "processing run_stacking -sd $START_DATE -ed $END_DATE -p 1 -c $STACKING_COMPONENT -b $STACKING_BATCH_SIZE" \
        "Running stacking (component=$STACKING_COMPONENT, batch=$STACKING_BATCH_SIZE)" "$LOG_DIR/stacking.log"
    
    local count=$(get_db_count "ccfstack")
    log_success "Stacking complete: $count results"
}

run_stacking_cylindrical() {
    log_info "Running cylindrical stacking..."
    check_container_running
    
    # Cylindrical stacking requires cylindrical CCFs to be computed first
    local cyl_ccf_count=$(get_db_count "crosscorrelation_cylindrical")
    if [ "$cyl_ccf_count" = "0" ]; then
        log_error "No cylindrical cross-correlations found! Run crosscorr_cyl first."
        log_info "Cylindrical stacking requires cylindrical CCFs as input."
        return 1
    fi
    
    # Check if stacking schema exists (reuse from cartesian stacking)
    local config_count=$(docker exec "$POSTGRES_CONTAINER" psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -t -c \
        "SELECT COUNT(*) FROM stacking_schema;" 2>/dev/null | tr -d ' ')
    
    if [ "$config_count" = "0" ]; then
        run_noiz "configs add_stacking_schema -f $PARAM_DIR/$STACKING_SCHEMA_CONFIG" \
            "Adding stacking schema"
    fi
    
    local parallel_flag="--parallel"
    if [ "$USE_PARALLEL" != "true" ]; then
        parallel_flag="--no_parallel"
    fi
    
    run_noiz_bg "processing run_stacking_cylindrical -sd $START_DATE -ed $END_DATE -p 1 -c $STACKING_COMPONENT_CYLINDRICAL -b $STACKING_BATCH_SIZE $parallel_flag" \
        "Running cylindrical stacking (component=$STACKING_COMPONENT_CYLINDRICAL, batch=$STACKING_BATCH_SIZE, $parallel_flag)" "$LOG_DIR/stacking_cylindrical.log"
    
    local count=$(get_db_count "ccfstack_cylindrical")
    log_success "Cylindrical stacking complete: $count results"
}

run_stacking_cylindrical_sequential() {
    log_info "Running cylindrical stacking (sequential mode)..."
    USE_PARALLEL="false" run_stacking_cylindrical
}

run_all_processing() {
    prepare_datachunks
    run_qcone
    run_ppsd
    process_datachunks
    run_crosscorrelations
    run_qctwo
    run_stacking
    run_beamforming
}

# =============================================================================
# Status and Monitoring Functions
# =============================================================================
show_status() {
    log_info "Checking workflow status..."
    
    check_container_running
    check_postgres_running
    
    echo ""
    echo "==========================================="
    echo "         NOIZ WORKFLOW STATUS              "
    echo "==========================================="
    echo ""
    
    # Container status
    echo "Container Status:"
    docker ps --filter "name=noiz-deployment" --format "  {{.Names}}: {{.Status}}"
    echo ""
    
    # Database counts
    echo "Database Counts:"
    echo "  Stations:              $(get_db_count 'station')"
    echo "  Components:            $(get_db_count 'component')"
    echo "  Component Pairs (Cart):$(get_db_count 'componentpair_cartesian')"
    echo "  Component Pairs (Cyl): $(get_db_count 'componentpair_cylindrical')"
    echo "  Timespans:             $(get_db_count 'timespan')"
    echo "  Datachunks:            $(get_db_count 'datachunk')"
    echo "  QCOne Results:         $(get_db_count 'qcone_results')"
    echo "  PPSD Results:          $(get_db_count 'ppsd_result')"
    echo "  Processed Chunks:      $(get_db_count 'processeddatachunk')"
    echo "  CCF Cartesian:         $(get_db_count 'crosscorrelation_cartesian')"
    echo "  CCF Cylindrical:       $(get_db_count 'crosscorrelation_cylindrical')"
    echo "  QCTwo Results:         $(get_db_count 'qctwo_results')"
    echo "  Stacks Cartesian:      $(get_db_count 'ccfstack')"
    echo "  Stacks Cylindrical:    $(get_db_count 'ccfstack_cylindrical')"
    echo "  Beamforming Results:   $(get_db_count 'beamforming_result')"
    echo ""
    
    # Config counts
    echo "Configuration Counts:"
    echo "  Datachunk Params:         $(get_db_count 'datachunk_params')"
    echo "  QCOne Configs:            $(get_db_count 'qcone_config')"
    echo "  PPSD Params:              $(get_db_count 'ppsd_params')"
    echo "  Processed DC Params:      $(get_db_count 'processed_datachunk_params')"
    echo "  CrossCorr Cart Params:    $(get_db_count 'crosscorrelation_cartesian_params')"
    echo "  CrossCorr Cyl Params:     $(get_db_count 'crosscorrelation_cylindrical_params')"
    echo "  QCTwo Configs:            $(get_db_count 'qctwo_config')"
    echo "  Stacking Schemas:         $(get_db_count 'stacking_schema')"
    echo "  Beamforming Params:       $(get_db_count 'beamforming_params')"
    echo ""
    echo "==========================================="
}

show_logs() {
    local logfile="${1:-ppsd_run.log}"
    log_info "Showing last 50 lines of $logfile..."
    docker exec "$CONTAINER_NAME" tail -50 "$LOG_DIR/$logfile" 2>/dev/null || \
        log_warn "Log file not found: $LOG_DIR/$logfile"
}

check_processes() {
    log_info "Checking running processes in container..."
    docker exec "$CONTAINER_NAME" ps aux | grep -E "noiz|python|dask" | grep -v grep || \
        echo "No noiz/python/dask processes running"
}

check_zombies() {
    log_info "Checking for zombie processes..."
    local zombies=$(docker exec "$CONTAINER_NAME" ps aux | grep -E "defunct|Z" | grep -v grep | wc -l)
    if [ "$zombies" -gt 0 ]; then
        log_warn "Found $zombies zombie processes"
        docker exec "$CONTAINER_NAME" ps aux | grep -E "defunct|Z" | grep -v grep
    else
        log_success "No zombie processes found"
    fi
}

# =============================================================================
# Interactive Mode
# =============================================================================
run_interactive() {
    log_info "Starting interactive shell in noiz container..."
    check_container_running
    
    echo ""
    echo "==========================================="
    echo "  NOIZ INTERACTIVE DEBUG SESSION          "
    echo "==========================================="
    echo ""
    echo "Useful commands:"
    echo "  noiz processing run_ppsd --help"
    echo "  noiz data add_inventory --help"
    echo "  ls $PARAM_DIR/"
    echo "  ls $SEISMIC_DATA_DIR/"
    echo ""
    echo "Exit with 'exit' when done."
    echo "==========================================="
    echo ""
    
    docker exec -it "$CONTAINER_NAME" bash
}

# =============================================================================
# Full Pipeline
# =============================================================================
run_full_pipeline() {
    log_info "Starting full pipeline..."
    
    deploy_fresh
    sleep 10
    
    load_all_data
    run_all_processing
    
    show_status
    
    log_success "Full pipeline complete!"
}

# =============================================================================
# Cleanup
# =============================================================================
cleanup() {
    log_warn "This will DELETE all data including:"
    log_warn "  - Docker containers and volumes"
    log_warn "  - PostgreSQL database (noiz-postgres-data/)"
    log_warn "  - All processed data (processed-data-dir-tutorial/*)"
    echo ""
    read -p "Are you sure you want to proceed? (yes/no): " confirmation
    
    if [ "$confirmation" != "yes" ]; then
        log_info "Cleanup cancelled."
        exit 0
    fi
    
    log_info "Cleaning up deployment..."
    
    cd "$COMPOSE_DIR"
    docker compose down -v 2>/dev/null || true
    sudo rm -rf noiz-postgres-data 2>/dev/null || rm -rf noiz-postgres-data 2>/dev/null || true
    
    # Clean up processed data directory (keep the directory, remove contents)
    log_step "Cleaning up processed data directory..."
    sudo rm -rf processed-data-dir-tutorial/* 2>/dev/null || rm -rf processed-data-dir-tutorial/* 2>/dev/null || true
    
    log_success "Cleanup complete"
}

# =============================================================================
# Help
# =============================================================================
show_help() {
    echo "Noiz Full Workflow Runner"
    echo ""
    echo "Usage: $0 [stage] [options]"
    echo ""
    echo "Deployment:"
    echo "  deploy          - Fresh deployment (removes existing data)"
    echo "  restart         - Restart containers (preserves data)"
    echo "  cleanup         - Stop containers and remove all data"
    echo ""
    echo "Data Loading:"
    echo "  load_inventory  - Load station inventory"
    echo "  load_seismic    - Load seismic data"
    echo "  add_timespans   - Add timespans"
    echo "  load_soh        - Load SOH data"
    echo "  load_data       - Load all data (inventory + seismic + timespans + SOH)"
    echo ""
    echo "Processing:"
    echo "  prepare         - Prepare datachunks"
    echo "  qcone           - Run QC One"
    echo "  ppsd            - Run PPSD (parallel mode)"
    echo "  ppsd_seq        - Run PPSD (sequential mode)"
    echo "  process_chunks  - Process datachunks"
    echo "  crosscorr       - Run cartesian cross-correlations"
    echo "  add_cyl_pairs   - Add cylindrical component pairs"
    echo "  crosscorr_cyl   - Run cylindrical cross-correlations (requires cartesian CCFs)"
    echo "  crosscorr_cyl_seq - Run cylindrical CCFs (sequential mode)"
    echo "  qctwo           - Run QC Two"
    echo "  stacking        - Run cartesian CCF stacking"
    echo "  stacking_cyl    - Run cylindrical CCF stacking (requires cylindrical CCFs)"
    echo "  stacking_cyl_seq - Run cylindrical stacking (sequential mode)"
    echo "  beamforming     - Run beamforming"
    echo "  process         - Run all processing steps"
    echo ""
    echo "Full Pipeline:"
    echo "  all             - Run complete pipeline (deploy + load + process)"
    echo ""
    echo "Monitoring:"
    echo "  status          - Show current workflow status"
    echo "  logs [file]     - Show log file (default: ppsd_run.log)"
    echo "  processes       - Check running processes"
    echo "  zombies         - Check for zombie processes"
    echo ""
    echo "Debug:"
    echo "  interactive     - Start interactive shell in container"
    echo "  help            - Show this help message"
    echo ""
    echo "Environment Variables:"
    echo "  CONTAINER_NAME  - Noiz container name (default: noiz-deployment-noiz-1)"
    echo "  START_DATE      - Processing start date (default: 2019-10-01)"
    echo "  END_DATE        - Processing end date (default: 2019-10-02)"
    echo "  USE_PARALLEL    - Use parallel mode (default: true)"
    echo "  USE_OVERWRITE   - Overwrite existing records for cylindrical CCF (default: false)"
    echo "  NETWORK         - Network code (default: EN)"
    echo "  TIMESPAN_WINDOW_LENGTH  - Timespan window length in seconds (default: 1860)"
    echo "  TIMESPAN_WINDOW_OVERLAP - Timespan window overlap in seconds (default: 60)"
    echo "  BEAMFORMING_FREQ_MIN    - Beamforming min frequency (default: 0.80)"
    echo "  BEAMFORMING_FREQ_MAX    - Beamforming max frequency (default: 1.4)"
    echo "  BEAMFORMING_FREQ_STEP   - Beamforming frequency step (default: 0.05)"
    echo "  BEAMFORMING_FREQ_WIDTH  - Beamforming frequency width (default: 0.1)"
    echo "  BEAMFORMING_PARAMS_IDS  - Beamforming params IDs to use (default: 1 10)"
    echo "                            Set to empty string to auto-detect all IDs"
    echo "  STACKING_COMPONENT      - Stacking component code for cartesian (default: ZZ)"
    echo "  STACKING_COMPONENT_CYLINDRICAL - Stacking component code for cylindrical (default: RR)"
    echo "  STACKING_BATCH_SIZE     - Stacking batch size (default: 200)"
    echo "  CYLINDRICAL_BATCH_SIZE  - Cylindrical CCF batch size (default: 125)"
    echo ""
    echo "Note: Beamforming uses raw Datachunks (not ProcessedDatachunks)."
    echo "      Beamforming params IDs are auto-detected from the database."
    echo "      Cylindrical CCFs require cartesian CCFs to be computed first."
    echo "      Cylindrical stacking requires cylindrical CCFs to be computed first."
    echo ""
    echo "Examples:"
    echo "  $0 deploy                    # Fresh deployment"
    echo "  $0 all                       # Full pipeline"
    echo "  $0 status                    # Check status"
    echo "  $0 ppsd                      # Run PPSD only"
    echo "  USE_PARALLEL=false $0 ppsd   # Run PPSD sequential"
    echo "  $0 crosscorr_cyl_seq         # Run cylindrical CCF (sequential, recommended first)"
    echo "  USE_OVERWRITE=true $0 crosscorr_cyl  # Re-run cylindrical CCF with overwrite"
    echo "  $0 stacking_cyl_seq          # Run cylindrical stacking (sequential)"
    echo "  STACKING_COMPONENT_CYLINDRICAL=TT $0 stacking_cyl  # Stack TT component"
    echo "  $0 interactive               # Debug in container"
    echo ""
}

# =============================================================================
# Main
# =============================================================================
case "${1:-help}" in
    # Deployment
    deploy)         deploy_fresh ;;
    restart)        deploy_restart ;;
    cleanup)        cleanup ;;
    
    # Data Loading
    load_inventory) load_inventory ;;
    load_seismic)   load_seismic_data ;;
    add_timespans)  add_timespans ;;
    load_soh)       load_soh_data ;;
    load_data)      load_all_data ;;
    
    # Processing
    prepare)        prepare_datachunks ;;
    qcone)          run_qcone ;;
    ppsd)           run_ppsd ;;
    ppsd_seq)       run_ppsd_sequential ;;
    process_chunks) process_datachunks ;;
    crosscorr)      run_crosscorrelations ;;
    add_cyl_pairs)  add_cylindrical_componentpairs ;;
    crosscorr_cyl)  run_crosscorrelations_cylindrical ;;
    crosscorr_cyl_seq) run_crosscorrelations_cylindrical_sequential ;;
    qctwo)          run_qctwo ;;
    stacking)       run_stacking ;;
    stacking_cyl)   run_stacking_cylindrical ;;
    stacking_cyl_seq) run_stacking_cylindrical_sequential ;;
    beamforming)    run_beamforming ;;
    process)        run_all_processing ;;
    
    # Full Pipeline
    all)            run_full_pipeline ;;
    
    # Monitoring
    status)         show_status ;;
    logs)           show_logs "$2" ;;
    processes)      check_processes ;;
    zombies)        check_zombies ;;
    
    # Debug
    interactive)    run_interactive ;;
    
    # Help
    help|--help|-h) show_help ;;
    
    *)
        log_error "Unknown command: $1"
        show_help
        exit 1
        ;;
esac