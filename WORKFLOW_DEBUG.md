# Noiz Workflow Testing Guide

Quick reference for testing the full noiz processing pipeline.

## Quick Start

```bash
# Full pipeline (deploy + load data + process including beamforming)
./workflow_runner.sh all

# Check status
./workflow_runner.sh status

# Cleanup everything (removes containers and data)
./workflow_runner.sh cleanup
```

## Step-by-Step

### 1. Deploy

```bash
./workflow_runner.sh deploy
```

### 2. Load Data

```bash
./workflow_runner.sh load_data
```

### 3. Run Processing

```bash
# Runs: prepare → qcone → ppsd → process_chunks → crosscorr → qctwo → stacking → beamforming
./workflow_runner.sh process
```

## Individual Commands

### Deployment

| Stage | Command | Description |
|-------|---------|-------------|
| Fresh deploy | `./workflow_runner.sh deploy` | Removes existing data and starts fresh |
| Restart | `./workflow_runner.sh restart` | Restart containers (preserves data) |
| Cleanup | `./workflow_runner.sh cleanup` | Stop containers and remove all data |

### Data Loading

| Stage | Command |
|-------|---------|
| Load all data | `./workflow_runner.sh load_data` |
| Load inventory | `./workflow_runner.sh load_inventory` |
| Load seismic data | `./workflow_runner.sh load_seismic` |
| Add timespans | `./workflow_runner.sh add_timespans` |
| Load SOH data | `./workflow_runner.sh load_soh` |

### Processing

| Stage | Command |
|-------|---------|
| All processing | `./workflow_runner.sh process` |
| Prepare datachunks | `./workflow_runner.sh prepare` |
| Run QC One | `./workflow_runner.sh qcone` |
| Run PPSD (parallel) | `./workflow_runner.sh ppsd` |
| Run PPSD (sequential) | `./workflow_runner.sh ppsd_seq` |
| Process datachunks | `./workflow_runner.sh process_chunks` |
| Run cross-correlations | `./workflow_runner.sh crosscorr` |
| Add cylindrical pairs | `./workflow_runner.sh add_cyl_pairs` |
| Run cylindrical CCF (parallel) | `./workflow_runner.sh crosscorr_cyl` |
| Run cylindrical CCF (sequential) | `./workflow_runner.sh crosscorr_cyl_seq` |
| Run QC Two | `./workflow_runner.sh qctwo` |
| Run stacking | `./workflow_runner.sh stacking` |
| Run beamforming | `./workflow_runner.sh beamforming` |

## Monitoring

```bash
# Database counts and container status
./workflow_runner.sh status

# View logs (default: ppsd_run.log)
./workflow_runner.sh logs
./workflow_runner.sh logs crosscorrelations.log
./workflow_runner.sh logs beamforming.log

# Available log files:
# - prepare_datachunks.log
# - average_soh_gps.log
# - calc_datachunk_stats.log
# - qcone.log
# - ppsd_run.log
# - process_datachunks.log
# - crosscorrelations.log
# - crosscorrelations_cylindrical.log
# - qctwo.log
# - stacking.log
# - beamforming.log

# Check for issues
./workflow_runner.sh processes
./workflow_runner.sh zombies
```

## Interactive Debug

```bash
./workflow_runner.sh interactive
```

## Configuration

Edit variables at top of `workflow_runner.sh`:

```bash
# Date range
START_DATE="2019-10-01"
END_DATE="2019-10-02"

# Network
NETWORK="EN"

# Processing mode
USE_PARALLEL="true"

# Timespan parameters
TIMESPAN_WINDOW_LENGTH="1860"   # seconds
TIMESPAN_WINDOW_OVERLAP="60"    # seconds
```

Or override via environment:

```bash
USE_PARALLEL=false ./workflow_runner.sh ppsd
START_DATE=2019-10-01 END_DATE=2019-10-03 ./workflow_runner.sh process
TIMESPAN_WINDOW_LENGTH=3600 ./workflow_runner.sh add_timespans
```

## Expected Results (1-Day Test Dataset: 2019-10-01 to 2019-10-02)

| Metric | Count |
|--------|-------|
| Stations | 11 |
| Components | 33 |
| Timespans | 48 |
| Datachunks | ~1,518 |
| QCOne Results | ~1,518 |
| PPSD Results | ~1,485 |
| Processed Datachunks | ~1,480 |
| Cross-correlations (Cartesian) | ~21,153 |
| Cross-correlations (Cylindrical) | ~18,776 |
| Component Pairs (Cylindrical) | 440 |
| QCTwo Results | varies |
| Stacking Results | varies |
| Beamforming Results | varies by frequency config |

**Note:** Results scale with date range. For full week (2019-10-01 to 2019-10-08):
- Datachunks: ~10,152
- Cross-correlations: ~137,739

## Beamforming Configuration

Default beamforming parameters (can be overridden via environment variables):

```bash
BEAMFORMING_FREQ_MIN=0.80      # Minimum frequency
BEAMFORMING_FREQ_MAX=1.4       # Maximum frequency
BEAMFORMING_FREQ_STEP=0.05     # Frequency step
BEAMFORMING_FREQ_WIDTH=0.1     # Frequency width
BEAMFORMING_PARAMS_IDS="1 10"  # Beamforming params IDs
```

Example with custom parameters:

```bash
BEAMFORMING_FREQ_MIN=0.5 BEAMFORMING_FREQ_MAX=2.0 ./workflow_runner.sh beamforming
```

## Cylindrical CCF Configuration

Cylindrical CCF computation requires cartesian CCFs to be computed first. The workflow automatically creates cylindrical component pairs if they don't exist.

Default parameters:

```bash
CYLINDRICAL_BATCH_SIZE=125     # Timespans per batch (controls memory usage)
USE_OVERWRITE=false            # Set to true to overwrite existing records
```

Examples:

```bash
# Run cylindrical CCF in sequential mode (recommended for first run/testing)
./workflow_runner.sh crosscorr_cyl_seq

# Run cylindrical CCF in parallel mode
./workflow_runner.sh crosscorr_cyl

# Run with overwrite (deletes existing records before insert)
USE_OVERWRITE=true ./workflow_runner.sh crosscorr_cyl

# Custom batch size (lower = less memory, more batches)
CYLINDRICAL_BATCH_SIZE=50 ./workflow_runner.sh crosscorr_cyl
```

**Note:** When `USE_OVERWRITE=true`, existing cylindrical CCF records matching the batch criteria are deleted before inserting new ones. This is faster than upsert for re-processing.

## Stacking Configuration

Default stacking parameters:

```bash
STACKING_COMPONENT="ZZ"        # Component code (e.g., ZZ, ZN, ZE, etc.)
STACKING_BATCH_SIZE=200        # Batch size for processing
```

Example with custom parameters:

```bash
STACKING_COMPONENT=ZN STACKING_BATCH_SIZE=100 ./workflow_runner.sh stacking
```

## Processing Pipeline Order

When running `./workflow_runner.sh all` or `./workflow_runner.sh process`, the following steps execute in order:

1. **prepare_datachunks** - Creates datachunks, averages SOH GPS, calculates stats
2. **run_qcone** - Quality control step 1
3. **run_ppsd** - Probabilistic Power Spectral Density calculation
4. **process_datachunks** - Process the datachunks
5. **run_crosscorrelations** - Compute cartesian cross-correlations between station pairs
6. **run_crosscorrelations_cylindrical** - Compute cylindrical CCFs (requires step 5)
7. **run_qctwo** - Quality control step 2 (on cross-correlations)
8. **run_stacking** - Stack cross-correlations
9. **run_beamforming** - Beamforming analysis

## Automation Notes

- All commands auto-answer "y" to prompts (no manual intervention needed)
- Logs are saved to `/processed-data-dir/logs/` inside the container
- The `cleanup` command also removes log files from `processed-data-dir-tutorial/logs/`

## Troubleshooting

```bash
# Check container status
docker compose ps

# View container logs
docker logs noiz-deployment-noiz-1

# Interactive debugging
./workflow_runner.sh interactive

# Check for zombie processes
./workflow_runner.sh zombies
```
