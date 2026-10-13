import argparse
import hashlib
import json
import sys
from dataclasses import asdict
from pathlib import Path

if __package__ in {None, ''}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from function2.bayrate import EventReplayBatch, BayrateConfig, run_bayrate_loaded
from function2.bayrate_sql import (
    DEFAULT_DDL_PATH,
    complete_run,
    connect_sql,
    ensure_schema,
    fail_run,
    get_cutover_control,
    get_replay_boundary,
    load_checkpoint_before_event,
    load_connection_string,
    load_event_boundary,
    load_event_ordinal_lookup,
    load_snapshot_seed_before_event,
    load_games_from_sql,
    load_official_history_from_sql,
    mark_event_computed,
    publish_event_batch,
    publish_run_metrics,
    purge_from_event,
    refresh_event_index,
    start_run,
)

MODEL_LABEL = 'bayrate_best'
BEST_CONFIG = BayrateConfig(
    posterior_sigma_multiplier=1.1,
    inactivity_growth_per_day=0.0005,
    performance_trigger_threshold=1.0,
    performance_trigger_sigma_scale=0.25,
    performance_trigger_sigma_cap=1.5,
    performance_trigger_low_sigma_reference=0.35,
    performance_trigger_low_sigma_power=0.15,
    performance_trigger_low_sigma_rating_taper_start=5.0,
    performance_trigger_low_sigma_rating_taper_end=7.0,
)


def compute_config_hash(config: BayrateConfig) -> bytes:
    payload = json.dumps(asdict(config), sort_keys=True, default=str).encode('utf-8')
    return hashlib.sha256(payload).digest()


def main() -> None:
    parser = argparse.ArgumentParser(description='Run incremental Bayrate replay against Azure SQL sources.')
    parser.add_argument('--model-label', default=MODEL_LABEL)
    parser.add_argument('--ensure-schema', action='store_true')
    parser.add_argument('--ddl-path', type=Path, default=DEFAULT_DDL_PATH)
    args = parser.parse_args()

    conn = connect_sql(load_connection_string())
    run_id: int | None = None
    try:
        if args.ensure_schema:
            ensure_schema(conn, args.ddl_path)
        refresh_event_index(conn, args.model_label)
        boundary = get_replay_boundary(conn, args.model_label)
        if boundary is None:
            print(json.dumps({'status': 'noop', 'reason': 'no_dirty_events'}, indent=2))
            return

        effective_boundary = dict(boundary)
        seed_mode = 'checkpoint'
        cutover = get_cutover_control(conn, args.model_label)
        if cutover and cutover.get('CutoverEventOrdinal') is not None:
            cutover_ordinal = int(cutover['CutoverEventOrdinal'])
            if int(effective_boundary['EventOrdinal']) < cutover_ordinal:
                cutover_boundary = load_event_boundary(conn, args.model_label, cutover_ordinal)
                if cutover_boundary is None:
                    raise RuntimeError(f'Cutover event ordinal {cutover_ordinal} not found for model label {args.model_label}.')
                effective_boundary = cutover_boundary
            if int(effective_boundary['EventOrdinal']) == cutover_ordinal and bool(cutover.get('SnapshotSeedEnabled')):
                seed_mode = 'snapshot'

        run_id = start_run(conn, args.model_label, compute_config_hash(BEST_CONFIG), None, effective_boundary)
        if seed_mode == 'snapshot':
            td_list, momentum_streaks = load_snapshot_seed_before_event(conn, args.model_label, int(effective_boundary['EventOrdinal']))
        else:
            td_list, momentum_streaks = load_checkpoint_before_event(conn, args.model_label, int(effective_boundary['EventOrdinal']))
        purge_from_event(conn, args.model_label, int(effective_boundary['EventOrdinal']))
        games = load_games_from_sql(conn, BEST_CONFIG)
        history = load_official_history_from_sql(conn)
        event_index_lookup = load_event_ordinal_lookup(conn, args.model_label)

        def sink(batch: EventReplayBatch) -> None:
            publish_event_batch(conn, run_id, args.model_label, batch)
            mark_event_computed(conn, args.model_label, batch.event_ordinal, run_id)

        result = run_bayrate_loaded(
            games,
            history,
            BEST_CONFIG,
            initial_td_list=td_list,
            initial_momentum_streaks=momentum_streaks,
            start_event_ordinal=int(effective_boundary['EventOrdinal']),
            event_index_lookup=event_index_lookup,
            event_sink=sink,
        )
        publish_run_metrics(conn, run_id, result)
        complete_run(conn, run_id, result)
        print(json.dumps({'status': 'completed', 'run_id': run_id, 'model_label': args.model_label, 'replay_from_event_ordinal': int(effective_boundary['EventOrdinal']), 'replayed_event_count': result.event_count, 'replayed_game_count': len(result.game_results), 'player_count': result.player_count, 'seed_mode': seed_mode}, indent=2))
    except Exception as exc:
        if run_id is not None:
            fail_run(conn, run_id, str(exc))
        raise
    finally:
        conn.close()


if __name__ == '__main__':
    main()
