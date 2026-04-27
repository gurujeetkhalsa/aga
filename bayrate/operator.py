import argparse
import sys
from pathlib import Path
from typing import Any, Callable, TextIO

from bayrate.replay_staged_run import print_replay_summary, run_staged_replay
from bayrate.sql_adapter import SqlAdapter, get_sql_connection_string
from bayrate.stage_reports import (
    StageSqlAdapter,
    apply_tournament_review_decision,
    build_insert_statements,
    build_staging_payload,
    ensure_payload_run_id,
    explain_staged_run_review,
    load_staged_run,
    refresh_payload_summary,
    update_staged_run_review,
)


InputFunc = Callable[[], str]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Interactive BayRate report staging operator.")
    parser.add_argument("inputs", nargs="*", type=Path, help="Report text files to stage.")
    parser.add_argument("--run-id", help="Review and update an existing staged BayRate RunID.")
    parser.add_argument("--explain-only", action="store_true", help="Print review explanation and exit without prompts or writes.")
    parser.add_argument("--replay-only", action="store_true", help="Run a read-only BayRate replay for --run-id and exit.")
    parser.add_argument("--replay-output", type=Path, help="JSON artifact path for --replay-only.")
    parser.add_argument("--connection-string", help="SQL connection string. Defaults to SQL_CONNECTION_STRING/local.settings.json.")
    parser.add_argument("--skip-duplicate-checks", action="store_true", help="Skip production duplicate checks.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn_str = args.connection_string or get_sql_connection_string()
    if not conn_str:
        print("Missing SQL connection string. Set SQL_CONNECTION_STRING or pass --connection-string.", file=sys.stderr)
        raise SystemExit(1)
    try:
        adapter = SqlAdapter(conn_str)
        if args.replay_only:
            if args.run_id and args.inputs:
                raise ValueError("Pass --replay-only with either --run-id or report files, not both.")
            if args.run_id:
                payload = run_existing_run_replay(args.run_id, adapter=adapter, output_path=args.replay_output)
            elif args.inputs:
                payload = run_input_replay(
                    args.inputs,
                    adapter=adapter,
                    duplicate_check=not args.skip_duplicate_checks,
                    output_path=args.replay_output,
                )
            else:
                raise ValueError("--replay-only requires --run-id or report files.")
        elif args.run_id:
            if args.inputs:
                raise ValueError("Pass either --run-id or report files, not both.")
            payload = run_existing_run_review(args.run_id, adapter=adapter, explain_only=args.explain_only)
        else:
            if not args.inputs:
                raise ValueError("Pass report files to stage, or --run-id to review an existing staged run.")
            payload = run_operator(
                args.inputs,
                adapter=adapter,
                duplicate_check=not args.skip_duplicate_checks,
                explain_only=args.explain_only,
            )
    except KeyboardInterrupt:
        print("\nCancelled.")
        raise SystemExit(130)
    except Exception as exc:
        print(f"Operator staging failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
    if args.run_id:
        if payload.get("updated"):
            print(f"\nUpdated RunID: {payload['run_id']}")
    elif payload.get("written"):
        print(f"\nStaged RunID: {payload['run_id']}")


def run_operator(
    input_paths: list[Path],
    *,
    adapter: StageSqlAdapter,
    duplicate_check: bool = True,
    explain_only: bool = False,
    input_func: InputFunc | None = None,
    output: TextIO | None = None,
    run_id: int | str | None = None,
) -> dict[str, Any]:
    out = output or sys.stdout
    read_answer = input_func or _read_stdin_line
    reports = [(str(path), path.read_text(encoding="utf-8")) for path in input_paths]
    payload = build_staging_payload(
        reports,
        adapter=adapter,
        duplicate_check=duplicate_check,
        run_id=run_id,
    )
    payload["written"] = False
    payload["dry_run"] = True

    print_report_summary(payload, out)
    explanations = explain_staged_run_review(adapter, payload)
    print_review_explanations(explanations, out)
    if explain_only:
        print("Explain-only mode: no SQL rows were written.", file=out)
        return payload

    review_tournaments(payload, read_answer, out, explanations=explanations)
    print_report_summary(payload, out, title="After Review")

    if payload["validation_failed_count"]:
        write_prompt = "Write this validation_failed run to staging for audit?"
    elif payload["needs_review_count"]:
        write_prompt = "Write this run to staging even though it still needs review?"
    else:
        write_prompt = "Write this staged run to SQL now?"
    if not ask_yes_no(write_prompt, default=False, input_func=read_answer, output=out):
        print("No SQL rows were written.", file=out)
        return payload

    ensure_payload_run_id(payload, adapter)
    adapter.execute_statements(build_insert_statements(payload))
    payload["written"] = True
    payload["dry_run"] = False
    print(f"Wrote staged run {payload['run_id']} with status {payload['status']}.", file=out)
    return payload


def run_existing_run_review(
    run_id: int | str,
    *,
    adapter: StageSqlAdapter,
    explain_only: bool = False,
    input_func: InputFunc | None = None,
    output: TextIO | None = None,
) -> dict[str, Any]:
    out = output or sys.stdout
    read_answer = input_func or _read_stdin_line
    payload = load_staged_run(adapter, run_id)
    payload["written"] = True
    payload["dry_run"] = False

    print_report_summary(payload, out, title="Existing Staged Run")
    explanations = explain_staged_run_review(adapter, payload)
    print_review_explanations(explanations, out)
    if explain_only:
        print("Explain-only mode: no SQL rows were changed.", file=out)
        payload["updated"] = False
        return payload

    review_tournaments(payload, read_answer, out, explanations=explanations)
    print_report_summary(payload, out, title="After Review")

    if not ask_yes_no("Save these review changes to SQL?", default=False, input_func=read_answer, output=out):
        print("No SQL rows were changed.", file=out)
        payload["updated"] = False
        return payload

    update_staged_run_review(adapter, payload)
    payload["updated"] = True
    print(f"Updated staged run {payload['run_id']} with status {payload['status']}.", file=out)
    return payload


def run_existing_run_replay(
    run_id: int | str,
    *,
    adapter: StageSqlAdapter,
    output_path: Path | None = None,
    output: TextIO | None = None,
) -> dict[str, Any]:
    out = output or sys.stdout
    artifact = run_staged_replay(
        adapter,
        run_id=str(run_id),
        output_path=output_path,
        allow_needs_review=True,
    )
    print_replay_summary(artifact, out)
    return artifact


def run_input_replay(
    input_paths: list[Path],
    *,
    adapter: StageSqlAdapter,
    duplicate_check: bool = True,
    output_path: Path | None = None,
    output: TextIO | None = None,
    run_id: int | str | None = None,
) -> dict[str, Any]:
    out = output or sys.stdout
    reports = [(str(path), path.read_text(encoding="utf-8")) for path in input_paths]
    payload = build_staging_payload(
        reports,
        adapter=adapter,
        duplicate_check=duplicate_check,
        run_id=run_id,
    )
    payload["written"] = False
    payload["dry_run"] = True

    print_report_summary(payload, out, title="Replay Dry Run Staging Summary")
    explanations = explain_staged_run_review(adapter, payload)
    print_review_explanations(explanations, out)
    artifact = run_staged_replay(
        adapter,
        payload=payload,
        output_path=output_path,
        allow_needs_review=True,
    )
    print_replay_summary(artifact, out)
    return artifact


def review_tournaments(
    payload: dict[str, Any],
    input_func: InputFunc,
    output: TextIO,
    *,
    explanations: dict[str, Any] | None = None,
) -> None:
    explanation_by_ordinal = {
        explanation.get("source_report_ordinal"): explanation
        for explanation in (explanations or {}).get("tournaments", [])
    }
    for tournament in payload.get("staged_tournaments") or []:
        status = tournament.get("status")
        ordinal = int(tournament["source_report_ordinal"])
        if status == "validation_failed":
            print_validation_errors(tournament, output)
            continue
        if status == "ready_for_rating":
            print(
                f"Report {ordinal}: {tournament['tournament_row'].get('Tournament_Code')} is ready_for_rating.",
                file=output,
            )
            continue
        if status == "needs_review" and tournament.get("duplicate_candidate"):
            review_duplicate_tournament(
                payload,
                tournament,
                input_func,
                output,
                explanation=explanation_by_ordinal.get(ordinal),
            )
            continue
        if status in {"staged", "needs_review"}:
            code = tournament["tournament_row"].get("Tournament_Code")
            mark_ready = ask_yes_no(
                f"Report {ordinal}: mark tournament {code} ready_for_rating?",
                default=False,
                input_func=input_func,
                output=output,
            )
            if mark_ready:
                note = ask_optional_text(
                    "Optional approval note",
                    input_func=input_func,
                    output=output,
                )
                apply_tournament_review_decision(payload, ordinal, mark_ready=True, operator_note=note)
    refresh_payload_summary(payload)


def review_duplicate_tournament(
    payload: dict[str, Any],
    tournament: dict[str, Any],
    input_func: InputFunc,
    output: TextIO,
    *,
    explanation: dict[str, Any] | None = None,
) -> None:
    ordinal = int(tournament["source_report_ordinal"])
    row = tournament["tournament_row"]
    duplicate = tournament["duplicate_candidate"]
    print("", file=output)
    print(f"Report {ordinal} needs review:", file=output)
    print(f"  Staged code: {row.get('Tournament_Code')}", file=output)
    print(f"  Title: {row.get('Tournament_Descr')}", file=output)
    print(f"  Date: {row.get('Tournament_Date')}", file=output)
    print(f"  Likely duplicate: {duplicate.get('tournament_code')}", file=output)
    print(f"  Production title: {duplicate.get('tournament_descr')}", file=output)
    print(f"  Duplicate score: {duplicate.get('score')}", file=output)
    score_parts = duplicate.get("score_parts") or {}
    if score_parts:
        print(
            "  Score parts: "
            + ", ".join(f"{key}={value:.3f}" for key, value in score_parts.items() if isinstance(value, (int, float))),
            file=output,
        )
    if explanation:
        print_review_explanation(explanation, output, indent="  ")

    use_duplicate = ask_yes_no(
        f"Use production Tournament_Code {duplicate.get('tournament_code')} for this staged tournament?",
        default=True,
        input_func=input_func,
        output=output,
    )
    if use_duplicate:
        mark_ready = ask_yes_no(
            "Mark it ready_for_rating after reusing that code?",
            default=True,
            input_func=input_func,
            output=output,
        )
        note = None
        if mark_ready:
            note = ask_optional_text(
                "Optional approval note",
                input_func=input_func,
                output=output,
            )
        apply_tournament_review_decision(
            payload,
            ordinal,
            use_duplicate_code=True,
            mark_ready=mark_ready,
            operator_note=note,
        )
        return

    mark_ready = ask_yes_no(
        f"Keep staged code {row.get('Tournament_Code')} and mark this as a new ready_for_rating tournament?",
        default=False,
        input_func=input_func,
        output=output,
    )
    note = None
    if mark_ready:
        note = ask_optional_text(
            "Optional approval note",
            input_func=input_func,
            output=output,
        )
    apply_tournament_review_decision(
        payload,
        ordinal,
        use_duplicate_code=False,
        mark_ready=mark_ready,
        operator_note=note,
    )


def print_report_summary(payload: dict[str, Any], output: TextIO, *, title: str = "Dry Run Summary") -> None:
    print("", file=output)
    print(title, file=output)
    print(f"  RunID: {payload['run_id']}", file=output)
    print(f"  Status: {payload['status']}", file=output)
    print(f"  Reports: {payload['source_report_count']}", file=output)
    print(f"  Tournaments: {payload['tournament_count']}", file=output)
    print(f"  Games: {payload['game_count']}", file=output)
    print(f"  Ready: {payload['ready_tournament_count']}", file=output)
    print(f"  Needs review: {payload['needs_review_count']}", file=output)
    print(f"  Validation failed: {payload['validation_failed_count']}", file=output)
    for tournament in payload.get("staged_tournaments") or []:
        row = tournament["tournament_row"]
        print(
            f"  - report {tournament['source_report_ordinal']}: "
            f"{row.get('Tournament_Code')} | {row.get('Tournament_Date')} | {tournament.get('status')}",
            file=output,
        )
        warnings = tournament.get("parser_warnings") or []
        highlighted = [warning for warning in warnings if warning.get("highlight") or warning.get("severity") == "highlight"]
        if warnings:
            suffix = f", {len(highlighted)} highlighted" if highlighted else ""
            print(f"    warnings: {len(warnings)}{suffix}", file=output)
            for warning in warnings[:6]:
                marker = "!" if warning.get("highlight") or warning.get("severity") == "highlight" else "-"
                print(f"      {marker} {warning.get('message') or warning.get('type')}", file=output)
            if len(warnings) > 6:
                print(f"      ... {len(warnings) - 6} more warning(s)", file=output)


def print_review_explanations(explanations: dict[str, Any], output: TextIO) -> None:
    tournaments = explanations.get("tournaments") or []
    if not tournaments:
        return
    print("", file=output)
    print("Review Explanation", file=output)
    for explanation in tournaments:
        print_review_explanation(explanation, output, indent="  ")


def print_review_explanation(explanation: dict[str, Any], output: TextIO, *, indent: str = "") -> None:
    duplicate = explanation.get("duplicate_candidate") or {}
    game_diff = explanation.get("game_diff") or {}
    same_date_order = explanation.get("same_date_order") or []
    ordinal = explanation.get("source_report_ordinal")
    print(f"{indent}Report {ordinal}: {explanation.get('staged_code')} | {explanation.get('status')}", file=output)
    if duplicate.get("tournament_code"):
        print(
            f"{indent}  Duplicate candidate: {duplicate.get('tournament_code')} "
            f"(score {duplicate.get('score')})",
            file=output,
        )
    if game_diff:
        print(
            f"{indent}  Game comparison: matched {game_diff.get('matched_game_count')} of "
            f"{game_diff.get('staged_game_count')} staged games against "
            f"{game_diff.get('production_game_count')} production games.",
            file=output,
        )
        if game_diff.get("staged_only_count") or game_diff.get("production_only_count"):
            print(
                f"{indent}  Mismatches: staged-only {game_diff.get('staged_only_count')}, "
                f"production-only {game_diff.get('production_only_count')}.",
                file=output,
            )
            for row in (game_diff.get("staged_only") or [])[:8]:
                print(f"{indent}    staged-only {format_review_game(row)}", file=output)
            for row in (game_diff.get("production_only") or [])[:8]:
                print(f"{indent}    production-only {format_review_game(row)}", file=output)
            if (game_diff.get("staged_only_count") or 0) + (game_diff.get("production_only_count") or 0) > 16:
                print(f"{indent}    ... more mismatches omitted", file=output)
    if same_date_order:
        print(f"{indent}  Same-date production tournament order:", file=output)
        for item in same_date_order:
            marker = ""
            if item.get("is_duplicate_candidate"):
                marker = " <- duplicate candidate"
            elif item.get("is_staged_code"):
                marker = " <- staged code already in production"
            print(
                f"{indent}    {item.get('order')}. {item.get('tournament_code')} "
                f"ratings={item.get('first_rating_row_id')}..{item.get('last_rating_row_id')} "
                f"games={item.get('game_count')}{marker}",
                file=output,
            )


def format_review_game(row: dict[str, Any]) -> str:
    source = f"#{row['source_game_ordinal']}" if row.get("source_game_ordinal") else f"Game_ID {row.get('game_id')}"
    return (
        f"{source}: r{row.get('round')} {row.get('pin_player_1')}-{row.get('pin_player_2')} "
        f"H{row.get('handicap')} K{row.get('komi')} result={row.get('result')}"
    )


def print_validation_errors(tournament: dict[str, Any], output: TextIO) -> None:
    row = tournament["tournament_row"]
    print("", file=output)
    print(f"Report {tournament['source_report_ordinal']} has validation errors for {row.get('Tournament_Code')}:", file=output)
    for error in tournament.get("validation_errors") or []:
        print(f"  - {error}", file=output)


def ask_yes_no(
    question: str,
    *,
    default: bool,
    input_func: InputFunc,
    output: TextIO,
) -> bool:
    suffix = "[Y/n]" if default else "[y/N]"
    while True:
        print(f"{question} {suffix} ", end="", file=output)
        output.flush()
        answer = input_func().strip().lower()
        if not answer:
            return default
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no"}:
            return False
        print("Please answer yes or no.", file=output)


def ask_optional_text(question: str, *, input_func: InputFunc, output: TextIO) -> str | None:
    print(f"{question} (press Enter to skip): ", end="", file=output)
    output.flush()
    answer = input_func().strip()
    return answer or None


def _read_stdin_line() -> str:
    return sys.stdin.readline()


if __name__ == "__main__":
    main()
