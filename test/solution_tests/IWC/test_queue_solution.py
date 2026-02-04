from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])

def test_rule_of_3() -> None:
    run_queue([
        call_enqueue(provider="companies_house", user_id=2, timestamp=iso_ts(delta_minutes=0)).expect(1),
        call_enqueue(provider="companies_house", user_id=1, timestamp=iso_ts(delta_minutes=1)).expect(2),
        call_enqueue(provider="id_verification", user_id=1, timestamp=iso_ts(delta_minutes=2)).expect(3),
        call_enqueue(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=3)).expect(4),
        call_size().expect(4),
        call_dequeue().expect("companies_house", 1),
    ])

def test_timestamp_ordering() -> None:
    run_queue([
        call_enqueue(provider="companies_house", user_id=1, timestamp=iso_ts(delta_minutes=2)).expect(1),
        call_enqueue(provider="id_verification", user_id=1, timestamp=iso_ts(delta_minutes=1)).expect(2),
        call_enqueue(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=0)).expect(3),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 1),
    ])

def test_dependency_resolution() -> None:
    run_queue([
        call_enqueue(provider="credit_check", user_id=1, timestamp=iso_ts(delta_minutes=0)).expect(2),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("credit_check", 1),
    ])

def test_deduplication() -> None:
    run_queue([
        call_enqueue(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=0)).expect(1),
        call_enqueue(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=5)).expect(1),
        call_enqueue(provider="id_verification", user_id=1, timestamp=iso_ts(delta_minutes=5)).expect(2),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 1),
    ])


def test_bank_statements_with_rule_of_3() -> None:
    run_queue([
        call_enqueue(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=0)).expect(1),
        call_enqueue(provider="id_verification", user_id=1, timestamp=iso_ts(delta_minutes=5)).expect(2),
        call_enqueue(provider="companies_house", user_id=1, timestamp=iso_ts(delta_minutes=5)).expect(3),
        call_enqueue(provider="id_verification", user_id=2, timestamp=iso_ts(delta_minutes=2)).expect(4),
        call_enqueue(provider="bank_statements", user_id=2, timestamp=iso_ts(delta_minutes=2)).expect(5),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 2),
        call_dequeue().expect("bank_statements", 2),
    ])


def test_bank_statements_without_rule_of_3() -> None:
    run_queue([
        call_enqueue(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=0)).expect(1),
        call_enqueue(provider="id_verification", user_id=1, timestamp=iso_ts(delta_minutes=5)).expect(2),
        call_enqueue(provider="companies_house", user_id=1, timestamp=iso_ts(delta_minutes=5)).expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_age() -> None:
    run_queue([
        call_enqueue(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=0)).expect(1),
        call_enqueue(provider="id_verification", user_id=1, timestamp=iso_ts(delta_minutes=5)).expect(2),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 1),
    ])


def test_bank_statement_skip() -> None:
    run_queue([
        call_enqueue(provider="id_verification", user_id=1, timestamp=iso_ts(delta_minutes=0)).expect(1),
        call_enqueue(provider="bank_statements", user_id=2, timestamp=iso_ts(delta_minutes=1)).expect(2),
        call_enqueue(provider="companies_house", user_id=3, timestamp=iso_ts(delta_minutes=7)).expect(3),
        call_dequeue().expect("id_verification", 1),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("companies_house", 3),
    ])

def test_bank_statement_skip_2() -> None:
    run_queue([
        call_enqueue(provider="companies_house", user_id=1, timestamp=iso_ts(delta_minutes=0)).expect(1),
        call_enqueue(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=0)).expect(2),
        call_enqueue(provider="id_verification", user_id=6, timestamp=iso_ts(delta_minutes=6)).expect(3),
        call_dequeue().expect("companies_house", 1),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("id_verification", 6),
    ])

def test_bank_statement_skip_3() -> None:
    run_queue([
        call_enqueue(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=0)).expect(1),
        call_enqueue(provider="bank_statements", user_id=2, timestamp=iso_ts(delta_minutes=0)).expect(2),
        call_enqueue(provider="companies_house", user_id=6, timestamp=iso_ts(delta_minutes=1)).expect(3),
        call_enqueue(provider="id_verification", user_id=2, timestamp=iso_ts(delta_minutes=7)).expect(4),
        call_dequeue().expect("bank_statements", 2),
        call_dequeue().expect("bank_statements", 1),
        call_dequeue().expect("companies_house", 2),
        call_dequeue().expect("id_verification", 2),
    ])
