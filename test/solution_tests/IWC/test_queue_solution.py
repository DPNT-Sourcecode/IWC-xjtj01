from __future__ import annotations

from .utils import call_dequeue, call_enqueue, call_size, iso_ts, run_queue


def test_enqueue_size_dequeue_flow() -> None:
    run_queue([
        call_enqueue("companies_house", 1, iso_ts(delta_minutes=0)).expect(1),
        call_size().expect(1),
        call_dequeue().expect("companies_house", 1),
    ])

def test_enque_rule_of_3() -> None:
    run_queue([
        call_enqueue(provider="companies_house", user_id=2, timestamp=iso_ts(delta_minutes=0)).expect(1),
        call_enqueue(provider="companies_house", user_id=1, timestamp=iso_ts(delta_minutes=1)).expect(2),
        call_enqueue(provider="id_provider", user_id=1, timestamp=iso_ts(delta_minutes=2)).expect(3),
        call_enqueue(provider="bank_statements", user_id=1, timestamp=iso_ts(delta_minutes=3)).expect(4),
        call_size().expect(4),
        call_dequeue().expect("companies_house", 1),
    ])

