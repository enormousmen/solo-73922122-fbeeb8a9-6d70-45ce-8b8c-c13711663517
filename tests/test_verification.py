"""核验判定纯逻辑单测：不依赖数据库与 FastAPI。"""
from __future__ import annotations

import unittest

from app.verification import (
    VerifyStatus,
    judge_verification,
    lsn_int,
    wal_segment_name,
)


def standby(receive="0/2000", replay="0/1000", paused=False, streaming=True,
            in_recovery=True):
    return {
        "in_recovery": in_recovery,
        "receive_lsn": receive,
        "replay_lsn": replay,
        "replay_paused": paused,
        "streaming": streaming,
    }


class LsnHelperTest(unittest.TestCase):
    def test_lsn_int_ordering(self) -> None:
        self.assertLess(lsn_int("0/1"), lsn_int("0/2"))
        self.assertLess(lsn_int("0/FFFFFFFF"), lsn_int("1/0"))
        self.assertEqual(lsn_int("1/0"), 1 << 32)

    def test_wal_segment_name(self) -> None:
        # 16MB 边界：0/0 -> 000000010000000000000001（段号从 1 开始的常用推算）
        self.assertEqual(wal_segment_name("0/0"), "000000010000000000000000")
        self.assertEqual(
            wal_segment_name("0/FFFFFF"), "000000010000000000000000"
        )
        self.assertEqual(
            wal_segment_name("0/1000000"), "000000010000000000000001"
        )


class JudgeVerificationTest(unittest.TestCase):
    TICKET = "MH20261123000001"
    BOUND = "0/1500"

    def test_confirmed_when_receive_covers_bound(self) -> None:
        result = judge_verification(self.TICKET, self.BOUND, standby(receive="0/1600"))
        self.assertIs(result.status, VerifyStatus.CONFIRMED)
        self.assertTrue(result.to_dict()["confirmed"])

    def test_confirmed_with_equal_lsn(self) -> None:
        result = judge_verification(self.TICKET, self.BOUND, standby(receive=self.BOUND))
        self.assertIs(result.status, VerifyStatus.CONFIRMED)

    def test_confirmed_even_when_replay_paused(self) -> None:
        # 回放暂停：落盘（receive）覆盖即可确认，提示里说明原因。
        result = judge_verification(
            self.TICKET, self.BOUND,
            standby(receive="0/2000", replay="0/10", paused=True),
        )
        self.assertIs(result.status, VerifyStatus.CONFIRMED)
        self.assertTrue(result.replay_paused)
        self.assertIn("回放已暂停", result.message)

    def test_confirmed_even_when_stream_disconnected(self) -> None:
        # 断流：WAL 接收进程不在，但此前已落盘的位置不动，仍确认。
        result = judge_verification(
            self.TICKET, self.BOUND,
            standby(receive="0/2000", streaming=False),
        )
        self.assertIs(result.status, VerifyStatus.CONFIRMED)
        self.assertFalse(result.streaming)
        self.assertIn("断流", result.message)

    def test_behind_when_receive_below_bound(self) -> None:
        result = judge_verification(
            self.TICKET, self.BOUND, standby(receive="0/1400")
        )
        self.assertIs(result.status, VerifyStatus.BEHIND)
        self.assertFalse(result.to_dict()["confirmed"])

    def test_behind_when_receive_null(self) -> None:
        result = judge_verification(
            self.TICKET, self.BOUND, standby(receive=None)
        )
        self.assertIs(result.status, VerifyStatus.BEHIND)

    def test_unreachable_when_snapshot_none(self) -> None:
        result = judge_verification(self.TICKET, self.BOUND, None)
        self.assertIs(result.status, VerifyStatus.UNREACHABLE)

    def test_promoted_when_not_in_recovery(self) -> None:
        result = judge_verification(
            self.TICKET, self.BOUND, standby(in_recovery=False)
        )
        self.assertIs(result.status, VerifyStatus.PROMOTED)

    def test_ticket_not_found(self) -> None:
        result = judge_verification("NOPE", None, standby())
        self.assertIs(result.status, VerifyStatus.TICKET_NOT_FOUND)

    def test_replay_lag_irrelevant_if_receive_covers(self) -> None:
        # 回放位置落后甚至为 0，只要 receive 覆盖绑定位置就确认。
        result = judge_verification(
            self.TICKET, self.BOUND,
            standby(receive="0/3000", replay="0/0", paused=True),
        )
        self.assertIs(result.status, VerifyStatus.CONFIRMED)


if __name__ == "__main__":
    unittest.main()
