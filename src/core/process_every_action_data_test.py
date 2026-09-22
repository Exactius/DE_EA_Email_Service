import unittest

from src.core.process_every_action_data import repair_overflow_row

# Compact but representative EA schema. Real export has 48 columns; the anchors
# the repair relies on (Contribution ID / Date Received / Amount / the free-text
# Online Reference Number notes field) are what matter here.
COLUMNS = [
    "Contribution ID", "VANID", "Contact Name", "Date Received", "Amount",
    "Online Reference Number", "Mailing City", "Mailing State",
]


class RepairOverflowRowTest(unittest.TestCase):
    def test_recovers_row_with_single_comma_in_notes(self):
        # Notes "She gave two! 2,500.00 checks" -> the comma splits it into 2 fields,
        # giving one field too many.
        fields = [
            "20503998", "V1", "Ackerman, Frances", "2026-02-06", "2500.0000",
            "She gave two! 2", "500.00 checks", "Atlanta", "GA",
        ]
        repaired = repair_overflow_row(fields, COLUMNS)
        self.assertEqual(repaired, [
            "20503998", "V1", "Ackerman, Frances", "2026-02-06", "2500.0000",
            "She gave two! 2,500.00 checks", "Atlanta", "GA",
        ])

    def test_recovers_row_with_multiple_commas_in_notes(self):
        # Notes with two commas -> two extra fields.
        fields = [
            "20680048", "V2", "Offline", "2026-03-25", "1151.8400",
            "MAR Wire - James Case", " Jordon Black", " Christina Case", "", "",
        ]
        repaired = repair_overflow_row(fields, COLUMNS)
        self.assertEqual(repaired, [
            "20680048", "V2", "Offline", "2026-03-25", "1151.8400",
            "MAR Wire - James Case, Jordon Black, Christina Case", "", "",
        ])

    def test_returns_none_for_wellformed_row(self):
        fields = [
            "111", "V3", "Doe, John", "2026-01-01", "10.0000",
            "just a note", "Boise", "ID",
        ]
        self.assertIsNone(repair_overflow_row(fields, COLUMNS))

    def test_returns_none_when_repair_would_not_validate(self):
        # Extra field but Date Received is garbage after the merge -> the overflow
        # was not in the notes field, so we must NOT guess.
        fields = [
            "222", "V4", "Doe, John", "not-a-date", "10.0000",
            "note", "extra", "Boise", "ID",
        ]
        self.assertIsNone(repair_overflow_row(fields, COLUMNS))


if __name__ == "__main__":
    unittest.main()
