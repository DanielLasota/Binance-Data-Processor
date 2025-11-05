from binance_data_processor import DataSinkContinuityRegistry
from binance_data_processor.continuity_registry.continuity_entry import ContinuityEntry
from binance_data_processor.enums.continuity_event_type import ContinuityEventType

if __name__ == '__main__':
    continuity_register = DataSinkContinuityRegistry()

    continuity_register.add_continuity_entry(
        ContinuityEntry(
            timestamp="2025-05-27T15:00:00.000Z",
            instance_numer=2,
            event_type=ContinuityEventType.START,
            comment=""
        )
    )
    continuity_register.add_continuity_entry(
        ContinuityEntry(
            timestamp="2025-07-08T20:43:47.667Z",
            instance_numer=2,
            event_type=ContinuityEventType.STOP,
            comment=""
        )
    )
    continuity_register.add_continuity_entry(
        ContinuityEntry(
            timestamp="2025-07-12T09:48:48.291Z",
            instance_numer=2,
            event_type=ContinuityEventType.START,
            comment=""
        )
    )
    continuity_register.add_continuity_entry(
        ContinuityEntry(
            timestamp="2025-08-22T14:00:00.000Z",
            instance_numer=2,
            event_type=ContinuityEventType.STOP,
            comment="Unexpected Crash"
        )
    )
    continuity_register.add_continuity_entry(
        ContinuityEntry(
            timestamp="2025-08-25T22:20:00.000Z",
            instance_numer=2,
            event_type=ContinuityEventType.START,
            comment="Start after Unexpected Crash"
        )
    )
    # continuity_register.load_from_csv()

    for entry in continuity_register.continuity_entry_list:
        if entry.instance_numer == 2:
            print(entry)

    continuity_register.plot_timeline()
