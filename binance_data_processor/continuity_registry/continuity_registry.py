from datetime import datetime, timedelta
import csv
import os
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

from binance_data_processor.continuity_registry.continuity_entry import ContinuityEntry
from binance_data_processor.enums.continuity_event_type import ContinuityEventType


class ContinuityRegister:

    __slots__ = [
        'continuity_entry_list'
    ]

    def __init__(self):
        self.continuity_entry_list = []

    def add_continuity_entry(self, continuity_entry: ContinuityEntry) -> None:
        self.continuity_entry_list.append(continuity_entry)

    def dump_to_csv(self) -> None:
        file_path = os.path.join(os.path.expanduser('~'), 'Documents/ContinuityChangelog.csv')
        with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['timestamp', 'instance_number', 'event_type', 'bucket_name', 'comment'])
            for entry in self.continuity_entry_list:
                writer.writerow([
                    entry.timestamp,
                    entry.instance_numer,
                    entry.event_type.value,
                    entry.comment
                ])

    def load_from_csv(self) -> None:
        file_path = os.path.join(os.path.expanduser('~'), 'Documents/ContinuityChangelog.csv')
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                event_type = ContinuityEventType(row['event_type'])
                instance_number = int(row['instance_number'])
                timestamp   = row['timestamp']
                comment     = row.get('comment', '') or ''
                entry = ContinuityEntry(
                    timestamp=timestamp,
                    event_type=event_type,
                    instance_numer=instance_number,
                    comment=comment
                )
                self.add_continuity_entry(entry)

    def plot_timeline(self) -> None:

        # 1. Sortowanie wpisów chronologicznie
        entries = sorted(
            self.continuity_entry_list,
            key=lambda e: e.timestamp.lstrip('~')
        )

        # 2. Przygotowanie wykresu
        fig, ax = plt.subplots(figsize=(10, 4))

        # 3. Rysowanie START/STOP i błędów
        starts = {}
        for entry in entries:
            ts = datetime.strptime(entry.timestamp.lstrip('~'),
                                   '%Y-%m-%dT%H:%M:%S.%fZ')
            ts_num = mdates.date2num(ts)
            inst = entry.instance_numer

            if entry.event_type == ContinuityEventType.START:
                starts[inst] = ts_num

            elif entry.event_type == ContinuityEventType.STOP:
                if inst in starts:
                    start_num = starts.pop(inst)
                    ax.barh(
                        inst,
                        ts_num - start_num,
                        left=start_num,
                        height=0.4,
                        zorder=1
                    )

            elif entry.event_type == ContinuityEventType.ERROR_CONTINUITY_LOST:
                ax.plot(
                    ts_num,
                    inst,
                    marker='x',
                    markersize=8,
                    color='black',
                    zorder=2
                )

        # 4. Dorysowanie otwartych okresów (START bez STOP) aż do teraz
        if starts:
            now_num = mdates.date2num(datetime.utcnow())
            for inst, start_num in starts.items():
                ax.barh(
                    inst,
                    now_num - start_num,
                    left=start_num,
                    height=0.4,
                    zorder=1
                )

        # 5. Formatowanie osi
        ys = sorted({e.instance_numer for e in entries})
        ax.set_yticks(ys)
        ax.set_ylabel('Instance Number')
        ax.set_xlabel('Date')
        ax.xaxis_date()
        ax.xaxis.set_major_locator(mdates.DayLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        fig.autofmt_xdate()

        # 6. Pionowe linie na północy – rysowane NA WIERZCHU (zorder=3)
        x0, x1 = ax.get_xlim()
        date0 = mdates.num2date(x0).date()
        date1 = mdates.num2date(x1).date()
        current = datetime.combine(date0, datetime.min.time())
        while current.date() <= date1:
            ax.axvline(
                mdates.date2num(current),
                color='black',
                linewidth=0.5,
                linestyle='--',
                zorder=3
            )
            current += timedelta(days=1)

        plt.tight_layout()
        plt.show()
