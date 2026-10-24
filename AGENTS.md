# AGENTS.md — zasady pracy dla Astry i kolejnych agentów

Repozytorium jest biblioteką Python do odbierania publicznych danych Binance, archiwizacji, pobierania archiwów do CSV, kontroli jakości i przygotowania danych do odtwarzania order booka.

## Zacznij tutaj

1. Przeczytaj podsumowanie, P0 i dowody walidacji w [PROJECT_STATE.md](PROJECT_STATE.md). Stan dotyczy lokalnego working tree, nie wdrożenia.
2. Sprawdź `git status --short`, diff roboczy i staged. Zastano rozległe zmiany użytkownika, rename'y i pliki nieśledzone; nie cofaj ich ani nie przywracaj historycznych modułów automatycznie.
3. Wybierz wiersz „Zmieniasz X — sprawdź też Y” w [MAP.md](MAP.md#zadanie). Czytaj tylko potrzebne sekcje mapy, następnie prześledź wywołujących i wywoływanych w aktualnym kodzie.
4. Rozróżniaj przepływ aktywny, zamierzony kontrakt i kod odłączony. Zakomentowana implementacja jest wskazówką historyczną; README i istnienie klasy nie dowodzą działającej integracji.
5. Dobierz walidację do zmiany. Raportuj oddzielnie kontrolę statyczną, testy lokalne i integrację z siecią; podaj ograniczenia mocków i datę wyników.

**Punkt orientacyjny na 2026-10-24:** zapis streamów jest podłączony przez osobny archiwizer; aktywne są trade, depth i snapshoty, a dla Backblaze także retry ZIP. Nadal otwarte są handshake rotacji WS, widoczność odrzuceń na wejściu i pełny lifecycle procesów. Listener oraz API wymagają osobnej integracji. Szczegóły i status napraw utrzymuj w `PROJECT_STATE.md`.

## Podział dokumentacji

| Plik | Co w nim utrzymywać |
| --- | --- |
| [AGENTS.md](AGENTS.md) | Zasady pracy, obowiązkowe kroki, pułapki i zasady walidacji |
| [MAP.md](MAP.md) | Punkty wejścia, zależności, aktywne i odłączone ścieżki, kontrakty danych |
| [PROJECT_STATE.md](PROJECT_STATE.md) | Stan funkcjonalny, P0/P1/P2, dowody, ograniczenia testów i historia audytów |

Kod jest źródłem prawdy. Po zmianie aktualizuj właściwy dokument: zależności i kontrakty w mapie, status i dowody w stanie projektu, trwałe zasady pracy tutaj. Nie kopiuj backlogu ani wyników testów do mapy. Historyczne wyniki oznaczaj datą; nie przedstawiaj ich jako nowej walidacji.

## Zasady pracy w tym repozytorium

- Zachowuj zakres bieżącego zadania. Backlog jest opisem potrzeb, nie zgodą na wykonanie wszystkich napraw.
- Nie uruchamiaj `bin/main_*.py` w ciemno. Część ładuje rzeczywiste `.env` użytkownika, pobiera duże zakresy danych, uruchamia procesy lub zapisuje poza repo. Najpierw przeczytaj konfigurację i ustal skutki w ramach bieżącego zadania.
- Nie kopiuj wartości sekretów do dokumentacji, logów ani komunikatów. Do opisów używaj nazw zmiennych środowiskowych.
- Zachowuj lekkie importy ścieżki online: `pandas`, `numpy`, `alive_progress` są importowane lokalnie w funkcjach analitycznych. `utils/sys.py` celowo wykrywa ich obecność podczas uruchamiania sinka. Nie rozwiązuj adnotacji `pd.DataFrame` przez automatyczne dodanie ciężkiego importu na poziomie modułu.
- `__init__.py` eksportuje wiele fasad, a część modułów importuje symbole z korzenia pakietu. Uważaj na cykle i kolejność importów; sprawdzaj import w świeżym interpreterze.
- Nie zmieniaj po cichu schematu CSV, kodów enumów, nazw plików ani jednostek czasu. Wpływają na scraper, quality checker, merger i zewnętrzny moduł C++.
- Nie usuwaj cudzych segmentów shared memory, nie zabijaj innych procesów w celu „naprawienia testów”. Stałe nazwy `client_to_collector_tq_*` i `client_to_collector_dq_*` mogą należeć do innej instancji. Izolacja testów jest osobnym zadaniem.
- W PowerShell nie zakładaj semantyki Bash. Używaj jawnego interpretera `.venv/Scripts/python.exe`, poprawnego katalogu roboczego i `rg` do wyszukiwania.
- Jeśli Git zgłasza `dubious ownership`, lokalny odczyt działał z `git -c safe.directory=C:/Users/daniel/PycharmProjects/Binance-Data-Processor ...`. Nie trzeba zmieniać globalnej konfiguracji Git.

- Nie zmieniaj kryteriów overlap bez przykładów interleavingu, duplikatów, brakujących par i co najmniej dwóch rotacji. Uwzględnij oba cid oraz wiele instrumentów.
- Nie instaluj ani nie aktualizuj pakietów tylko po to, żeby audyt wyglądał na zielony. Zależność `cpp_binance_orderbook` wymaga ustalenia pochodzenia i zgodności.
- Historyczne skrypty `sync_binance_clock*.py` nie są obecne w tym drzewie. Nie odtwarzaj ich automatycznie ani nie uruchamiaj trybu ustawiającego zegar OS w ramach zwykłej walidacji; historia jest w `PROJECT_STATE.md`.

## Walidacja po zmianie

Testy quality wyznaczają katalog `test_csvs` względem `__file__`, niezależnie od CWD. Poniższy przykład z root nadal działa, bez bytecode i cache pytest; można też użyć `.venv/Scripts/python.exe -X utf8 -B -m pytest binance_data_processor/test/test_data_quality_checker.py -q -p no:cacheprovider` bez zmiany katalogu:

```powershell
Push-Location binance_data_processor/test
try {
    ..\..\.venv\Scripts\python.exe -X utf8 -B -c "import sys; sys.path.insert(0, '../..'); import pytest; raise SystemExit(pytest.main(['test_data_quality_checker.py','-q','-p','no:cacheprovider']))"
} finally {
    Pop-Location
}
```

**Przed uruchomieniem testów kolejek sprawdź kolizje nazw SRB i cleanup po błędzie.** Nie uruchamiaj ich równolegle z aktywnym sinkiem. Przy diagnozie pojedynczej usterki użyj konkretnego nodeid w świeżym interpreterze, aby ograniczyć kaskadę `_instances`. Nie dostosowuj testów mechanicznie do implementacji: najpierw ustal kontrakt.

| Zmieniany obszar | Sensowna walidacja po przyszłej zmianie |
| --- | --- |
| Kolejki / overlap | Kolejność, brak utraty/duplikatów, oba cid, ≥2 rotacje, wiele par, cleanup po błędzie |
| SRB / lifecycle | Zawijanie, pełny bufor, za duży rekord, spawn/attach, shutdown z zaległymi rekordami, izolowane nazwy |
| Writer / collector | Krótki offline replay → rotacja → finalny flush → parse JSON/ZIP; mock upload/fallback |
| CSV / daty | MS vs US, północ UTC, dzień następny, zerowa ilość depth, round-trip nazw i komentarzy |
| Merger | Poprzedni dzień, pierwsza migawka, IsLast, stałe TS, wiele rynków/par, zgodność kodów C++ |
| Konfiguracja / API | Walidacja typów, błędne żądania, propagacja do procesów, brak skutków ubocznych przy błędzie |

Wyniki i środowisko konkretnych przebiegów zapisuj w [PROJECT_STATE.md](PROJECT_STATE.md). Zielone testy predykatów jakości nie dowodzą poprawnej archiwizacji ani odtwarzania order booka.
