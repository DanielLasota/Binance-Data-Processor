# PROJECT_STATE.md — stan projektu i backlog

Stan odczytany: **2026-10-24**, lokalny working tree (staged, unstaged i pliki nieśledzone), nie czysty HEAD ani wdrożenie. Przegląd aktualizuje audyt z 2026-10-10. W audycie stanu zmieniono **wyłącznie PROJECT_STATE.md**; nie naprawiano kodu ani nie uruchamiano produkcyjnych launcherów. Późniejsze porządkowanie dokumentacji opisano osobno poniżej.

**Najważniejsza zmiana:** zapis streamów jest już podłączony przez nowy proces `DataArchiver`. Dawny opis „kolektor tylko gromadzi deque i nic nie zapisuje” jest nieaktualny. Włączono też depth, snapshoty i retry ZIP dla Backblaze. P0-01 i P0-04 są częściowo wykonane; P0-02 i wejściowa część P0-03 nadal blokują wiarygodność długiego zbierania danych.

Zasady pracy są w [AGENTS.md](AGENTS.md), a mapa zależności, przepływów i kontraktów w [MAP.md](MAP.md). Ten dokument utrzymuje stan funkcjonalny, backlog i dowody walidacji. Poniższe ścieżki są względem `binance_data_processor/`, chyba że zaznaczono root.

## Podsumowanie stanu funkcjonalnego

| Funkcja | Ustalenie na 2026-10-24 |
| --- | --- |
| Import biblioteki | Działa w świeżym interpreterze; bez pandas/numpy/alive_progress |
| Odbiór WebSocketów | Aktywne trade **i depth** dla każdego skonfigurowanego rynku; bez nowego testu sieciowego |
| Zapis streamów | **Podłączony**: collector → SRB → osobny archiwizer → writer; grupowanie, rotacja i flush po sentinel potwierdzone próbą offline z mockiem writera |
| Rotacja WS | **Nadal niespięta**: brak mostu flag w collectorze; pozostaje przypisanie `did_ws_switch = False` |
| Pełny bufor | Collector czeka na miejsce w SRB archiwizera; Client nadal ignoruje odrzucenia w wejściowym SRB |
| Stop i cleanup | Archiwizer ma sentinel/join/close; fasada nadal bez join i potwierdzenia zakończenia producentów przed drain |
| Snapshoty | Proces uruchamiany; błędy HTTP/timeout nadal mogą zakończyć usługę wskutek niespójnych typów wyjątków |
| Retry lokalnych ZIP | Uruchamiane tylko dla `SaveTarget.BACKBLAZE`; upload przed usunięciem pliku, bez nowego testu storage |
| Listener | Nadal niezgodne konstruktory i model kolejek |
| API / CLI | Odłączone; import `core.api_srv` dodatkowo kończy się brakiem `core.light_api` |
| Scraper / merger | Główne ograniczenia backlogu pozostają; brak dystrybucji `cpp-binance-orderbook` w zastanej .venv |
| Quality predicates | **131 passed**; nie jest to test całych raportów ani pipeline |
| Docker/deployment | Nadal nieistniejący entry point; nie budowano i nie wdrażano |

## Nawigacja do przepływu i kontraktów

Aktualny przepływ online, granice SRB i lifecycle opisuje [MAP.md — online](MAP.md#online), kontrakt transportu [sekcja transportu](MAP.md#transport), a aktualne nazwy konfiguracji [sekcja konfiguracji](MAP.md#konfiguracja). Status nieśledzonego `core/archive_worker.py` oraz nieobecnych modułów wskazano w mapie. Dowody i otwarte kryteria P0 pozostają poniżej.

## Dowody i ograniczenia audytu 2026-10-24

**Przegląd kodu i kontrola statyczna**

- Sprawdzono `git status --short`, staged/unstaged diff oraz aktualne źródła wywołujących i wywoływanych w ścieżkach sinka, kolejek, archiwizacji, snapshotów i sterowania. Ponownie sprawdzono konfiguracje i miejsca wskazane przez backlog offline; analiza nie jest walidacją wszystkich możliwych danych wejściowych.
- AST: **73 pliki Python** (pakiet, `setup.py`, `copy_to_clipboard.py`), bez błędów i bez zapisu bytecode. Świeży import pakietu poprawny, bez ciężkich bibliotek. Osobny import `binance_data_processor.core.api_srv`: **ModuleNotFoundError** dla `core.light_api`.
- Środowisko: Windows, CPython 3.11.4, pytest 9.0.2, websockets 16.0, pandas 3.0.0, numpy 2.4.2, concurrent-log-handler 0.9.28, psutil 7.2.2. Brak metadanych `cpp-binance-orderbook`. Niczego nie instalowano.
- Porównanie hashy wykryło równoległe zmiany kodu, AGENTS.md i logów oraz zmiany indeksu Git wykonane poza tym zadaniem. Agent ich nie wykonywał ani nie cofał. Po rename'ach ponowiono AST (nadal 73 pliki), świeży import i testy quality. Próby SRB/archiwizera wykonano wcześniej; ich opis rozróżnia zakres mocków i nie jest deklaracją nieruchomego snapshotu całego repo.

**Testy lokalne**

- `test_data_quality_checker.py`: **131 passed in 1.52 s** w końcowym przebiegu po rename'ach (wcześniej również 131 passed, 1.21 s). Są to testy `IndividualColumnChecker`, nie pełnych raportów.
- `--collect-only`: **176 testów** (131 quality, 27 depth, 18 trade). Pełnego zestawu kolejek tym razem nie uruchamiano: nadal używa stałych nazw SRB i cleanup po asercjach, więc może kolidować z działającym sinkiem. Wynik **145 passed / 27 failed / 4 skipped** pochodzi wyłącznie z 2026-10-10.
- Jednorazowe próby przez stdin, bez dodawania plików testów: rzeczywisty SRB pod własnymi nazwami UUID przeszedł zawijanie nagłówka/payloadu, pełny bufor, nadmierny rozmiar rekordu, metadane, attach i odrzucenie capacity 0/8/17. Zamknięto i usunięto tylko własne segmenty.
- Próba kolejek domenowych → `DataCollector._forward_to_arch_srb` → rzeczywisty SRB → `DataArchiver.run`: **3 wiadomości, 3 grupy, 2 rynki, trade/depth**, zachowane `_E`, rotacja czasowa i końcowy flush. Writer i zegar rotacji były mockowane; wykonanie w jednym procesie, bez testu spawn i bez fizycznych plików JSON/ZIP.
- Potwierdzono wyjątek dla nieobsługiwanego celu writera oraz propagację `OSError` z zapisu JSON (mock `open`). Potwierdzono ignorowanie `False` z SRB przez klienta i wyjście ogólnego `Exception` z pobierania snapshotów po mockowanym `requests.Timeout`.
- Komendy pytest z katalogu `binance_data_processor/test`:

```powershell
..\..\.venv\Scripts\python.exe -X utf8 -B -c "import sys; sys.path.insert(0, '../..'); import pytest; raise SystemExit(pytest.main(['test_data_quality_checker.py','-q','-p','no:cacheprovider']))"
..\..\.venv\Scripts\python.exe -X utf8 -B -c "import sys; sys.path.insert(0, '../..'); import pytest; raise SystemExit(pytest.main(['--collect-only','-q','-p','no:cacheprovider']))"
```

**Integracja i granice wniosków**

Nie uruchamiano sieci Binance/Backblaze, launcherów `bin/main_*.py`, pełnego sinka, GUI, kontenera ani deploymentu. Nie zmieniano czasu OS. Próby z mockami nie zamykają kryterium pełnej archiwizacji na dysk, bezstratnego shutdown ani pracy pod obciążeniem. Historia wcześniejszych pomiarów pozostaje na końcu dokumentu.

## Jak czytać backlog

**P0**: blokuje podstawowy pipeline lub stwarza bezpośrednią utratę danych. **P1**: istotny błąd funkcjonalny, wiarygodności wyników albo testowania. **P2**: przenośność, obsługa błędów, utrzymanie i ergonomia. „Potwierdzone w kodzie” nie oznacza wykonanej reprodukcji z realną siecią. Ryzyka wymagające pomiaru lub decyzji są wyraźnie opisane osobno.

Status „częściowo wykonane” oznacza potwierdzony postęp i nadal otwarte kryteria akceptacji. Pozostałe zadania są otwarte, chyba że wskazano inaczej. Przy zamykaniu dopisz dowód i test/komendę; sama obecność klasy ani zmiana nazwy metody nie wystarcza.

## P0 — przywrócenie podstawowego przepływu

### P0-01 — zapis i ograniczenie przyrostu pamięci — częściowo wykonane

- **Historia:** 2026-10-10 collector odkładał dane do deque bez aktywnego flush.
- **Zrobione:** `_forward_to_arch_srb` opróżnia deque, nowy proces `DataArchiver` grupuje wiadomości i zapisuje przez writer. Jest rotacja czasowa i końcowy flush po sentinel. `_send` czeka na miejsce w 16 MiB SRB, odrzuca zbyt duży payload wyjątkiem i sprawdza żywotność archiwizera. Błąd zapisu nie jest już cicho połykany przez lokalny writer.
- **Dowód:** `core/data_collector.py::run/_send/_forward_to_arch_srb`, `core/archive_worker.py::run/_collect/_flush`, `core/data_writer.py`; próba offline 3 wiadomości/3 grupy opisana wyżej.
- **Pozostało:** pełny replay do fizycznych JSON/ZIP i test wolnego storage. Listy archiwizera nie mają limitu bajtów/wiadomości na okno; `join` do JSON i kompresja zwiększają szczytowe RAM. Gdy archiwizer blokuje się na zapisie, collector czeka, wejściowe SRB się zapełniają, a Client nadal cicho odrzuca rekordy (P0-03). `_send` nie ma deadline ani przerwania przez stop przy żywym, lecz zawieszonym archiwizerze.
- **Gotowe gdy:** replay wszystkich par zgadza się liczbowo z plikami, stop zapisuje pełny ogon, a przeciążenie ma określony limit pamięci i widoczny wynik dla każdego rekordu. Sprawdzić kolizję nazwy między rotacją a finalnym flush w tej samej sekundzie (P2-06).

### P0-02 — pełny handshake rotacji WS — otwarte

- **Dowód:** `DataCollector._drain_client_srb` nadal tylko przekazuje rekordy do `queue.put`; nie obsługuje `set_switching_ws_mode` ani nie przekazuje `queue.did_ws_switch` do mp.Event. `StreamService._stream_loop` czeka na ten event, a po hipotetycznym sukcesie nadal wykonuje `did_ws_switch = False` zamiast `clear()`. Historycznego `SRBReader` nie ma już w drzewie.
- **Skutek:** pierwszy overlap może trwać bez końca; stary cid pozostaje akceptowany, nowy ignorowany. Wspólny `is_ws_overlapping` blokuje rotację pozostałych streamów. Sama naprawa mostu pozostawi błąd kolejnej rotacji.
- **Do zrobienia:** most i reset obu eventów oraz flag domenowych, timeout/błąd overlap, reset wspólnego stanu w `finally`, zgodność accepted cid po restarcie klienta. Przykładowe 30 s lifetime szybko dochodzi do tej ścieżki; nie jest to wynik próby live.
- **Gotowe gdy:** co najmniej dwie kolejne rotacje i błąd w trakcie overlap kończą się bez utraty/duplikacji i bez blokowania innych rynków; stop przerywa oczekiwanie.

### P0-03 — pełny SRB i widoczna utrata danych — wejście nadal otwarte

- **Nadal niegotowe:** `Client._recv_loop` ignoruje wynik `client_to_collector_srb.put_nowait`. Brak miejsca lub rekord większy niż 4 MiB daje `False` bez licznika i zdarzenia utraty ciągłości. Ignorowanie zwrotu potwierdzono próbą z mockowanym SRB.
- **Zrobione poza wejściem:** collector → archiver ponawia zapis zamiast usuwać wiadomość z deque przy pełnym buforze. SRB waliduje power-of-two >= 16, little-endian i minimalny rozmiar segmentu przy attach, wykrywa nieprawidłową długość rekordu; szybka ścieżka korzysta z `memoryview.cast("Q")`. Próby wrap/full/oversize/attach przeszły.
- **Do zrobienia:** spójna polityka przeciążenia całego przepływu, również zbyt dużych rekordów; metryki i jawne oznaczenie przerwanej ciągłości. Blokowanie kolektora na drugim SRB nie zapewnia backpressure do źródła.
- **Gotowe gdy:** kontrolowany overflow nie jest cichy; bilans wejścia/archiwum/odrzuceń jest jednoznaczny, także przy wolnym writerze i wielu streamach.

### P0-04 — lifecycle procesów i shared memory — częściowo wykonane

- **Zrobione:** collector zachowuje uchwyt procesu archiwizera, sprawdza jego żywotność, po zwykłym stop drenuje wejścia i wysyła sentinel; `_stop_archiver` robi join, sprawdza exitcode i zamyka/unlinkuje własny UUID SRB. Archiwizer po sentinel flushuje i zamyka writer oraz uchwyt.
- **Pozostało w fasadzie:** procesy kolektora/stream/snapshot/retry są lokalnymi zmiennymi; gotowość zastępuje `sleep(3)`, `stop()` ustawia wspólny event i czeka 5 s. Brak join i propagacji awarii tych procesów do rodzica.
- **Ryzyko utraty ogona:** ten sam stop zatrzymuje producentów i kolektor. Collector może zobaczyć puste SRB i wysłać sentinel, zanim Client zakończy ostatni recv/put. Nie ma potwierdzenia „wszyscy producenci zakończeni”. Po wyjątku pętli collector przechodzi do `finally` bez gwarantowanego drain wejść/deque; archiwizer po wyjątku nie wykonuje normalnego końcowego flush.
- **Pozostałe zasoby:** brak wywołania `queue.close()` przez collector i zamknięcia wejściowego SRB w `Client.close()`; wejściowe nazwy nadal globalne. Nieudany konstruktor kolejki pozostawia wpis w `_instances`. Join archiwizera nie ma limitu czasu; snapshot i emergency sender nie mają pełnego cleanup sesji na normalnym wyjściu pętli.
- **Gotowe gdy:** gotowość i błędy są jawnie sygnalizowane, stop przebiega producenci → potwierdzenie końca → drain → flush → join → cleanup właściciela. Ponowny start po stop/awarii działa, nie dotyka cudzych segmentów i nie traci końcowych danych.

## P1 — integracje, kontrakty i wiarygodność wyników

### P1-01 — obsługiwane źródła danych — włączone, integracja do potwierdzenia

- **Zrobione:** `StreamService._run_async` uruchamia trade i depth; fasada uruchamia `SnapshotSrv`, a dla BACKBLAZE także `EmergencySender`. Retry przegląda lokalne ZIP, wysyła i usuwa plik po powodzeniu wywołania uploadu.
- **Pozostało:** test całego przepływu streamy + snapshoty + odzyskiwanie uploadu; błąd snapshotu opisano w P2-05, brak nadzoru i cleanup w P0-04. Włączone usługi nie oznaczają potwierdzonej kompletności order booka.
- **Gotowe gdy:** konfiguracja daje przewidywalny zestaw usług, replay/integracja sprawdza streamy i snapshot, a awaryjny ZIP jest ponownie wysłany i usuwany dopiero po potwierdzonym sukcesie.

### P1-02 — przebudować niespójny listener

- `listener_facade.py`: `ListenerQueuePool()` bez wymaganego cfg; `StreamService(queue_pool=...)` zamiast `queue_flags_pool`; `SnapshotSrv(snapshot_strategy=...)`, choć konstruktor tego nie przyjmuje.
- Nawet po poprawieniu argumentów `StreamService.run()` i `SnapshotSrv.run()` są blokujące; stare sekwencyjne `listener.run()` nie uruchomi poprawnie whistleblowera. Kolejki nie publikują zaakceptowanych wiadomości do global_queue; `close()` zakłada metodę unlink nieobecną w zwykłej kolejce.
- `bin/main_listener.py` używa `DataSinkCfg(show_logo=False)` bez wymaganych pól.
- Gotowe: listener startuje i oddaje sterowanie, observer otrzymuje stream/snapshot dokładnie raz, detach i shutdown działają; wyjątek observera nie zabija po cichu całej dystrybucji.

### P1-03 — naprawić sterowanie i komunikację między procesami

- API/CLI nie startują w sinku. `CLI.shutdown` jest pusty; `stop_callback` nie jest inicjalizowany. `modify_subscription` używa nieistniejącej `queue_pool.cmd_queue`. `show_status` traktuje deque jak `queue.Queue` (`qsize`, `.queue`).
- **Poprawione w kliencie:** `update_subscription` nie odwołuje się już do `self.queue`, a `_fast_recv_loop` usunięto. Nadal brak kanału komend między procesami i potwierdzenia zastosowania subskrypcji; zmiana cfg w jednym procesie spawn nie aktualizuje pozostałych.
- **Dodatkowy bloker:** `core/api_srv.py` importuje nieobecny `core/light_api.py`; sam import modułu API nie działa (sprawdzone w świeżym interpreterze).
- Gotowe: istnieje jawny kanał komend/odpowiedzi i aktualizacji configu; operacja nie zmienia lokalnego modelu przed nieudaną propagacją. Nowy rynek/para tworzy potrzebne flagi i transport albo zostaje jawnie odrzucony.

### P1-04 — naprawić TimeCfg i walidację konfiguracji sinka

- `enums/time_cfg.py::TimeCfg.update_interval` dopuszcza stare `snapshot_fetcher_interval_seconds`, `websocket_lifetime_seconds`; slots mają `snapshot_interval_seconds`, `ws_lifetime_seconds`. Rename modułu nie naprawił whitelisty. Brak dodatnich granic interwałów i odrzucenia bool jako int.
- `DataSinkCfg.save_target` nadal dopuszcza w adnotacji string, a writer obsługuje `SaveTarget`. **Poprawione:** nieobsługiwany cel rzuca teraz `ValueError`, zamiast cichego no-op. Walidacja następuje jednak przy zapisie, nie przed startem procesów. `instruments/time` nie są konwertowane z dict; brak walidacji Backblaze credentials i obsługi `LISTEN_ONLY_FOR_TEST_PURPOSES` w writerze.
- Gotowe: publiczny kontrakt jest jednoznaczny, złe typy i nieobsługiwane cele są odrzucane przed uruchomieniem; każdy zaakceptowany interwał aktualizuje rzeczywiste pole i docelowy proces.

### P1-05 — naprawić domyślne pola i wejścia DataConcatenatorConfig

- `scraper/concatenator_config.py` ma końcowe przecinki w defaultach: `join_pairs_into_one_csv=False,`, `join_markets_into_one_csv=False,`, `skip_existing=True,`, `csvs_nest_catalog='...',`. Wynikiem są jednoelementowe tuple; `(False,)` jest truthy.
- Adnotacja dopuszcza enumy, ale `__post_init__` zawsze wywołuje `.lower()`. Scraper z kolei zakłada niepuste listy i sprawdza jedynie pierwszy element.
- Gotowe: domyślne flagi są bool, katalog str, enumy/stringi obsługiwane konsekwentnie, puste lub mieszane listy dają kontrolowany błąd; wszystkie cztery warianty grupowania mają test.

### P1-06 — rozstrzygnąć zależność i kontrakt cpp_binance_orderbook

- Merger importuje `compute_final_ob` lokalnie, ale repo nie zawiera implementacji, sposobu instalacji ani wersji/ABI. W sprawdzonej `.venv` brak metadanych dystrybucji.
- Ustalić pochodzenie zależności, wersję, schemat wejściowy, jednostki timestampów i zgodność kodów Symbol/Market/StreamType. Nie implementować przybliżonego order booka tylko po to, aby ominąć import.
- Gotowe: udokumentowana instalacja i minimalny fixture poprzedniego dnia daje deterministyczny final snapshot zgodny z oczekiwanym stanem order booka.

### P1-07 — uzupełnić symbole bez zmiany dotychczasowych kodów

- `enums/symbol_enum.py` kończy się na SUIUSDT. Stock production config zawiera również m.in. PEPEUSDT, XLMUSDT, LINKUSDT, VIRTUALUSDT, 1000PEPEUSDT, HYPEUSDT, 1000SHIBUSDT. `symbol_to_int` rzuci KeyError dla nieznanego symbolu.
- Gotowe: wszystkie wspierane instrumenty mają uzgodnione kody zgodne z odbiorcą C++; dotychczasowe wartości pozostają stabilne; nieznany instrument jest wykrywany przed długim merge.

### P1-08 — dodać walidację pustych danych i FINAL_DEPTH_SNAPSHOT

- `IndividualColumnChecker` używa `unique()[0]`, `iloc[0/-1]`, `diff().min()` bez jednolitej polityki pustych/single-row danych. `DataQualityReport.is_positive` uznaje pusty rejestr za pozytywny, a dowolny truthy wynik (np. tekst błędu) za sukces; formatowanie używa innego warunku `result == True`.
- `DataQualityChecker.calc_quality_report` nie obsługuje `FINAL_DEPTH_SNAPSHOT`; appendix jawnie zawiera TODO. `get_main_merged_csv_report` wyłącza final snapshot z kontroli dnia/monotoniczności, a nie sprawdza pełnej rekonstrukcji.
- `conduct_csv_files_data_quality_analysis` dzieli przez zero przy braku raportów (także gdy wszystkie pliki odpadną); potem wymaga inputu.
- Gotowe: brak danych i nieobsługiwany stream nie dostają certyfikatu pozytywnego; błąd ma kontrolowany raport; final snapshot ma sprawdzoną dobę, strony, ilości, symbol i kompletność zgodną z kontraktem C++.

### P1-09 — raport jakości musi wskazywać faktycznie eksportowany etap

- Merger liczy raporty **przed** `_drop_non_standard_x_and_m_unknown_parameters`, redukcją kolumn i `to_int`. Potem zapisuje ten sam raport nad innym dataframe. Shape/liczba wierszy i zakres sprawdzeń nie odpowiadają bezpośrednio eksportowi.
- Gotowe: raport rozróżnia wejście i eksport albo końcowy zbiór jest walidowany ponownie; liczby usuniętych rekordów i schemat finalny są czytelne. Sprawdzić obsługę `'NA'`/NaN z pandas i puste wyniki filtrowania M/X.

### P1-10 — urealnić testy kolejek i zagwarantować cleanup — częściowo wykonane

- **Naprawa 2026-10-24 po audycie:** zachowano istniejące scenariusze, literalne wiadomości i styl asercji. Poprawiono argumenty `put`, pusty odczyt (`IndexError`), czyszczenie depth przez `queue.clear()` i przypisanie `global_queue` do `srb`. Dawne testy listenera opisują teraz lokalny bufor wyjściowy kolejki; nie dowodzą integracji observera.
- Włączono cztery pominięte testy depth, przywrócono asercję rozmiaru i zamknięcie kolejki. Test niedopasowanych sekwencji oraz benchmark jawnie włączają tryb przełączania WS; benchmark sprawdza także zmianę cid i liczbę zaakceptowanych wiadomości. Kod produkcyjny kolejek pozostał bez zmian.
- **Testy lokalne:** obie kolejki **45 passed**, cały zestaw uruchomiony w odwrotnej kolejności **176 passed**; szczegóły środowiska i ograniczenia w dodatku naprawy poniżej. Historyczne niepowodzenia z 2026-10-10 pozostają zapisem wcześniejszego stanu.
- **Pozostało:** osobne testy rzeczywistego transportu, cleanup zasobów OS i integracji listenera. Benchmark pozostaje w dotychczasowym pliku zgodnie z zakresem tej naprawy; nie jest pomiarem wydajności produkcyjnego pipeline.
- Gotowe: kontrakt odczytu/observera uzgodniony, fixtures mają `yield/finally`, nazwy zasobów są izolowane, testy przechodzą pojedynczo i w zestawie w dowolnej kolejności. Pomiary benchmarkowe oddzielone od testów poprawności.

### P1-11 — naprawić instalację i obraz kontenera

- `setup.py` nie deklaruje `psutil` i `concurrent-log-handler`, mimo importów używanych przez publiczny pakiet; nie deklaruje też matplotlib dla wykresów ani zależności C++ dla mergera. `requirements.txt` zawiera już psutil i pytest, ale nadal brakuje concurrent-log-handler. `requirements-dev.txt` nie zawiera pełnego zestawu runtime. `copy_to_clipboard.py` wymaga osobnego pyperclip.
- `Dockerfile` uruchamia `binance_archiver/main_data_sink.py`, którego nie ma; kod jest w `binance_data_processor/bin/`. Samo poprawienie ścieżki nie rozwiązuje P0 ani CPU affinity.
- Gotowe: instalacja z metadanych w czystym środowisku umożliwia import i jasno opisuje opcjonalne funkcje; obraz startuje z właściwym modułem/configiem i kontrolowanym lokalnym targetem. Dane/logi trafiają do deklarowanych wolumenów.

## P2 — niezawodność i utrzymanie

### P2-01 — downloader: ograniczenia retry, błędy i granice doby

- `download_file_and_put_json_into_queue` ma `while True` z retry co 10 s bez cancel/limitu; join wątków może wisieć bez końca. Parser wadliwego ZIP/JSON zwraca None zamiast propagować błąd, który potem wychodzi przy iteracji.
- `BackBlazeS3Client.list_files_with_prefixes` łapie błędy i zwraca częściową listę; `read_file` nie zamyka jawnie StreamingBody. Brak strategii weryfikacji kompletności oraz jawnego porządku chronologicznego po listowaniu.
- Prefix następnego dnia `T00-0` obejmuje jedynie pierwsze 10 minut. Aktualny przykład sinka ma file_duration=180 s (dawne 10000 s jest nieaktualne), lecz większe interwały i opóźnienia zapisu nadal mogą wyprowadzić nazwę paczki poza to okno.
- Gotowe: błąd listowania/downloadu nie udaje pełnej doby; retry ma limit/cancel, stream body jest zamknięty, testy obejmują pusty bucket, wadliwy ZIP i pliki przekraczające północ poza oknem 10 minut.

### P2-02 — merger: edge cases i jednostki snapshotu

- `_load_depth_snapshot_root_csv` dla SPOT kopiuje `TimestampOfReceive` MS do kolumny nazwanej US bez mnożenia. Późniejsza iniekcja nadpisuje TS na poprawny depth-US, więc to nie dowód błędnego finalnego outputu każdej ścieżki, ale kontrakt pośredniego dataframe jest nieprawdziwy.
- `_merge_difference_depth_with_depth_snapshot_with_trade_within_single_pair` zakłada obecność wszystkich trzech streamów, mimo dowolnej listy w configu; `next(..., None)` przechodzi dalej. Pusty snapshot/C++ book uderza w `iloc[0]`/`index[-1]`. Snapshot bez kolejnego depth kończy cały merge wyjątkiem.
- `make_merged_csvs` zawsze wywołuje downloader, nawet jeśli lokalne pliki wystarczają lub wszystkie merged są pominięte. Walidator liczby finalnych poziomów rekonstruuje je inaczej niż zewnętrzny C++ — zgodność wymaga testu.
- Gotowe: jawna walidacja wejść, spójna jednostka każdego etapu, opisane zachowanie końca dnia i lokalny tryb bez credentials.

### P2-03 — przenośność ścieżek i nazw plików

- `prepare_dump_path_catalog` używa bezwarunkowo `os.startfile`; oba configi CSV otwierają GUI. Domyślne katalogi mergera są specyficzne dla dysku D:.
- `get_list_of_existing_*` dzieli ścieżkę po `'/'`, podczas gdy `os.walk` na Windows może zwracać backslash. Parser root jest podatny na podkreślenia w katalogu rodzica.
- Nazwa merged nie koduje listy streamów; dekoder odtwarza iloczyn wszystkich rozpoznanych par/rynków i trzech streamów, który nie zawsze odpowiada pierwotnej grupie. Ryzyko fałszywego skip lub kolizji nazw trzeba sprawdzić na mieszanych rynkach i podzbiorach streamów.
- Gotowe: pathlib/basename, round-trip na Windows/Linux, brak wymaganego GUI, skip zgodny z rzeczywistą zawartością.

### P2-04 — HTTP/CLI: odtworzyć obsługiwaną warstwę protokołu

- Aktualny `ApiSrv` importuje nieobecny `core.light_api`; obecnie nie można sprawdzić zachowania serwera na żądaniach. Ustalenia z 2026-10-10 o jednym komunikacie ASGI, `more_body` i podwójnej serializacji dotyczyły wcześniejszej implementacji, nie istniejącego dziś modułu.
- `CLI` nadal bierze pierwszy element body bez walidacji, część komend zwraca `json.dumps`, a błąd subskrypcji może zwrócić obiekt Exception. Te kontrakty trzeba uzgodnić z docelowym adapterem HTTP.
- Gotowe: działający import, schema request/response, fragmentowane body, kontrolowane 4xx/5xx, spójne typy i lifecycle API z globalnym stop. Konfiguracja `ApiSrv` nadal wskazuje `127.0.0.1:5000`.

### P2-05 — snapshot: odporność i stop — częściowo poprawione

- **Zrobione:** `_fetch_and_save_snapshots` sprawdza stop przed kolejną parą, pobranie ma blok try per para; błędy zapisu są logowane i propagowane. Samo przerwanie sleep nie musi już powodować nowego requestu.
- **Pozostały błąd, potwierdzony mockiem:** `_request_snapshot_with_timestamps` opakowuje każdy błąd w ogólny `Exception`, natomiast wyżej łapane są `SnapshotStopped`, `requests.RequestException` i `ValueError`. Timeout/HTTP error omija tę obsługę i kończy usługę, zamiast przejść do następnej pary.
- `SnapshotSrv.run` nie ma końcowego cleanup sesji/writera; `stop()` zamyka sesję, lecz fasada go nie wywołuje. Nie ma nadzoru rodzica nad zakończeniem procesu.
- Gotowe: określona obsługa 429/5xx/timeout, pojedynczy błąd nie gubi pozostałych par, stop nie inicjuje dalszych requestów i zamyka zasoby. Aktualne zasady API trzeba sprawdzić przed implementacją retry.

### P2-06 — storage: potwierdzenie zapisu i błędów — częściowo wykonane

- **Zrobione:** JSON i ZIP są otwierane w trybie `x`; `OSError` jest logowany i ponownie rzucany. Nieobsługiwany cel daje `ValueError`. W `S3Client._upload_payload` usunięto mylący print `uploaded 200`; błąd HTTP propaguje się, response jest zamykana w finally.
- **Pozostało:** brak atomowego zapisu przez plik tymczasowy. Nazwy mają dokładność jednej sekundy: ponowny flush tej samej grupy może zakończyć archiwizer przez `FileExistsError`; tryb `x` chroni przed nadpisaniem, lecz nie zapewnia obsługi kolizji. Klucz Backblaze także nie ma identyfikatora paczki.
- Emergency sender skanuje wszystkie `.zip`, bez protokołu „plik ukończony”; przy równoległym zapisie fallback ZIP może zobaczyć niedokończone archiwum. To ryzyko wynikające z kodu, bez testu wyścigu.
- Nieużywany przez writer `B2Client` nadal nie uzupełnia bucket id z nazwy przy braku go w autoryzacji, a requesty nie mają timeoutów.
- Gotowe: niedokończony plik nie wygląda na pełne archiwum, kolizja nie traci paczki, retry nie publikuje pliku w trakcie zapisu; potwierdzone błędy JSON/ZIP/upload/fallback i sukces całej ścieżki.

### P2-07 — hot path, timestampy, kompatybilność websockets i retry

- **Zrobione:** usunięto print każdej wiadomości i bezpośredni odczyt `ws.recv_messages.frames.queue`. Nowy `TimestampedConnection` wiąże czas `data_received` z zakończonymi wiadomościami przez `process_event` i własną deque.
- **Pozostało:** testy batchy, fragmentacji, ramek sterujących, zamknięcia połączenia i zgodności z docelową wersją websockets. W zastanej .venv jest 16.0, ale wymagania nadal nie przypinają wersji. Rozdzielczość nowego odczytu czasu Windows nie jest dowodem dokładności względem Binance.
- Connect nadal ma `max_size=None`, `max_queue=None`; brak pomiaru pamięci przy przeciążeniu. Collector obecnie wykonuje busy-spin także przy pustych kolejkach, bez sleep/wait; archiwizer śpi 1 ms przy pustym wejściu.
- Pętle reconnect nie mają jawnego backoff. Regex trade wymaga sąsiadujących pól s/t; brak match daje wyjątek, a identyczne podpisy z jednego cid mogą przełączyć strategię.
- Gotowe: zgodność timestampów i wersji jest sprawdzona, retry i pamięć kontrolowane, pomiary CPU/latencji potwierdzają sens busy-spin, testy overlap obejmują duplikaty jednego klienta i reconnect.

### P2-08 — semantyka aktualizacji par i transportu

- `DifferenceDepthQueue.update_pairs_n` zmienia długości deque porównania, lecz nie `pairs_amount`; `TradeQueue.update_pairs_n` pozostaje puste. Dynamiczna zmiana par podczas overlap nie ma spójnego kontraktu.
- Historycznych `PipeQueue` i `SRBReader` nie ma w bieżącym drzewie. Dawne usterki ich metod nie są aktualnymi zadaniami naprawy istniejącego kodu; nie należy ich przywracać automatycznie.
- Gotowe: jeden jawny kontrakt sterowania i transportu, aktualizacja par propagowana do producentów i kolejek, z określonym zachowaniem podczas overlap.

### P2-09 — CPU affinity i logger

- Fasada wymusza CPU 12 dla stream i CPU 6 dla kolektora/snapshot/retry, bez sprawdzenia dostępności rdzeni, ograniczonego cpuset i platform bez cpu_affinity. Błąd ustawienia affinity może zakończyć potomka jeszcze przed utworzeniem usługi. Wymusza też globalny start method multiprocessing w konstruktorze biblioteki.
- `setup_logger` przy każdym wywołaniu czyści handlers bez jawnego close; zapisuje `archiver.log` i lock względem CWD, a nie `/app/logs` z Dockerfile. Whistleblower używa innej nazwy loggera. Sink ignoruje `show_logo=False` przy swoim logowaniu logo.
- Gotowe: affinity jest opcjonalne/zweryfikowane, biblioteka nie zmienia niejawnie globalnej konfiguracji hosta, wielokrotne setup nie wycieka uchwytów, ścieżka logu jest kontrolowana.

### P2-10 — rejestr ciągłości CSV gubi komentarz

- `continuity_registry.py::dump_to_csv` zapisuje 5 nagłówków (`...event_type,bucket_name,comment`) i 4 wartości; tekst komentarza trafia pod bucket_name, a `load_from_csv` czyta pusty comment. `load` dokłada wpisy bez resetu; ścieżka jest stała w Documents, wykres wymusza TkAgg.
- Gotowe: round-trip wpisów zachowuje komentarz, kontrakt schematu jest zgodny, ścieżka konfigurowalna; tryb bez GUI jest określony. Integrację z runtime traktować jako osobne rozszerzenie.

### P2-11 — dokumentacja publiczna, repo i deployment

- README pokazuje brakujące default cfg, dict config, stare nazwy pól i wywołanie scrapera wieloma kwargs oraz Azure Blob, którego aktualny kod nie implementuje. Stock JSON-y mają historyczny schemat i brak aktywnego loadera.
- `.gitignore` nie obejmuje `.venv`, `__pycache__`, `.pytest_cache`, ogólnych `.env` ani aktualnych logów/locka w bin. `.dockerignore` jest pusty; build context może obejmować lokalne środowisko i artefakty. `MANIFEST.in` wyklucza `.env`, ale nie zastępuje polityki Git/Docker.
- Workflow Azure ma filtr `'Dockerfile/**'`, ścieżkę `appSourcePath` wskazującą plik Dockerfile i placeholder build arguments. Zmiany kodu nie są objęte filtrem. Nie stwierdzono skuteczności obecnego deploymentu; wymaga osobnej walidacji konfiguracji usługi. Brak workflow testów.
- Gotowe: przykłady zgodne z API, jasny status funkcji, minimalny test CI, kontrolowany build context i poprawny workflow. Zmiana tych plików wymaga przyszłego zadania; nie wykonano jej w audycie dokumentacyjnym.

### P2-12 — drobne niespójności kontraktów

- `AssetParams.__str__` ma `self.date if not None else ''` (warunek zawsze prawdziwy); przy braku daty pojawia się None. `get_ap_with_specified_pair` gubi date; `get_date_in_ymd_format` po błędzie drukuje wyjątek i zwraca None zamiast propagować kontrolowany błąd.
- `DataQualityReport` adnotuje wyniki także jako string, mimo oczekiwania bool. Kilka adnotacji pd/cpp używa nazw nieobecnych w module; dzięki future annotations import działa, ale runtime `get_type_hints` może nie działać.
- Gotowe: określone typy i zachowanie błędów; poprawki nie powodują ciężkich importów w ścieżce sinka.

## Jawne TODO i pozostałości refaktoringu

Ponownie sprawdzono `rg -n -i 'todo|fixme|hack|xxx|outdated' -g '*.py'`. Numery dotyczą stanu 2026-10-24.

| Miejsce | Treść / intencja | Powiązanie |
| --- | --- | --- |
| `core/stream_srv.py:112` | `did_ws_switch = False` | P0-02 |
| `core/stream_srv.py:44,132` | Przebudowa async / outdated update_subscriptions | P1-03 / P2-07 |
| `core/client.py:31` | Obsługa Windows/Linux | P2-07; sam odczyt czasu ma już obie gałęzie |
| `core/trade_queue.py:29,111` | Miejsce dopisywania _E / puste update_pairs_n | P2-08 |
| `core/difference_depth_queue.py:140` | update_pairs_n | P2-08 |
| `core/api_srv.py:21` | Usunąć zakomentowaną diagnostykę | P1-03 |
| `scraper/data_downloader.py:126` | receive timestamp int64 vs str | Zachować liczbowy kontrakt czasu |
| `data_quality/data_quality_report.py:30` | is_positive / get_status | P1-08 |
| `data_quality/individual_column_checker.py:529` | FINAL DEPTH SNAPSHOT CHECK | P1-08 |
| `test/test_difference_depth_queue.py:1185` | Sekcja listenera | P1-02 / P1-10 |

`...` w klasach abstrakcyjnych i wyjątkach nie musi oznaczać braku implementacji. Funkcjonalne braki nadal obejmują `CLI.shutdown` i `TradeQueue.update_pairs_n`; historyczny opis zakomentowanego flush nie dotyczy już aktywnego archiwizera.

## Ryzyka do weryfikacji, nie ogłoszone jako zreprodukowane błędy

- Atomowość i widoczność indeksów SRB między procesami/platformami oraz długie obciążenie. Docstring jawnie nie gwarantuje przenośnych atomików acquire/release. Walidacja zera i minimalnego rozmiaru segmentu została dodana; attach nadal nie weryfikuje dokładnej zgodności capacity z producentem (mniejszy niż rzeczywisty power-of-two może przejść).
- Możliwość bezstratnego overlap dla wielu instrumentów o różnej aktywności przy obecnym algorytmie porównania; timeout nie może udawać poprawnego dopasowania.
- Dokładność dziesiętna Price/Quantity po automatycznym read_csv/float i kompatybilność finalnego CSV z konsumentem C++.
- Nazwy archiwów do sekundy, zmiany interwału, restart i wiele instancji zapisujących ten sam klucz: potrzebny test idempotencji/kolizji.
- Snapshot o tej samej LastUpdateId pobrany więcej niż raz, brak jednej strony order booka, brak danych w części doby i mylące wyniki walidatorów opartych na pustym `.all()`.
- Aktualność endpointów/limitów Binance, pól M/X/ps i obsługiwanych symboli. Audyt opisał kod i fixture, nie weryfikował bieżącej dokumentacji zewnętrznych usług.

## Zalecana kolejność następnej pracy

1. Domknąć handshake WS i widoczność utraty na wejściu (P0-02/03), wraz z poprawną kolejnością stop i nadzorem procesów (P0-04). Zapis jest już podłączony — nie trzeba ponownie projektować go jako brakującego połączenia.
2. Potwierdzić P0-01 replayem przez procesy do rzeczywistych JSON/ZIP: rotacja, końcowy ogon, wolny writer, awaria i kolizja nazwy; zapewnić izolację testów (P1-10).
3. Poprawić obsługę wyjątków aktywnego snapshot service (P2-05), walidację konfiguracji (P1-04), atomowość plików i retry ZIP (P2-06); sprawdzić razem już włączone depth/snapshot/retry.
4. Listener i API potraktować jako osobne integracje (P1-02/03, brak LightApi). Ustabilizować offline config, C++ i raporty (P1-05–09); odtworzyć małą dobę od archiwum do CSV.
5. Uzgodnić dokumentację i przykłady z aktualnymi kontraktami, uwzględnić nieśledzony archive_worker w przyszłym commicie, naprawić instalację, Docker i CI/deployment. Mierzyć CPU i pamięć po zapewnieniu obserwowalności strat.

Kolejne naprawy powinny dopisać wynik konkretnej weryfikacji i odpowiednio zaktualizować dokumentację. W audycie stanu agent edytował tylko `PROJECT_STATE.md`; późniejsza reorganizacja objęła trzy dokumenty wskazane poniżej. Backlog nie stanowi zgody na uruchomienie produkcji ani automatyczne wykonanie wszystkich prac.

## Porządkowanie dokumentacji 2026-10-24

- Skrócono `AGENTS.md` do zasad pracy i walidacji. Dodano `MAP.md` z nawigacją według zadania, aktywnymi i odłączonymi ścieżkami oraz kontraktami przeniesionymi z instrukcji. Opis online uzgodniono z aktualnie odczytanym kodem; usunięto nieaktualne twierdzenia o braku zapisu i odłączonych depth/snapshot/retry.
- Szczegółowy opis przepływu przeniesiono z tego dokumentu do mapy. Zachowano backlog, kryteria akceptacji, wyniki i ograniczenia wcześniejszych prób; opis zegarów pozostaje historyczny.
- Zakres tej reorganizacji: tylko `AGENTS.md`, `MAP.md`, `PROJECT_STATE.md`. Odczytano źródła, bez uruchamiania sinka, testów runtime, sieci ani zmiany kodu. Wyniki testów powyżej pochodzą z wcześniejszego audytu, nie z reorganizacji dokumentacji.

## Naprawa testów kolejek 2026-10-24

- Zakres: `test/test_difference_depth_queue.py`, `test/test_trade_queue.py` i ten dokument. Zachowano istniejące dane testowe i prosty styl testów; bez zmian w implementacji kolejek i kryteriach overlap.
- **Statycznie:** AST obu plików poprawne; import pakietu w świeżym interpreterze działa bez pandas/numpy/alive_progress; `git diff --check` bez błędów.
- **Lokalnie:** Windows, CPython 3.11.4, pytest 9.0.2. Oba pliki kolejek: **45 passed in 0.26 s**, bez skip/xfail. Pełny zestaw z odwróconą kolejnością wszystkich testów: **176 passed in 1.21 s**. Uruchomienia z `-B`, `-p no:cacheprovider`.
- Komenda kolejek z root: `.venv/Scripts/python.exe -X utf8 -B -m pytest binance_data_processor/test/test_difference_depth_queue.py binance_data_processor/test/test_trade_queue.py -q -p no:cacheprovider --tb=short`. Pełny zestaw z `binance_data_processor/test`, root dodany do `sys.path`; jednorazowy plugin `pytest_collection_modifyitems` wykonał `items.reverse()` przed uruchomieniem.
- **Izolacja i ograniczenia:** testy wywołują domenowe `put` bezpośrednio, omijając przesył przez SRB. Po potwierdzeniu użytkownika 2026-10-24 przywrócono w obu plikach krótki fixture z komentarzem po angielsku: mockuje tworzenie SRB i czyści `_instances` również po nieudanej asercji. Testy nie tworzą segmentów pamięci współdzielonej; nie sprawdzają rzeczywistego transportu ani jego cleanup. Ponowny przebieg obu plików powyższą komendą: **45 passed in 0.24 s**, bez skip/xfail; pełnego zestawu ponownie nie uruchamiano. Pierwszy przebieg przed naprawą asercji, z izolacją obu klas, dał 32 passed / 9 failed / 4 skipped, bez kaskady limitu instancji.
- **Integracja sieciowa:** nie uruchamiano Binance, Backblaze ani sinka. Zielony wynik potwierdza istniejące scenariusze kolejek, nie integrację listenera, transport między procesami ani pełny handshake rotacji WS. Zmiana dawnych testów listenera na testy lokalnej deque odzwierciedla aktualny przepływ; P1-02 pozostaje otwarte.

## Naprawa ścieżek CSV w testach quality 2026-10-24

- Przyczyna: `TestIndividualColumnCheckerQuantitativeEdition` otwierała `test_csvs/...` względem CWD. Przed zmianą uruchomienie z root kończyło się `FileNotFoundError`, a z katalogu testów przechodziło 55 testów.
- W `test/test_data_quality_checker.py` dodano `TEST_CSVS_DIR = Path(__file__).resolve().parent / "test_csvs"` i użyto go w 59 odczytach CSV. Dane, asercje i kod produkcyjny pozostały bez zmian. Zaktualizowano instrukcję uruchamiania w `AGENTS.md`.
- **Walidacja lokalna:** Windows, istniejąca `.venv` (CPython 3.11.4, pytest 9.0.2), `-B`, `-p no:cacheprovider`. Z root: `.venv/Scripts/python.exe -X utf8 -B -m pytest binance_data_processor/test/test_data_quality_checker.py -q -p no:cacheprovider --tb=short` → **131 passed in 1.86 s**. Z `binance_data_processor/test`, root w `sys.path`, ta sama klasa quantitative → **55 passed in 1.74 s**.
- Równoległy zapis pliku cofnął pierwszą poprawkę ścieżek. Ponownie nałożono ją na aktualną wersję; końcowy przebieg całego pliku z root: **131 passed in 1.19 s**.
- **Kontrola statyczna:** diff zawiera zmianę podstawy ścieżek, bez zmiany scenariuszy; `git diff --check` bez błędów. **Integracja sieciowa:** nie uruchamiano; testy korzystają z lokalnych CSV, bez mockowania odczytu plików.

## Historia wcześniejszych audytów

Poniższe wyniki zachowano jako zapis wcześniejszych sesji. **Nie są wynikami walidacji aktualnego drzewa z 2026-10-24.** Rootowe skrypty zegarów opisane w dodatkach nie są obecnie dostępne w tym workspace; ich wcześniejszych pomiarów nie powtarzano.

### Historyczna walidacja 2026-10-10

- Przejrzano źródła modułów, skrypty wejściowe, konfiguracje, opakowanie pakietu, workflow, testy i schematy 16 fixture CSV. Wielkie fixture i powtarzalne literalne wiadomości testów analizowano strukturalnie; pliki wygenerowane, `.git`, `.venv`, konfiguracja IDE i sekrety nie są dokumentacją architektury.
- Parsowanie AST wszystkich **75 plików Python** projektu (łącznie z `setup.py` i `copy_to_clipboard.py`) zakończyło się powodzeniem. Nie generowano `.pyc`.
- Pełny pytest zbiera **176 testów**: 131 quality, 27 depth queue, 18 trade queue.
- Ostatni pełny przebieg: **145 passed, 27 failed, 4 skipped**, 0,99 s. CWD: `binance_data_processor/test`; root dodany do `sys.path`; `-B`, `-p no:cacheprovider`, `--tb=no`, `-rN`.
- Pierwszy przebieg wykazał dodatkowo `FileExistsError [WinError 183]` dla `dq_SPOT` i `tq_SPOT`, a dalej kaskadę limitu instancji. Wynik zależy od stanu zasobów; nie należy traktować liczby failures jako stałej ani przypisywać wszystkich niezależnym usterkom. Nie ustalono właściciela wcześniejszych segmentów i nie usuwano ich.
- Osobny przebieg `test_data_quality_checker.py`: **131 passed**, 0,98 s. Są to testy `IndividualColumnChecker`, w tym wersje z CSV, nie pełna certyfikacja całego `DataQualityChecker`.
- Nie uruchamiano usług live, uploadu/downloadu Backblaze, REST Binance, przykładowych launcherów, GUI, budowy obrazu ani deploymentu. Ustalenia o ich przepływach wynikają z kodu.
- Zastane środowisko: CPython 3.11.4, pytest 9.0.2, websockets 16.0, pandas 3.0.0, numpy 2.4.2, concurrent-log-handler 0.9.28, psutil 7.2.2. To zapis audytu, nie zadeklarowana macierz kompatybilności. Metadata dystrybucji `cpp-binance-orderbook`: brak.
- Kontrola hashy podczas przygotowania dokumentów wykryła równoległą zmianę `core/client.py`: do diagnostycznego print w `_recv_loop` dopisano `{msg}`. Agent dokumentujący nie zmieniał tego pliku i nie cofał zmiany; opis hot path uwzględnia odczytaną wersję końcową. Pozostałe 102 pliki objęte porównaniem (źródła, konfiguracje, fixture i dokumentacja poza tymi dwoma plikami; bez cache/IDE/środowiska/logów) zachowały hashe. Wyniki pytest pochodzą z przebiegów przed tą końcową kontrolą.

## Dodatek 2026-10-17 - wirtualny zegar Binance Tier S

### Historyczny kontrakt narzędzia

Poniższy opis przeniesiono z dawnego `AGENTS.md`; pliku nie ma w bieżącym workspace.

sync_binance_clock2.py jest niezaleznym narzedziem Python 3.11+ dla Windows/Linux.
Nie zmienia zegara OS ani kodu pakietu. Zaleznosc sieciowa: websockets==16.0.
Stala sesja WS API time, minimum-delay/MAD/Huber, odporny konsensus przedzialow,
Kalman [offset_ns, drift_ppb], monotoniczna baza UTC i wykrywanie nieciaglosci.
--self-test uruchamia testy offline (stdlib) i benchmark S/A/B/C; --benchmark
tylko benchmark. --window oznacza sekundy; --jsonl zapisuje surowe probki i metryki.
Publiczne API: VirtualClock, ClockService.run(stop_event), binance_now_ns/us/ms,
get_clock_offset_ns, get_clock_drift_ppb, get_sync_status, get_sync_metrics.
API modulowe wymaga set_default_clock(service.clock); CLI robi to automatycznie.
Przed kalibracja i po jej uniewaznieniu: ClockNotReady; przestarzaly pomiar/watchdog:
ClockStale. Surowa predykcja moze cofnac czas; opcjonalny wariant monotoniczny
ogranicza cofanie i raportuje odchylenie od predykcji.
Offset jest Binance minus lokalny UTC; drift jest wzgledem perf_counter.
Kowariancja opisuje model statystyczny, a granice offsetu sa warunkowe.
Nie utozsamiac mikrosekundowej rozdzielczosci z dokladnoscia lub matching engine.
Wyniki walidacji sa w dodatku PROJECT_STATE.md; nie dotycza integracji sinka.

### Wcześniejsza implementacja i walidacja

- Dodano samodzielny sync_binance_clock2.py; brak zmian w binance_data_processor.
- Python 3.11+, Windows/Linux, bez ustawiania zegara systemowego. Jedyna zaleznosc
  sieciowa: websockets==16.0; self-test/benchmark korzystaja tylko ze stdlib.
- Specyfikacje Binance WS API time/general/rate limits oraz API websockets 16.0
  zweryfikowano w oficjalnej dokumentacji. URL-e i zalozenia sa w docstringu.
- Implementacja: stala sesja TLS, pojedynczy pending request, receiver przed parsowaniem,
  MS/US detection, timeout/reconnect/backoff, retryAfter epoch i budzet IP z headroom.
  Przy braku serverTime deadline korzysta z dolnej granicy czasu, aby zbyt szybki
  lokalny UTC nie skrocil bana. Logowanie JSONL ma ograniczona kolejke i licznik strat.
- Estymator: minimum-delay z adaptacja selekcji, MAD/Huber, konsensus k-z-n z transportem
  przez ograniczenie dryfu, Kalman 2x2 z calkowanym Q i aktualizacja Josepha.
  Nowa probka zasila Kalmana tylko raz. Mieszanki midpointow obnizaja status i
  poszerzaja granice do obwiedni obserwowanych zrodel; nie identyfikuja backendu.
- Walidacja statyczna: AST, compile bez .pyc, swiezy import, --help i parsowanie CLI.
- Testy lokalne: .venv/Scripts/python.exe -B sync_binance_clock2.py --self-test:
  30 testow PASS, w tym +5 ppb, dlugi dryf, asymetria i jej zmiana, jitter,
  opoznienia, UTC step/slew/suspend, rozne backendy, brak odpowiedzi, MS/US,
  kowariancja, przedzialy, limity/ban, reconnect, watki i lifecycle/logi.
- Benchmark: te same 900 probek na scenariusz, 60 probek rozgrzewki; raport RMSE/p95
  w us dla S/A/B/C. Dla symmetric+jitter RMSE: S 41.647, A 53.701, B 28.676,
  C 82.934. Dla spikes+drift: S 42.285, A 58.972, B 69.098, C 6993.544.
  Nie ma zalozenia, ze S wygrywa zawsze. Stala asymetria pozostawia ok. 6 ms biasu.
- Integracja sieciowa Windows, 2026-10-17 ok. 12:21 UTC: proba 45 s, 44 odpowiedzi,
  42 zaakceptowane/2 odrzucone, min RTT 233.1714 ms. Status DEGRADED przed
  zatrzymaniem, bez deklarowania wysokiej dokladnosci. Systemowy czas niezmieniony.
  Pierwsza proba sandbox byla zablokowana WinError 5; powiodla sie proba poza sandbox.
- Koncowa proba live 6 s: kod wyjscia 0, jednostka microsecond, zaakceptowane/odrzucone 5/0.
- Przenosnosc sprawdzona w kodzie; nie wykonywano uruchomienia na Linux/Vultr.
  W Windows CPython 3.11.4 get_clock_info('time') raportuje rozdzielczosc 15.625 ms.
  Jest uwzgledniona w mapowaniu/przedzialach, a wspolny blad poczatku ukladu
  odniesienia nie jest wielokrotnie usredniany jako niezalezny szum pomiarowy.
- Nie jest to test sinka ani dowod dokladnosci zegara matching engine. Warunkowe
  granice zakladaja polozenie S w czasie request-response i ograniczony dryf;
  nieznana asymetria i przyszle nieobserwowane backendy pozostaja ograniczeniem.


## Dodatek 2026-10-14 — samodzielny skrypt synchronizacji

- Dodano `sync_binance_clock.py` w root; nie zmieniono kodu data sinka.
- NIST: 4 próbki NTP, przerwy 2 s; Binance SPOT: 10 próbek HTTPS. Wybór próbki
  z najmniejszym RTT. Raport obejmuje wszystkie próbki, błędy, medianę, zakres,
  RTT, zastosowaną korektę i ponowne pomiary obu źródeł.
- Po pomiarach klawisz `N` wybiera NIST, `B`/Enter lub upływ 3 s wybiera Binance.
  Tryb zmiany czasu wymaga Windows i administratora; `--dry-run` nie zmienia zegara.
  `--source nist|binance` umożliwia jawny wybór bez interakcji.
- Wymagane minimum 3 poprawne próbki wybranego źródła, domyślny limit korekty 5 s,
  RTT 1 s. Skrypt wykrywa zmianę lokalnego czasu w trakcie pomiarów. Nie przełącza
  automatycznie na inne źródło, gdy wybrane źródło zawiedzie. Konfiguracja NTP Windows
  pozostaje bez zmian; pomiar offsetu nie jest pomiarem tempa dryfu.
- Testy lokalne: 5 testów `unittest` (NTP, błędne odpowiedzi, timeout wyboru,
  limity, dry-run, raport po niepełnej weryfikacji) przeszło. Zmiana zegara mockowana.
- Integracja sieciowa: `--dry-run` 2026-10-14 około 11:15 UTC: NIST 4/4,
  Binance 10/10 poprawnych próbek; offset odpowiednio +27,672 i +26,738 ms.
  Zegar nie został zmieniony. Nowego skryptu nie uruchamiano z rzeczywistym zapisem czasu.
- Statycznie: import i `--help` poprawne. Poniższy audyt historyczny nadal dotyczy
  stanu z 2026-10-10 i nie stanowi opisu aktualnej integracji sinka.
