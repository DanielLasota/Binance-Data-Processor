import os
import pyperclip

def collect_source_files_content(directories, extensions=('.py', '.cpp', '.c', '.h')):
    all_contents = []

    for directory in directories:
        for root, _, files in os.walk(directory):
            for file in files:
                if file.endswith(extensions):
                    filepath = os.path.join(root, file)
                    try:
                        print(f' readin file: {filepath}')
                        with open(filepath, 'r', encoding='utf-8') as f:
                            content = f.read()
                        entry = f"// ===== {filepath} =====\n{content}\n"
                        all_contents.append(entry)
                    except Exception as e:
                        print(f"⚠️ Nie udało się odczytać {filepath}: {e}")

    return "\n".join(all_contents)

def copy_sources_to_clipboard(directories):
    combined_content = collect_source_files_content(directories)
    pyperclip.copy(combined_content)
    print("📋 Skopiowano do schowka!")

if __name__ == '__main__':
    dirs_to_scan = [
        './binance_data_processor/cloud_storage_clients/',
        # './binance_data_processor/continuity_registry/',
        './binance_data_processor/core/',
        # './binance_data_processor/data_quality/',
        './binance_data_processor/data_sink/',
        './binance_data_processor/enums/',
        # './binance_data_processor/listener/',
        # './binance_data_processor/scraper/',
        # './binance_data_processor/utils/',
    ]
    copy_sources_to_clipboard(dirs_to_scan)