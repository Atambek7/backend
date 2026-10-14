#!/usr/bin/env python3
import json
import re
from collections import defaultdict
from pathlib import Path

BACKEND_DIR = Path("/home/Altera/workdir/kaufland/backend")

# 1. Загружаем EAN из completed_desc_updates_jv.txt
input_file = BACKEND_DIR / "completed_desc_updates_jv.txt"
jv_eans = [line.strip() for line in input_file.read_text(encoding="utf-8").splitlines() if line.strip()]
print(f"Загружено EAN из {input_file.name}: {len(jv_eans):,}")

# 2. Загружаем кэш соответствия EAN -> source_file для JV
cache_file = BACKEND_DIR / "reports" / "aftercool_ean_factory_cache.json"
with open(cache_file, "r", encoding="utf-8") as f:
    cache = json.load(f)
print(f"Загружено записей в кэше Aftercool JV: {len(cache):,}")

# Функция очистки названия фабрики
def clean_factory_name(source_file: str) -> str:
    if not source_file:
        return "Неизвестная фабрика"
    name = re.sub(r"\.(csv|xlsx)$", "", str(source_file), flags=re.IGNORECASE)
    name = re.sub(r"__\d+$", "", name)
    return name.strip()

# 3. Группируем EAN по фабрикам
groups = defaultdict(list)
missing_count = 0

for ean in jv_eans:
    sf = cache.get(ean)
    if sf:
        fac = clean_factory_name(sf)
        groups[fac].append(ean)
    else:
        groups["НЕИЗВЕСТНАЯ_ФАБРИКА"].append(ean)
        missing_count += 1

print(f"Уникальных фабрик: {len(groups)}")
print(f"Не найдено в кэше: {missing_count}")

# 4. Сортируем фабрики по алфавиту
sorted_factories = sorted(groups.keys(), key=lambda x: x.lower())
total_eans_grouped = sum(len(groups[fac]) for fac in sorted_factories)
print(f"Всего сгруппировано EAN: {total_eans_grouped:,}")
assert total_eans_grouped == len(jv_eans), f"Mismatch: {total_eans_grouped} vs {len(jv_eans)}"

# 5. Формируем выходной файл по требуемому формату
output_lines = []

for idx, fac in enumerate(sorted_factories, 1):
    eans_list = groups[fac]
    output_lines.append(f"{idx}. {fac} ({len(eans_list)}):")
    for e in eans_list:
        output_lines.append(e)
    output_lines.append("")  # пустая строка между фабриками для читаемости

out_file_1 = BACKEND_DIR / "factories_with_eans_jv.txt"
out_file_2 = BACKEND_DIR / "reports" / "factories_with_eans_jv.txt"

content = "\n".join(output_lines)
out_file_1.write_text(content, encoding="utf-8")
out_file_2.write_text(content, encoding="utf-8")

print(f"Файл успешно сохранен:")
print(f"  -> {out_file_1} ({len(output_lines):,} строк)")
print(f"  -> {out_file_2}")
