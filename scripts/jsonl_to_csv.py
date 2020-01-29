import json
import csv


def _get_or_default(obj, key_path, default=None):
    value = obj
    for key in key_path:
        if key not in obj:
            return default
        value = obj[key]
    return value


def _get_headers(jsonl_filepath):
    headers = set()
    for row_obj in _iter_jsonl(jsonl_filepath):
        headers.update(set(row_obj.keys()))
    return sorted(list(headers))


def _iter_jsonl(jsonl_filepath):
    with open(jsonl_filepath, 'r') as fp:
        for json_line in fp:
            yield json.loads(json_line)


def convert_jsonl_to_csv(jsonl_path, csv_path):
    headers = _get_headers(jsonl_path)
    print('HEADERS:', headers)

    with open(csv_path, 'w') as fp:
        writer = csv.DictWriter(fp, fieldnames=headers)
        writer.writeheader()

        for row_dict in _iter_jsonl(jsonl_path):
            writer.writerow(row_dict)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help='input json-per-line filepath')
    parser.add_argument('--output', help='output csv filepath')

    args = parser.parse_args()
    convert_jsonl_to_csv(args.input, args.output)
