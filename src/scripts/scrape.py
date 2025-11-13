import re
import sys
from typing import Tuple

state_territories = [
    'ALABAMA',
    'ALASKA',
    'ARIZONA',
    'ARKANSAS',
    'CALIFORNIA',
    'COLORADO',
    'CONNECTICUT',
    'DELAWARE',
    'FLORIDA',
    'GEORGIA',
    'HAWAII',
    'IDAHO',
    'ILLINOIS',
    'INDIANA',
    'IOWA',
    'KANSAS',
    'KENTUCKY',
    'LOUISIANA',
    'MAINE',
    'MARYLAND',
    'MASSACHUSETTS',
    'MICHIGAN',
    'MINNESOTA',
    'MISSISSIPPI',
    'MISSOURI',
    'MONTANA',
    'NEBRASKA',
    'NEVADA',
    'NEW HAMPSHIRE',
    'NEW JERSEY',
    'NEW MEXICO',
    'NEW YORK',
    'NORTH CAROLINA',
    'NORTH DAKOTA',
    'OHIO',
    'OKLAHOMA',
    'OREGON',
    'PENNSYLVANIA',
    'RHODE ISLAND',
    'SOUTH CAROLINA',
    'SOUTH DAKOTA',
    'TENNESSEE',
    'TEXAS',
    'UTAH',
    'VERMONT',
    'VIRGINIA',
    'WASHINGTON',
    'WEST VIRGINIA',
    'WISCONSIN',
    'WYOMING',
    'GUAM',
    'VIRGIN ISLANDS',
    'NORTHERN MARIANA ISLANDS',
    'DISTRICT OF COLUMBIA',
    'AMERICAN SAMOA',
    'PUERTO RICO',
]


def get_text(fname: str) -> list[str]:
    with open(fname) as fh:
        text = fh.read()
    lines = text.split('\n')
    return lines


def extract_state_results(lines: list[str]) -> dict[str, list[str]]:
    lineno = 0
    startmarker = re.compile(
        'FOR UNITED STATES REPRESENTATIVE|FOR DELEGATE|FOR RESIDENT COMMISSIONER'
    )
    endmarker = 'Recapitulation'
    all_results: dict[str, list[str]] = {}
    while lineno < len(lines):
        state_terr = lines[lineno].strip()
        if state_terr not in state_territories:
            lineno += 1
            if lineno == len(lines):
                break
            continue
        else:
            state_terr_results = []

            while not re.match(startmarker, lines[lineno].strip()):
                lineno += 1

            if lines[lineno].startswith('FOR RESIDENT COMMISSIONER'):
                state_terr_results = [lines[lineno].replace('FOR ', '')]
            if lines[lineno].startswith('FOR DELEGATE'):
                state_terr_results = [lines[lineno].replace('FOR ', '')]
            lineno += 1
            while lineno < len(lines) and not lines[lineno].startswith(
                endmarker
            ):
                if re.search(
                    r'Continued|^\d+$|^Total|Continuing Ballots|'
                    'Exhausted Ballots',
                    lines[lineno],
                ):
                    lineno += 1
                    continue
                state_terr_results.append(lines[lineno])
                lineno += 1
            all_results[state_terr] = state_terr_results
    return all_results


def split_state_results(results: list[str]) -> dict[str, list[str]]:
    district, lno = '', 0
    candstrings: list[str] = []
    by_district: dict[str, list[str]] = {}
    while lno < len(results):
        line = results[lno]
        if matchobj := re.match(
            'AT LARGE|DELEGATE|RESIDENT COMMISSIONER', line
        ):
            if candstrings:
                by_district[district] = candstrings
            candstrings = []
            district = matchobj.group(0)
        elif matchobj := re.match(r'(\d+)\. (.*)', line):
            if candstrings:
                by_district[district] = candstrings
            district = matchobj.group(1)
            candstrings = [matchobj.group(2).strip()]
        else:
            candstrings.append(line)
        lno += 1
    by_district[district] = candstrings
    return by_district


districtparser1 = re.compile(r'([^,]*), ([^.]*)\D*(.*)')
districtparser2 = re.compile(
    '(Scattering|Write-in|Under Votes|Over Votes|Blank Votes|'
    'Miscellaneous|Blanks?|Void|All Others|Other Write-ins|'
    'Continuing Ballots|Exhausted Ballots|Working Families|Independent|'
    'Conservative|Common Sense Suffolk|Common Sense|Invalid)'
    r'\D*(.*)'
)


def parse_district_record(record: str) -> Tuple[str, str, int]:
    record = re.sub(', ?(Jr|Sr)', r' \1', record)
    name, party, votecountstr = '', '', ''
    matchobj = re.match(districtparser1, record)
    if matchobj:
        name, party, votecountstr = matchobj.groups()
    else:
        matchobj = re.match(districtparser2, record)
        if matchobj:
            name, party, votecountstr = 'None', 'None', matchobj.group(2)
        else:
            print(f'Cannot parse record {record}', file=sys.stderr)
            exit(1)
    party = party.replace(',', ' ').strip()
    votecount = int(re.sub('[,()]', '', votecountstr))
    return name, party, votecount


if __name__ == '__main__':
    results = extract_state_results(get_text('statistics2024.txt'))
    print('StateTerritory,District,Name,Party,Vote')
    for stateterr in state_territories:
        by_district = split_state_results(results[stateterr])
        for district, res in by_district.items():
            for record in res:
                name, party, votecount = parse_district_record(record)
                print(f'{stateterr},{district},{name},{party},{votecount}')
