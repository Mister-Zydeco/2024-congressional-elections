from HRElectViz.HrElection import HrElection
from HRElectViz.GerryMeter import GerryMeter
if __name__ == '__main__':
    vote_dfnames = [
        'aggregate_vote_by_district',
        'aggregate_vote_by_state',
        'district_major_party_vote',
        'district_winners_with_major_party',
        'district_winners',
        'ndistricts_per_state',
        'districts_ranked_by_vote',
        'state_nwinners_by_party',
    ];

    hrelect = HrElection()

    district_cols: set[str] = set()
    state_cols: set[str] = set()
    for dfname in vote_dfnames:
        method = getattr(hrelect, f'get_{dfname}')
        df = method()
        if dfname == 'district_winners':
            print(dfname, df.columns)
        to_update = district_cols if 'district' in dfname else state_cols
        to_update.update(df.columns)
    common_cols = district_cols.intersection(state_cols)
    district_cols = district_cols.difference(common_cols)
    state_cols = state_cols.difference(common_cols)

    print(f'{common_cols=}')
    print(f'{district_cols=}')
    print(f'{state_cols=}')

    for name, df in hrelect.dfs.items():
        if 'Democrat\nVote' in df.columns:
            print(name)

    gerry_meter = GerryMeter(hrelect)
    gerry_meter.get_gerrymander_metrics()

    for dfname, df in gerry_meter.dfs.items():
        print(f'columns of {dfname}: {df.columns}')