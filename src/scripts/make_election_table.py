
from HRElectViz.HrElection import HrElection,std_polars_config
from great_tables import GT, html
import pickle


if __name__ == '__main__':
    dfnames = [
        'aggregate_vote_by_district',
        'aggregate_vote_by_state',
        'district_major_party_vote',
        'district_winners',
        'district_winners_with_major_party',
        'districts_ranked_by_vote',
        'ndistricts_per_state',
        'state_nwinners_by_party',
    ]
    hr_elect = HrElection()
    prompt = '\n'.join(f'{x}: {dfname}'
        for x, dfname in enumerate(dfnames)
    ) + '\nEnter number next to desired dataframe above: '
    df_no = int(input(prompt))
    method = getattr(hr_elect, f'get_{dfnames[df_no]}')
    df = method().rename(lambda col: col.replace('\n', '<br>'))
    table = GT(df).tab_header(
        title=dfnames[df_no]
    ).cols_label({col: html(col) for col in df.columns})
    table.write_raw_html('df.html')
    with open('states.pickle', 'wb') as fh:
        pickle.dump(hr_elect.dfs['states'], fh)