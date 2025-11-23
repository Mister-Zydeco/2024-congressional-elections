import os
import zipfile as zf

import requests


def us_house_election_stats_url(year: str) -> str:
    house_clerk_root = 'https://clerk.house.gov/member_info/electionInfo'
    return f'{house_clerk_root}/{year}/statistics{year}.pdf'


def us_census_states_shp_url(year: str) -> str:
    tiger_root = 'https://www2.census.gov/geo/tiger'
    return f'{tiger_root}/TIGER{year}/STATE/tl_{year}_us_state.zip'


def download(url: str, dest_path: str) -> None:
    response: requests.Response = requests.get(url)
    if response.status_code != requests.codes.ok:
        print(f'Could not download {dest_path}: status={response.status_code}')
        exit(1)


def download_file(url: str, dest_dir: str):
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()

        # Extract filename from URL or Content-Disposition header if available
        filename = os.path.basename(url)
        if 'Content-Disposition' in response.headers:
            # Attempt to get filename from Content-Disposition header
            # Example: attachment; filename="example.pdf"
            cd = response.headers['Content-Disposition']
            if 'filename=' in cd:
                filename = cd.split('filename=')[1].strip('"')

        full_path = os.path.join(dest_dir, filename)

        with open(full_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f'File downloaded successfully to: {full_path}')
        return full_path

    except requests.exceptions.RequestException as e:
        print(f'Error downloading file: {e}')
    except IOError as e:
        print(f'Error saving file to disk: {e}')


def download_and_unzip(url: str, dest_dir: str) -> None:
    dest_path = download_file(url, dest_dir)
    try:
        with zf.ZipFile(dest_path, 'r') as zfh:
            zfh.extractall(dest_dir)
    except Exception as e:
        print(f'Unzipping problem: {e}')
        exit(1)


if __name__ == '__main__':
    url_geo = us_census_states_shp_url('2024')
    dest_dir = './data'
    download_and_unzip(url_geo, dest_dir)

    url_elect = us_house_election_stats_url('2024')
    download_file(url_elect, dest_dir)
