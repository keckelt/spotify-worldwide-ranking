# Spotify Charts
Automate data collection from Spotify's worldwide ranking in european countries, e.g.: https://spotifycharts.com/regional/at/daily/latest.
A previous snapshot of the data collected by the code has been published at [Kaggle datasets](https://www.kaggle.com/edumucelli/spotifys-worldwide-daily-song-ranking).

## Usage

This is still using Python 2. 
Install the required packages and run the script by

```python
pip install -r requirements.txt
python spotify.py
```

The collected data will be stored on the `data` directory.
Run twice to generate a data.csv file that merges all countries.