# Spotify Charts

Automate data collection from Spotify's worldwide ranking in european countries, e.g.: [Austria](https://spotifycharts.com/regional/at/daily/latest).
A previous snapshot of the data collected by the code has been published at [Kaggle datasets](https://www.kaggle.com/edumucelli/spotifys-worldwide-daily-song-ranking).

## Usage

```sh
conda create --name spotify python
conda activate spotify
```

Install the required packages and run the script by

```python
conda install --yes --file requirements.txt
python spotify.py
```

The collected data will be stored on the `data` directory.
Run a second time to generate a data.csv file that merges all countries.

## Convert to XLSX

XLSX provides some compression, so you might want to convert to that if zipped csv can not be read.

### PowerScript + Excel

Script:

```PowerScript
Dim file, WB

WScript.Echo "Start!"

With CreateObject("Excel.Application")
    On Error Resume Next
    For Each file In WScript.Arguments
        Set WB = .Workbooks.Open(file)
        WB.SaveAs Replace(WB.FullName, ".csv", ".xlsx"), 51
        WB.Close False
    Next
    .Quit
End With

WScript.Echo "Done!"
```

Run with:

```PowerScript
.\toXLSX.vbs Spotify_EU_2019.csvssconvert data.csv data.xlsx
```

### Excel

You can import (not open) the csv into excel and then save as XLSX. Does not work for large files due to row limit.

### Linux: gnumeric

`ssconvert` is part of the gnumeric package:

```sh
ssconvert data.csv data.xlsx
````
