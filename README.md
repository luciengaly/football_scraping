# Football Data Scraping

## Description

Football data scraping from Flashscore.fr

## Quick start

### Installation

<details open>
<summary>Install</summary>

Clone repo and install [requirements.txt] in a
[**Python>=3.7.0**](https://www.python.org/) environment.

```bash
git clone https://github.com/luciengaly/football_scraping.git  # Clone
cd football_scraping # Change directory
pip install -r requirements.txt  # Install
```

Download the [geckodriver.zip](https://github.com/mozilla/geckodriver/releases/download/v0.31.0/geckodriver-v0.31.0-win64.zip) and unzip the `.exe` file where you want.
</details>

<details open>
<summary>Launch</summary>

Open the `fs_scraper.py` Python file and modify the necessary parameters : 
```bash
url_league = 'https://www.flashscore.fr/football/france/ligue-1-2021-2022/resultats/' # URL of the league and year you want to scrape
opts = Options() # Options of the driver
opts.headless = False # Whether to display the Firefox driver or not
exec_path = 'D:\\Documents\\15_Outils\\geckodriver.exe' # Path to the geckodriver.exe
```
All is ready to scrap ! 
```bash
python fs_scraper.py # Run
```
</details>

<details open>
<summary>Output</summary>

All the outputs are in the `output` folder created at the first launch.



</details>