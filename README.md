# quant-tools

Just some stuff I made a long time ago - never bothered to clean up and open source, but why not.

Everything is written in python / jupyter notebook

# Running the code
Things you need to install (probably more req - not exhaustive)
- pandas
- numpy
- csv
- dotenv
- zipfile
....

# To download price data
- Get a token from tiingo.com
- create .env file and just type `TIINGO_TOKEN=SECRET`
- create empty folder `Data` in order to download files
- `python download_price_data.py`
*only daily data formatting is downloaded, prob req liks 100 MB*
