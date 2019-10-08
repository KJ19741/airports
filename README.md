# Uhh
The [iatacodes/README.md] file states that we don't use iatacodes, but the scrips which converts .csv to json definitely does use IATA codes.

# To add a new entry and ensure consistency:
## Airports:
1. Edit [sources/airports.csv](sources/airports.csv)
2. Run `node airports.js regenAirports`. It will update [sources/airports_with_codes.csv](sources/airports_with_codes.csv)
3. Run `node airports.js regenJson`. It will update [stations.json](stations.json)
4. Run `node airports.js`. It will read [stations.json](stations.json) and update the mongodb `Stations` collection by default, unless you specify the `AIRPORTS_collection` env.