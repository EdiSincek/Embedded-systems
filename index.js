const express = require('express')
const fetch = require('node-fetch')
const bodyParser = require('body-parser')
const path = require('path')

const app = express()
const port = 3000

const databaseFunctions = require('./databaseFunctions');
const apiFunctions = require('./apiFunctions');
const { param } = require('express/lib/request')
const { table } = require('console')
const { stat } = require('fs')

const ip_address1 = "161.53.17.95:8081";
const ip_address2 = "161.53.17.95:8085";
const ip_address3 = "161.53.17.95:8086";
const ip_address4 = "161.53.17.95:8089";
const ipAdrese = [ip_address1, ip_address2, ip_address3, ip_address4];
app.use(bodyParser.json())
app.use(
    bodyParser.urlencoded({
        extended: true,
    })
)
app.use(express.static('public'));


var server = app.listen(port, () => {
    console.log('App running on port: ' + port)
})
server.timeout = 0;

app.post('/stations_data', async (req, res) => {
    const parameters = req.body;
    console.log(parameters);
    const collection_enabled = parameters.collection_enabled == 'on';
    await apiFunctions.createStation(parameters.alias, parameters.ip, parameters.extra_response_time, parameters.security_code, collection_enabled, parameters.collect_base_date, parameters.collection_interval, parameters.collection_retry_interval, parameters.number_of_retries);
    await addNewStation(parameters.ip);
    res.sendFile(path.join(__dirname, '/public/stationImportSuccess.html'));
})


async function fetchData() {
    await databaseFunctions.resetTrackedStations();
    setInterval(async function () {
        const stations = await databaseFunctions.getAllStations();
        const trackedstations = await databaseFunctions.getAllTrackedStations();

        for (const station of stations.rows) {
            var alreadyTracked = false;
            for (const trackedstation of trackedstations) {
                if (station.id == trackedstation.id) {
                    alreadyTracked = true;
                }
            }
            if (!alreadyTracked) {
                const fetchingData = await databaseFunctions.getFetchingData(station.id);
                await databaseFunctions.addTrackedStation(station.id);
                setInterval(async function () {
                    await collect(fetchingData.collection_enabled, fetchingData.collect_base_date, fetchingData.collection_interval, fetchingData.collection_retry_interval, fetchingData.number_of_retries, station.id)
                }, 1000);
            }
        }

    }, 1000);



}



async function collect(collection_enabled, collect_base_date, collection_interval, collection_retry_interval, number_of_retries, station_id) {
    if (collection_enabled) {
        var now = new Date();
        var date = new Date(collect_base_date);

        var secondsNow = Math.round(now.getTime() / 1000);
        var secondsBaseDate = Math.round(date.getTime() / 1000);
        const mod = secondsBaseDate % collection_interval;
        if (secondsNow > secondsBaseDate) {
            if (secondsNow % collection_interval == mod) {
                const tables = await apiFunctions.getAllTables(station_id);
                for (var table of tables) {
                    var exists = false;
                    const name = "data.table_" + station_id + "_" + table.id;
                    exists = await databaseFunctions.checkIfTableExists(name);

                    if (exists) {
                        var res = await apiFunctions.insertIntoDataTable(station_id, table.id);
                        var retries = 1;
                        while (!res) {
                            res = await retry(station_id, table.id, 3000 * retries)

                            //res = await apiFunctions.insertIntoDataTable(station_id, table.id);
                            retries++;

                            if (retries > number_of_retries) {
                                res = true;
                            }
                        }
                    }

                }
            }
        }
    }

}
async function retry(station_id, table_id, interval) {
    setTimeout(async () => {
        const res = await apiFunctions.insertIntoDataTable(station_id, table_id);
        console.log("Retried " + station_id + " " + table_id)
        return res;
    }, interval)
}

async function addNewStation(ip_address) {
    const station = await databaseFunctions.getStationByIp(ip_address);
    const station_id = station.rows[0].id;
    const fetchingData = await databaseFunctions.getFetchingData(station_id);
    setInterval(async function () {
        await collect(fetchingData.collection_enabled, fetchingData.collect_base_date, fetchingData.collection_interval, fetchingData.collection_retry_interval, fetchingData.number_of_retries, station_id)
    }, 1000);
}

fetchData();