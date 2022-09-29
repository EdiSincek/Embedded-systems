const databaseFunctions = require('./databaseFunctions');
const fetch = require('node-fetch');
const { response } = require('express');



// --- STATIONS ---

async function createStation(alias, ip_address, extra_response_time, security_code, collection_enabled, collect_base_date, collection_interval, collection_retry_interval, number_of_retries) {
    const url = "http://" + ip_address + "/?command=dataquery&uri=Status&format=json&mode=most-recent";
    const res = await fetch(url, {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }).then(response => response.json())
        .then(response => station(alias, response, ip_address, extra_response_time, security_code, collection_enabled, collect_base_date, collection_interval, collection_retry_interval, number_of_retries));


    fetchTables(ip_address);

}

async function station(alias, response, ip_address, extra_response_time, security_code, collection_enabled, collect_base_date, collection_interval, collection_retry_interval, number_of_retries) {
    const data = response.head.environment;
    const res = await databaseFunctions.createStation(alias, data.station_name, data.model, data.serial_no, data.os_version, data.prog_name, ip_address, extra_response_time, security_code, collection_enabled, collect_base_date, collection_interval, collection_retry_interval, number_of_retries, false, 1, 1);
    if (res == 1 || res == 0) {
        await fetchTables(ip_address);
        await fetchFields(ip_address);
        const station = await databaseFunctions.getStationByIp(ip_address);
        const station_id = station.rows[0].id;
        const tables = await databaseFunctions.getAllStationsTables(station_id);

        for (const row of tables.rows) {
            const table_id = row.id;
            await createInitialDataTable(station_id, table_id);
        }

    }
}

async function getAllStations() {
    const res = await databaseFunctions.getAllStations();
    return res.rows;
}



// --- TABLES ---

async function fetchTables(ip_address) {
    const url = "http://" + ip_address + "/?command=browsesymbols&format=json";
    fetch(url, {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }).then(response => response.json())
        .then(response => {
            for (table of response.symbols) {
                databaseFunctions.createTable(ip_address, table.name, 1);
            }
        })
}

async function getAllTables(station_id) {
    const tables = await databaseFunctions.getAllStationsTables(station_id);
    return tables.rows;
}


// --- FIELDS ---


async function fetchFields(address) {
    const res = await databaseFunctions.getStationByIp(address);
    const station_id = res.rows[0].id;
    const res2 = await databaseFunctions.getAllStationsTables(station_id);
    const listOfTables = res2.rows;
    for (const table of listOfTables) {
        const tableId = await databaseFunctions.getTableId(station_id, table.name);
        const url = "http://" + address + "/?command=browsesymbols&uri=" + table.name + "&format=json";
        fetch(url, {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }).then(response => response.json())
            .then(response => {
                const symbols = response.symbols;
                let i = 0;
                while (symbols[i] != null) {
                    const nameField = typeof symbols[i].name == 'undefined' ? null : symbols[i].name;
                    const typeField = typeof symbols[i].type == 'undefined' ? null : symbols[i].type;
                    const unitsField = typeof symbols[i].units == 'undefined' ? null : symbols[i].units;
                    const processField = typeof symbols[i].process == 'undefined' ? null : symbols[i].process;
                    const stringLenField = typeof symbols[i].string_len == 'undefined' ? null : symbols[i].string_len;


                    databaseFunctions.createField(tableId.rows[0].id, i, nameField, typeField, unitsField, processField, stringLenField);
                    i++;
                }
            })
    }

}

// --- DATA TABLES ---

async function createInitialDataTable(station_id, table_id) {
    const tableName = await databaseFunctions.getTableFromStationIdAndTableId(station_id, table_id);
    const ip_address = await databaseFunctions.getStationsIpById(station_id);
    const url = "http://" + ip_address + "/?command=dataquery&uri=" + tableName + "&format=json&mode=since-time&p1=1970-01-01";

    const response = await
        fetch(url, {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }).then(response => response.json());
    var i = 0;
    var fieldNames = [];
    const fields = response.head.fields;
    fields.forEach(field => {
        fieldNames[i] = field.name;
        i++;
    })
    await databaseFunctions.createCustomTable(station_id, table_id, fieldNames);
    await initialDataTableInsert(station_id, table_id, fieldNames);


}

async function initialDataTableInsert(station_id, table_id, fieldNames) {
    const tableName = await databaseFunctions.getTableFromStationIdAndTableId(station_id, table_id);
    const ip_address = await databaseFunctions.getStationsIpById(station_id);
    const url = "http://" + ip_address + "/?command=dataquery&uri=" + tableName + "&format=json&mode=since-time&p1=1970-01-01";
    const response2 = await fetch(url, {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }).then(response => response.json());
    const data = response2.data;
    for (const dat of data) {
        await databaseFunctions.insertIntoDataTable(station_id, table_id, fieldNames, dat.time, dat.no, dat.vals);
    }
}


async function insertIntoDataTable(station_id, table_id) {
    const tableName = await databaseFunctions.getTableFromStationIdAndTableId(station_id, table_id);
    const ip_address = await databaseFunctions.getStationsIpById(station_id);
    if (tableName) {
        const res = await databaseFunctions.getLastRecord(station_id, table_id);
        if (typeof res.rows[0].timestamp !== 'undefined') {
            const timestamp = res.rows[0].timestamp;
            const url = "http://" + ip_address + "/?command=dataquery&uri=" + tableName + "&format=json&mode=since-time&p1=" + timestamp.toISOString();
            const response = await fetch(url, {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            }).then(response => response.json());
            var i = 0;
            var fieldNames = [];
            const fields = response.head.fields;
            fields.forEach(field => {
                fieldNames[i] = field.name;
                i++;
            })
            i = 1;
            for (const data of response.data) {
                if (i == 1) {
                    const record = await databaseFunctions.getLastRecord(station_id, table_id);
                    if (record != undefined) {
                        const date = new Date(data.time + ".000Z");
                        const lastRecord = record.rows[0].timestamp;
                        if (date.getMilliseconds() === lastRecord.getMilliseconds()) {
                            i = 2;
                            continue;
                        }
                    }
                    i = 2;
                }
                if (i != 1) {
                    const result = await databaseFunctions.insertIntoDataTable(station_id, table_id, fieldNames, data.time, data.no, data.vals);
                    return result;
                }

            }
        }

    }
    return false;



}


async function checkIfTableExists(station_id, table_id) {
    const tableName = "data.table_" + station_id + "_" + table_id;
    const res = await databaseFunctions.checkIfTableExists(tableName);
    return res;
}


module.exports = {
    createStation, fetchTables, fetchFields, insertIntoDataTable, createInitialDataTable, checkIfTableExists, getAllStations,
    getAllTables
};