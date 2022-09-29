const res = require('express/lib/response');
const { Client } = require('pg');


function createClient() {
    const client = new Client({
        user: 'postgres',
        host: 'localhost',
        database: 'postgres',
        password: 'password',
        port: 5432,
    });
    return client;
}

// ----- PUBLIC.STATIONS TABLE -----
async function createStation(alias, station_name, model, serial_no, os_version, prog_name, ip_address, extra_response_time, security_code,
    collection_enabled, collect_base_date, collection_interval, collection_retry_interval, number_of_retries, deleted, created_by, edited_by) {
    const client = createClient();
    client.connect();
    var alreadyExists = false;
    const insertQuery = "INSERT INTO public.stations (alias,station_name,model,serial_no, os_version, prog_name, ip_address, extra_response_time, security_code,collection_enabled, collect_base_date, collection_interval, collection_retry_interval, number_of_retries, deleted, created_by, edited_by)" +
        "VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)";
    const selectQuery = "SELECT * FROM public.stations WHERE serial_no = '" + serial_no + "'";
    const values = [alias, station_name, model, serial_no, os_version, prog_name, ip_address, extra_response_time, security_code, collection_enabled, collect_base_date, collection_interval, collection_retry_interval, number_of_retries, deleted, created_by, edited_by];

    try {
        const resSelect = await client.query(selectQuery);
        if (resSelect.rows.length > 0) {
            alreadyExists = true;
        }
        if (!alreadyExists) {
            const res = await client.query(insertQuery, values);
            console.log('New station with ip: ' + ip_address + ' created successfully.');
            return 1;
        } else {
            console.log("Station: " + serial_no + " already exists.");
            return 0;
        }

    } catch (err) {
        return -1;
    } finally {
        client.end();
    }
}

async function deleteAllStations() {
    const deleteQuery = "DELETE FROM public.stations";
    console.log(deleteQuery);
    const client = createClient();
    try {
        const res = await client.query(deleteQuery);
        console.log("All stations deleted.");
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function getStationByIp(ip_address) {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT id FROM public.stations WHERE ip_address = '" + ip_address + "'";
    try {
        return await client.query(selectQuery);
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end;
    }
}

async function getStationsIpById(station_id) {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT ip_address FROM public.stations WHERE id = '" + station_id + "'";
    try {
        const res = await client.query(selectQuery);
        return res.rows[0].ip_address;
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function getAllStations() {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT * FROM public.stations";
    try {
        const res = await client.query(selectQuery);
        return res;
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function getFetchingData(station_id) {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT collection_enabled,collect_base_date,collection_interval,collection_retry_interval,number_of_retries FROM public.stations WHERE id = " + station_id;
    try {
        const res = await client.query(selectQuery);
        return res.rows[0];
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function addTrackedStation(station_id) {
    const client = createClient();
    client.connect();
    const insertQuery = "INSERT INTO public.trackedstations (id) VALUES (" + station_id + ")";
    try {
        await client.query(insertQuery);
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function getAllTrackedStations() {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT * FROM public.trackedstations";
    try {
        const res = await client.query(selectQuery);
        return res.rows;
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function resetTrackedStations() {
    const client = createClient();
    client.connect();
    const query = "DELETE FROM public.trackedstations";
    try {
        await client.query(query);
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}
// ----- PUBLIC.TABLES TABLE -----
async function createTable(ip_address, name, unique_name_counter) {
    var alreadyExists = false;
    const client = createClient();
    client.connect();
    const insertQuery = "INSERT INTO public.tables (station_id,name,unique_name_counter,deleted,collection_enabled) VALUES ($1, $2, $3, $4, $5)";
    const stationIdQuery = "SELECT id,collection_enabled,deleted from public.stations WHERE ip_address = '" + ip_address + "'";

    try {
        const stationIdRes = await client.query(stationIdQuery);
        const station_id = stationIdRes.rows[0].id;
        const collection_enabled = stationIdRes.rows[0].collection_enabled;
        const deleted = stationIdRes.rows[0].deleted;
        const values = [station_id, name, unique_name_counter, deleted, collection_enabled];
        const selectQuery = "SELECT * FROM public.tables WHERE station_id = '" + station_id + "' AND name = '" + name + "'";
        const resSelect = await client.query(selectQuery);
        if (resSelect.rows.length > 0) {
            alreadyExists = true;
        }
        if (!alreadyExists) {
            const res = await client.query(insertQuery, values);
            console.log('New table ' + name + ' created successfully');
            return 1;
        } else {
            console.log('Table: ' + name + ', with station_id: ' + station_id + ' already exists.');
            return 0;
        }
    } catch (err) {
        console.log(err.stack);
        return -1;
    } finally {
        client.end();
    }
}

async function getAllStationsTables(station_id) {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT id,name FROM public.tables WHERE station_id = '" + station_id + "'";
    try {
        return await client.query(selectQuery);
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function getTableId(station_id, name) {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT id FROM public.tables WHERE station_id= '" + station_id + "' AND name = '" + name + "'";
    try {
        return await client.query(selectQuery);
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function getTableFromStationIdAndTableId(station_id, table_id) {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT name FROM public.tables WHERE station_id= '" + station_id + "' AND id = '" + table_id + "'";
    try {
        const res = await client.query(selectQuery);
        if (res.rows[0].name) {
            return res.rows[0].name
        }
        return false;

    } catch (err) {
        return false;
    } finally {
        client.end();
    }
}

// ----- PUBLIC.FIELDS TABLE -----
async function createField(table_id, column_number, name, type, units, process, string_len) {
    var alreadyExists = false;
    const client = createClient();
    client.connect();
    const query = "INSERT INTO public.fields (table_id, column_number, name, type, units, process, string_len) VALUES ($1, $2, $3, $4, $5, $6, $7)";
    const selectQuery = "SELECT * FROM public.fields WHERE table_id = '" + table_id + "' AND column_number = '" + column_number + "'";
    const values = [table_id, column_number, name, type, units, process, string_len];
    try {
        const resSelect = await client.query(selectQuery);
        if (resSelect.rows.length > 0) {
            alreadyExists = true;
        }
        if (!alreadyExists) {
            const res = await client.query(query, values);
            console.log('New field created successfully');
        } else {
            console.log("Field with tableid: " + table_id + " and column_number: " + column_number + " already exists");
        }

    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function getAllTablesFields(table_id) {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT id,name FROM public.fields WHERE table_id = '" + table_id + "'";
    try {
        const resSelect = await client.query(selectQuery);
        return resSelect.rows;
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}


// ----- PUBLIC.USERS TABLE -----
async function createUser(name) {
    const client = createClient();
    client.connect();
    const query = "INSERT INTO public.users (name) VALUES ($1)";
    const values = [name];
    try {
        const res = await client.query(query, values);
        console.log("User created");
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function deleteUser(id) {
    const client = createClient();
    client.connect();
    const query = "DELETE FROM public.users WHERE id = ($1)";
    const values = [id];
    try {
        const res = await client.query(query, values);
        console.log("User with id " + id + " was deleted.");
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }

}

// ----- DATA.CUSTOM TABLE -----
async function createCustomTable(station_id, table_id, fieldNames) {
    const client = createClient();
    client.connect();
    var query = "CREATE TABLE IF NOT EXISTS data.table_" + station_id + "_" + table_id + "(id SERIAL,timestamp timestamp NOT NULL,record int4 NOT NULL,";
    fieldNames.forEach(fieldName => {
        query += fieldName + " varchar(30),"
    })
    query = query.substring(0, query.length - 1);
    query += ");";
    try {
        const res = await client.query(query);
        console.log("Table created");
        return 1;
    } catch (err) {
        console.log(err.stack);
        return -1;
    } finally {
        client.end();
    }

}

async function insertIntoDataTable(station_id, table_id, fieldNames, timestamp, record, measuredValues) {
    const client = createClient();
    client.connect();
    var insertQuery = "INSERT INTO data.table_" + station_id + "_" + table_id + " (timestamp,record,";
    fieldNames.forEach(fieldName => {
        insertQuery += fieldName + ",";
    })
    insertQuery = insertQuery.substring(0, insertQuery.length - 1);
    insertQuery += ") VALUES(";
    for (let i = 1; i < measuredValues.length + 3; i++) {
        insertQuery += "$" + i + ", ";
    }
    insertQuery = insertQuery.substring(0, insertQuery.length - 2);
    insertQuery += ");";
    const vars = [timestamp, record];
    measuredValues.forEach(value => vars.push(value));
    try {
        const res = await client.query(insertQuery, vars);
        console.log("Values inserted in db: table_" + station_id + "_" + table_id);
        return true;
    } catch (err) {
        return false;
    } finally {
        client.end();
    }
}

async function getLastRecord(station_id, table_id) {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT timestamp FROM data.table_" + station_id + "_" + table_id + " ORDER BY id DESC LIMIT 1";
    try {
        const res = await client.query(selectQuery);
        return res;
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}

async function checkIfTableExists(tableName) {
    const client = createClient();
    client.connect();
    const selectQuery = "SELECT EXISTS (SELECT * FROM " + tableName + " )";
    try {
        const res = await client.query(selectQuery);
        return res.rows[0].exists;
    } catch (err) {
        return false;
    } finally {
        client.end();
    }
}

async function deleteDuplicates(station_id, table_id) {
    const client = createClient();
    client.connect();
    const tableName = "table_" + station_id + "_" + table_id;
    const deleteQuery = "DELETE FROM " + tableName + " x USING " + tableName + " y WHERE x.timestamp = y.timestamp";
    console.log(deleteQuery);
    try {
        const res = await client.query(deleteQuery);
    } catch (err) {
        return false;
    } finally {
        client.end();
    }
}

async function getData(station_id, table_id) {
    const client = createClient();
    client.connect();
    const tableName = "table_" + station_id + "_" + table_id;
    const selectQuery = "SELECT timestamp, record FROM data." + tableName + " ORDER BY id DESC LIMIT 50";
    try {
        const res = await client.query(selectQuery);
        return res.rows;
    } catch (err) {
        console.log(err.stack);
    } finally {
        client.end();
    }
}






module.exports = {
    createUser, deleteUser, createStation, createTable, createField, getStationByIp, deleteAllStations, getAllStationsTables, getAllStations,
    getTableId, createCustomTable, getTableFromStationIdAndTableId, getStationsIpById, getAllTablesFields, insertIntoDataTable, getLastRecord,
    checkIfTableExists, getFetchingData, deleteDuplicates, addTrackedStation, getAllTrackedStations, resetTrackedStations, getData
};