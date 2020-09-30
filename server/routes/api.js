const express = require('express');
const router = express.Router();
const Pool = require('pg').Pool
const pg = require('pg')
const pwd = 'hskpls@123'
// const client = new pg.Client(`postgres://hashkey_db:${pwd}@10.59.15.6:5432/hashkey_db?ssl=true&sslmode=require`)

// client.connect(function (err) {
//     if (err) {
//         return console.error('could not connect to DB', err);
//     }
//     client.query('SELECT * FROM ttd_ingestion.ttd_file_metadata ORDER BY id ASC', (error, results) => {
//         if (error) {
//             rej(error);
//         }
//
//         client.end();
//     });
// });

const pool = new Pool({
    user: 'hashkey_db',
    host: '10.59.15.6',
    database: 'hashkey_db',
    password: 'hskpls@123',
    port: 5432,
    ssl: {
        rejectUnauthorized: false,
    },
});

// const router = express.Router();
function dataFetch() {
    return new Promise((res, rej) => {
        pool.query('SELECT * FROM ttd_ingestion.ttd_file_metadata WHERE status=\'N\' limit 100', (error, results) => {
            if (error) {
                console.error('error connecting', error.stack)
                rej(error);
            }
            res(results.rows);
        });
    });
}

function getLandingData(req, res, next) {
    return dataFetch().then(rows => {
        res.json(rows);
    }).catch(err => {
        res.status(500).json(err);
    });
}

function addFile(req, res, next) {
    return new Promise((res, rej) => {
        const {file_name, status} = req.body;
        pool.query('INSERT INTO ttd_ingestion.ttd_file_metadata (file_name, status) VALUES ($1, $2)', [file_name, status], (error, results) => {
            if (error) {
                rej(error);
            }
            res(results);
        });
    })
        .then(dataFetch)
        .then(rows => {
            res.locals.newData = rows;
            next();
        }).catch(err => {
            res.status(500).json(err);
        });
}

function changeFile(req, res, next) {
    return new Promise((res, rej) => {
        const id = parseInt(req.params.id)
        const {file_name, status} = req.body;
        pool.query('UPDATE ttd_ingestion.ttd_file_metadata SET file_name = $1, status = $2 WHERE id = $3', [file_name, status, id], (error, results) => {
            if (error) {
                rej(error);
            }
            res(results);
        });
    })
        .then(dataFetch)
        .then(rows => {
            res.locals.newData = rows;
            next();
        }).catch(err => {
            res.status(500).json(err);
        });
}

function deleteFile(req, res, next) {
    return new Promise((res, rej) => {
        const id = parseInt(req.params.id)
        pool.query('DELETE FROM ttd_ingestion.ttd_file_metadata WHERE id = $1', [id], (error, results) => {
            if (error) {
                rej(error);
            }
            res(results);
        });
    })
        .then(dataFetch)
        .then(rows => {
            res.locals.newData = rows;
            next();
        }).catch(err => {
            res.status(500).json(err);
        });
}

/* GET home page. */
router.get('/', getLandingData);
router.post('/addFile', addFile);
router.put('/changeFile/:id', changeFile);
router.delete('/deleteFile/:id', deleteFile);

module.exports = router;