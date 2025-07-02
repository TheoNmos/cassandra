const express = require('express'); // npm install express
const router = express.Router();
const settings = require('../settings.js');
const { Client } = require('cassandra-driver'); // npm install cassandra-driver

router.get('/employee_by_manager', async (req, res) => {
    const manager_name = req.query.manager_name;
    if (!manager_name) {
        return res.status(400).json({ error: 'manager_name query parameter is required' });
    }
    const client = new Client({
        cloud: {
            secureConnectBundle: "secure-connect-cassandra-test.zip",
        },
        credentials: {
            username: settings.clientId,
            password: settings.secret,
        },
    });
    try {
        await client.connect();
        const csql = 'SELECT * FROM aulas.employee_por_manager WHERE manager_name = ?';
        const result = await client.execute(csql, [manager_name], { prepare: true });
        res.json(result.rows);
    } catch (err) {
        res.status(500).json({ error: err.message });
    } finally {
        await client.shutdown();
    }
});

module.exports = router;