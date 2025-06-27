const settings = require("./settings.js");
const { Client } = require("cassandra-driver");
const mysql = require("mysql2");

async function run() {
    const con = mysql.createConnection({
        host: 'localhost',
        user: 'root',
        password: 'root',
        database: 'sakila'
    });
    const client = new Client({
        cloud: {
            secureConnectBundle: "secure-connect-cassandra-test.zip",
        },
        credentials: {
            username: settings.clientId,
            password: settings.secret,
        },
    });
    await client.connect();
    await client.execute("DROP TABLE IF EXISTS aulas.customer_per_film");
    const SQL_RENTALS_CREATE_TABLE = "CREATE TABLE aulas.rentals (customer text,staff text, rental_id INT primary key, rental_date timestamp, amount FLOAT);";
    await client.execute(SQL_RENTALS_CREATE_TABLE);
    let sql = `SELECT concat(concat(c.first_name," "), c.last_name) as "Customer", concat(concat(s.first_name," "), s.last_name) as "Staff", r.rental_id, r.rental_date, p.amount
FROM sakila.payment p
inner join customer c on c.customer_id = p.customer_id
inner join staff s on s.staff_id = p.staff_id
inner join rental r on r.rental_id = p.payment_id order by 1`;
    con.query(sql, function (err, result) {
        result.forEach(async record => { //write data on cassandra
            let sql = "insert into aulas.rentals (customer, staff, rental_id, rental_date, amount)";
            sql += ` values('${record["Customer"]}','${record["Staff"]}',${record["rental_id"]},'${new Date(record["rental_date"]).toISOString()}',${record["amount"]})`;
            client.execute(sql);
        });
    });
}
// Run the async function
run();