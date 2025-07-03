const settings = require("../settings.js");
const { Client } = require("cassandra-driver");
const mysql = require("mysql2/promise");

async function run() {
  const con = await mysql.createConnection({
    host: 'localhost',
    user: 'root',
    password: '1234',
    database: 'employees'
  });

  const client = new Client({
    cloud: { secureConnectBundle: "secure-connect-cassandra-test.zip" },
    credentials: {
      username: settings.clientId,
      password: settings.secret,
    },
  });
  await client.connect();

  await client.execute("DROP TABLE IF EXISTS aulas.employee_por_manager");
  await client.execute(`
    CREATE TABLE aulas.employee_por_manager (
      manager_name TEXT, emp_no INT, birth_date DATE,
      first_name TEXT, last_name TEXT, gender TEXT, hire_date DATE,
      PRIMARY KEY (manager_name, emp_no)
    );
  `);

  const sql = `
    SELECT CONCAT(m.first_name, ' ', m.last_name) AS manager_name, e.* FROM dept_manager dm
    JOIN dept_emp de ON dm.dept_no = de.dept_no AND de.from_date <= CURDATE() AND de.to_date >= CURDATE()
    JOIN employees e ON de.emp_no = e.emp_no
    JOIN employees m ON dm.emp_no = m.emp_no;
  `;
  const [rows] = await con.execute(sql);

  const insertQuery = `
    INSERT INTO aulas.employee_por_manager
    (manager_name, emp_no, birth_date, first_name, last_name, gender, hire_date)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    `;

  const chunkSize = 1000;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const tasks = chunk.map(r =>
      client.execute(insertQuery, [
        r.manager_name,
        r.emp_no,
        new Date(r.birth_date),
        r.first_name,
        r.last_name,
        r.gender,
        new Date(r.hire_date)
      ], { prepare: true })
    );
    await Promise.all(tasks);
    console.log(`Inseridos registros ${i + 1} a ${i + chunk.length}`);
  }

  console.log(`Total migrado: ${rows.length} registros.`);
  await client.shutdown();
  await con.end();
}

run().catch(err => console.error(err));