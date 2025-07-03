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

  await client.execute("DROP TABLE IF EXISTS aulas.employee_por_dept_data");
  await client.execute(`
    CREATE TABLE IF NOT EXISTS aulas.employee_por_dept_data (
      dept_name TEXT, emp_no INT, birth_date DATE, first_name TEXT, last_name TEXT,
      gender TEXT, hire_date DATE, from_date DATE, to_date DATE,
      PRIMARY KEY ((dept_name, from_date), emp_no)
      );
  `);

  const sql = `
    SELECT d.dept_name, e.*, de.from_date, de.to_date FROM employees AS e
    JOIN dept_emp AS de ON e.emp_no = de.emp_no
    JOIN departments AS d ON de.dept_no = d.dept_no;
  `;

  const [rows] = await con.execute(sql);

  const insertQuery = `
    INSERT INTO aulas.employee_por_dept_data
    (dept_name, emp_no, birth_date, first_name, last_name, gender, hire_date, from_date, to_date)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `;

  const chunkSize = 2000;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const tasks = chunk.map(r =>
      client.execute(insertQuery, [
        r.dept_name,
        r.emp_no,
        new Date(r.birth_date),
        r.first_name,
        r.last_name,
        r.gender,
        new Date(r.hire_date),
        new Date(r.from_date),
        new Date(r.to_date)
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